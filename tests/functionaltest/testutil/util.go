package testutil

import (
	"bytes"
	"bufio"
	"context"
	"database/sql"
	"io/ioutil"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"time"
	"strings"
	"testing"
        "github.com/paypal/hera/client/gosqldriver"
	_"github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/utility/logger"
)

var (
	INCOMPLETE = errors.New("Incomplete row")
)

func StatelogGetField(pos int) (int, error) {
	out, err := exec.Command("/bin/bash", "-c", "/usr/bin/tail -n 1 state.log").Output()
	if err != nil {
		return -1, err
	}
	if len(out) != 99 {
		return -1, INCOMPLETE
	}
	c := 27 + 6*pos
	for ; out[c] == ' '; c++ {
	}
	if out[c] < '0' || out[c] > '9' {
		// got the header somehow (can this happen?), try again in a second for the next row
		time.Sleep(time.Second)
	}
	val := 0
	for {
		if c > len(out) || out[c] < '0' || out[c] > '9' {
			break
		}
		val = val*10 + int(out[c]-'0')
		c++
	}
	return val, nil
}

func BashCmd(cmd string) ([]byte, error) {
	return exec.Command("/bin/bash", "-c", cmd).Output()
}
/*
* Helper function for select, can be used when error happens during Fetch
*/
func RunSelect(query string) {
        hostname := GetHostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
        if err != nil {
                fmt.Println("Error connecting to OCC:", err)
        }
        db.SetMaxIdleConns(0)
        defer db.Close()

        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                fmt.Println("Error creating context:", err)
        }
        defer conn.Close()
        // cancel must be called before conn.Close()
        defer cancel()
        mux := gosqldriver.InnerConn(conn)
        err= mux.SetClientInfo(GetClientInfo().Appname, GetClientInfo().Host)
        if err != nil {
                fmt.Println("Error sending Client Info:", err)
        }
        stmt, err := conn.PrepareContext(ctx, query)
	if err != nil {
                fmt.Println("Error Pereparing context:", err)
        }
        defer stmt.Close()
        _, err = stmt.Query()
	if err != nil {
                fmt.Println("Error fetching row:", err)
        }
}


func Fetch (query string) (int) {
        count := 0;
        hostname := GetHostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
        if err != nil {
                fmt.Println("Error connecting to OCC:", err)
                return count
        }
        db.SetMaxIdleConns(0)
        defer db.Close()

        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                fmt.Println("Error creating context:", err)
                return count
        }
        defer conn.Close()
        // cancel must be called before conn.Close()
        defer cancel()
        stmt, _ := conn.PrepareContext(ctx, query)
        defer stmt.Close()
        rows, _ := stmt.Query()
	if err != nil {
                fmt.Println("Error while querying: ", err)
                return count;
        }
	if rows != nil {
             for rows.Next() {
                 count++;
             }
	}
        return count;
}

//Run mysql command line
func RunMysql(sql string) (string, error) {
        cmd := exec.Command("mysql","-h",os.Getenv("mysql_ip"),"-p1-testDb","-uroot", "heratestdb")
        cmd.Stdin = strings.NewReader(sql)
        var cmdOutBuf bytes.Buffer
        cmd.Stdout = &cmdOutBuf
        cmd.Run()
        return cmdOutBuf.String(), nil
}


func RunDML(dml string) error {
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", 0))
	if err != nil {
		return err
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// cancel must be called before conn.Close()
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
        mux := gosqldriver.InnerConn(conn)
        err= mux.SetClientInfo(GetClientInfo().Appname, GetClientInfo().Host)
        if err != nil {
               fmt.Println("Error sending Client Info:", err)
        }
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, dml)
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func RunDML1(dml string) error {
        hostname := GetHostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
        if err != nil {
                fmt.Println("Error connecting to OCC:", err)
                return err
        }
        db.SetMaxIdleConns(0)
        defer db.Close()

        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        // cleanup and insert one row in the table
        conn, err := db.Conn(ctx)
        if err != nil {
                return err
        }
        defer conn.Close()
        // cancel must be called before conn.Close()
        defer cancel()
        mux := gosqldriver.InnerConn(conn)
        err= mux.SetClientInfo(GetClientInfo().Appname, GetClientInfo().Host)
        if err != nil {
               fmt.Println("Error sending Client Info:", err)
        }
        tx, _ := conn.BeginTx(ctx, nil)
        stmt, _ := tx.PrepareContext(ctx, dml)
        defer stmt.Close()
        _, err = stmt.Exec()
        if err != nil {
                return err
        }
        err = tx.Commit()
        if err != nil {
                return err
        }

        return nil
}

func PopulateShardMap(max_scuttle int) error {
        db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", 0))
        if err != nil {
                return err
        }
        db.SetMaxIdleConns(0)
        defer db.Close()

        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        // cancel must be called before conn.Close()
        defer cancel()
        // cleanup and insert one row in the table
        conn, err := db.Conn(ctx)
        if err != nil {
                return err
        }
        defer conn.Close()
        tx, _ := conn.BeginTx(ctx, nil)
        for x := 0; x < max_scuttle; x++ {
	    dml := fmt.Sprint ("INSERT INTO hera_shard_map VALUES (", x, ", ",  x % 5, ",'Y','Y','Y','Initial')")
            stmt, _ := tx.PrepareContext(ctx, dml)
       	    defer stmt.Close()
            _, err = stmt.Exec()
	}
        if err != nil {
                return err
        }
        err = tx.Commit()
        if err != nil {
                return err
        }

        fmt.Println ("=== Finished loading shard map ===")
        return nil
}

func PopulateWhilelistShardMap() error {
	var query [9]string
	query[0] = "drop table IF EXISTS hera_whitelist"  
	query[1] = "create table hera_whitelist (SHARD_KEY int NOT NULL, SHARD_ID int NOT NULL, ENABLE CHAR(1), READ_STATUS CHAR(1), WRITE_STATUS CHAR(1), REMARKS VARCHAR(500))"
	query[2] = "INSERT INTO hera_whitelist ( enable, shard_key, shard_id, read_status, write_status ) VALUES ( 'Y', 000, 0, 'Y', 'Y' )"
	query[3] = "INSERT INTO hera_whitelist ( enable, shard_key, shard_id, read_status, write_status ) VALUES ( 'Y', 111, 1, 'Y', 'Y' )"
	query[4] = "INSERT INTO hera_whitelist ( enable, shard_key, shard_id, read_status, write_status ) VALUES ( 'Y', 222, 2, 'Y', 'Y' )"
	query[5] = "INSERT INTO hera_whitelist ( enable, shard_key, shard_id, read_status, write_status ) VALUES ( 'Y', 333, 3, 'Y', 'Y' )"
	query[6] = "INSERT INTO hera_whitelist ( enable, shard_key, shard_id, read_status, write_status ) VALUES ( 'Y', 444, 4, 'Y', 'Y' )"
	query[7] = "INSERT INTO hera_whitelist ( enable, shard_key, shard_id, read_status, write_status ) VALUES ( 'Y', 555, 5, 'Y', 'Y' )"
	query[8] = "INSERT INTO hera_whitelist ( enable, shard_key, shard_id, read_status, write_status ) VALUES ( 'Y', 1234, 4, 'Y', 'Y' )"
        db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", 0))
        if err != nil {
                return err
        }
        db.SetMaxIdleConns(0)
        defer db.Close()

        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        // cancel must be called before conn.Close()
        defer cancel()
        // cleanup and insert one row in the table
        conn, err := db.Conn(ctx)
        if err != nil {
                return err
        }
        defer conn.Close()
        tx, _ := conn.BeginTx(ctx, nil)
        for x := 0; x < len (query) ; x++ {
            stmt, _ := tx.PrepareContext(ctx, query[x])
            defer stmt.Close()
            _, err = stmt.Exec() 
        }
        if err != nil {
                return err
        }
        err = tx.Commit()
        if err != nil {
                return err
        }

        fmt.Println ("***Done loading shard map")
        return nil
}

func RunDMLCommitLater(dml string, wait_second int) error {
        hostname := GetHostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
        if err != nil {
                fmt.Println("Error connecting to OCC:", err)
                return err
        }
        //db.SetMaxIdleConns(0)
        defer db.Close()

        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                return err
        }
        defer conn.Close()
        // cancel must be called before conn.Close()
        defer cancel()
        tx, _ := conn.BeginTx(ctx, nil)
        fmt.Println ("Set autocommit to false")
        stmt, _ := tx.PrepareContext(ctx, "set autocommit=0")
        defer stmt.Close()
        _, err = stmt.Exec()
        if err != nil {
                fmt.Println ("Set autocommit to false: %s", err)
                return err
        }
        stmt, _ = tx.PrepareContext(ctx, dml)
        _, err = stmt.Exec()
        if err != nil {
                return err
        }
        time.Sleep (time.Duration(wait_second) * time.Second)

        err = tx.Commit()
        if err != nil {
                return err
        }

        return nil
}

func SetRacNodeStatus (status string, module string, diff_time int64)  error {
        hostname := GetHostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
        if err != nil {
                fmt.Errorf ("Error connection to go mux: %s", err)
                return err
        }
        db.SetMaxIdleConns(0)
        defer db.Close()

        err = RunDML("DELETE from hera_maint")
        if err != nil {
                fmt.Errorf ("Error preparing test (delete table) %s\n", err.Error())
                return err
        }

        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                fmt.Errorf ("Error getting connection %s\n", err.Error())
                return err
        }

        //Insert a rac maintenance row
        txn, _ := conn.BeginTx(ctx, nil)
        stmt, _ := txn.PrepareContext(ctx, "/*cmd*/insert into hera_maint (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
         _, err = stmt.Exec(0, status, time.Now().Unix()+diff_time, module , hostname)
        if err != nil {
        	fmt.Errorf ("Error executing sql rac insert statement %s\n", err.Error())
                return err
        }

	err = txn.Commit()
        if err != nil {
                return err
        }


        stmt.Close()
        cancel()
        conn.Close()
        return nil

}

func InsertRacEmptyTime (status string, module string, diff_time int64)  error {
        hostname := GetHostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
        if err != nil {
                fmt.Errorf ("Error connection to go mux: %s", err)
                return err
        }
        db.SetMaxIdleConns(0)
        defer db.Close()

        err = RunDML("DELETE from hera_maint")
        if err != nil {
                fmt.Errorf ("Error preparing test (delete table) %s\n", err.Error())
                return err
        }

        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                fmt.Errorf ("Error getting connection %s\n", err.Error())
                return err
        }
	 //Insert a rac maintenance row with no time
        txn, _ := conn.BeginTx(ctx, nil)
        stmt, _ := txn.PrepareContext(ctx, "/*cmd*/insert into hera_maint (inst_id, status, module, machine) values (?,?,?,?)")
         _, err = stmt.Exec(0, status, module , hostname)
        if err != nil {
                fmt.Errorf ("Error executing sql rac insert statement with empty time %s\n", err.Error())
                return err
        }

	//Insert a rac maintenance row
	stmt, _ = txn.PrepareContext(ctx, "/*cmd*/insert into hera_maint (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
         _, err = stmt.Exec(0, status, time.Now().Unix()+diff_time, module , hostname)
        if err != nil {
                fmt.Errorf ("Error executing sql rac insert statement %s\n", err.Error())
                return err
        }
        err = txn.Commit()
        if err != nil {
                return err
        }
        stmt.Close()
        cancel()
        conn.Close()
        return nil
}

func InsertRacEmptyTime2 (status string, module string, diff_time int64)  error {
        hostname := GetHostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
        if err != nil {
                fmt.Errorf ("Error connection to go mux: %s", err)
                return err
        }
        db.SetMaxIdleConns(0)
        defer db.Close()

        err = RunDML("DELETE from hera_maint")
        if err != nil {
                fmt.Errorf ("Error preparing test (delete table) %s\n", err.Error())
                return err
        }

        //Insert a rac maintenance row
        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                fmt.Errorf ("Error getting connection %s\n", err.Error())
                return err
        }
        txn, _ := conn.BeginTx(ctx, nil)
        stmt, _ := txn.PrepareContext(ctx, "/*cmd*/insert into hera_maint (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
         _, err = stmt.Exec(0, status, time.Now().Unix()+diff_time, module , hostname)
        if err != nil {
                fmt.Errorf ("Error executing sql rac insert statement with empty time %s\n", err.Error())
                return err
        }
        //Insert a rac maintenance row with no time
        stmt, _ = txn.PrepareContext(ctx, "/*cmd*/insert into hera_maint (inst_id, status, module, machine) values (?,?,?,?)")
         _, err = stmt.Exec(0, status, module , hostname)
        if err != nil {
                fmt.Errorf ("Error executing sql rac insert statement %s\n", err.Error())
                return err
        }
        err = txn.Commit()
        if err != nil {
                return err
        }
        stmt.Close()
        cancel()
        conn.Close()
        return nil
}


func RegexCount(regex string) int {
	return RegexCountFile(regex, "hera.log")
}
func RegexCountFile(regex string, filename string) int {
	time.Sleep(10 * time.Millisecond)
	fa, err := regexp.Compile(regex)
	if err != nil {
		logger.GetLogger().Log(logger.Debug, regex+"=regex err compile "+err.Error())
		return -2
	}
	fh, err := os.Open(filename)
	if err != nil {
		logger.GetLogger().Log(logger.Debug, "could not open "+filename+" "+err.Error())
		return -1
	}
	defer fh.Close()
	scanner := bufio.NewScanner(fh)
	count := 0
	lineNum := 0
	//fmt.Println("BEGIN searching "+regex)
	for scanner.Scan() {
		lineNum++
		ln := scanner.Text()
		loc := fa.FindStringIndex(ln)
		if loc != nil { // found
			//fmt.Printf("FOUND %d\n", lineNum)
			count++
		}
	}
	if err := scanner.Err(); err != nil {
		logger.GetLogger().Log(logger.Debug, "err scanning hera.log "+err.Error())
	}
	//fmt.Println("DONE searching "+regex)
	return count
}

/**
* Method to extract client ID
**/
func ExtractClientId (logfile string, client_num string) string {
    cmd := "grep 'server: accepted from' " +  " " + runFolder + "/" + logfile + " | awk -F\":\" '{print $7}' | awk 'NR==" + client_num + "'"
    //fmt.Println ("Grep command in ExtractClientId: ", cmd)
    out, err := BashCmd(cmd)
    if (err != nil) {
        fmt.Errorf("Error occur when extracting client id: %s", out)
    }
    client := strings.TrimSpace (string(out))
    return client
}

/**
* Method to verify if correct client is killed 
**/
func VerifyKilledClient (t *testing.T, client_num string){
    client_id := ExtractClientId ("hera.log", client_num)
    search_str := "'" + client_id + ".*use of closed network connection" + "'";
    fmt.Println ("Search String in VerifyKilledClient(): ", search_str)
    count := RegexCount (search_str)
    if (count < 1) {
	t.Fatalf ("Error: count: %d", count);
    }
}

/**
* Method to check if worker is assigned as LIFO
**/
func IsLifoUsed (t *testing.T, logfile string) bool {
    cmd := "grep 'Pool::SelectWorker' " + runFolder + "/" + logfile + " | awk '{print $6}'  > pids.tmp"
    //fmt.Println ("Grep command to extract worker Id: ", cmd)
    out, err := BashCmd(cmd)
    if (err != nil) {
        fmt.Errorf("Error occur when extracting worker id: %s", out)
    }
    file, err := os.Open("pids.tmp")
    if err != nil {
        t.Fatal(err)
    }
    defer file.Close()

    lifo_count := 0;
    scan_count := 0;
    scanner := bufio.NewScanner(file)
    scanner.Scan();
    workerId := scanner.Text();
    fmt.Println ("workerId: ", workerId);
    for scanner.Scan() {
	scan_count++;
        if !strings.Contains (scanner.Text(),strings.TrimSpace (workerId)) {
           fmt.Println(scanner.Text())
           break
        }
	lifo_count++;
    }
    fmt.Println ("lifo count: ", lifo_count)
    if err := scanner.Err(); err != nil {
        t.Fatal(err)
    }
    return (lifo_count == scan_count) 
}

/**
* Method to modify an opscfg entry in hera.txt
**/
func ModifyOpscfgParam (t *testing.T, logfile string, opscfg_param string, opscfg_value string) {
    //Read file
    data, err := ioutil.ReadFile(runFolder + "/" + logfile)
    if err != nil {
        t.Fatal(err)
    }
    lines := strings.Split(string(data), "\n")
    //Modify the opcfg value
    for i, line := range lines {
        if strings.Contains(line, opscfg_param) {
            lines[i] = "opscfg.default.server." + opscfg_param + "=" + opscfg_value
        }
    }
    output := strings.Join(lines, "\n")
    // write to file
    err = ioutil.WriteFile(runFolder + "/" + logfile, []byte(output), 0644)
        if err != nil {
               t.Fatal(err)
        }
}

func GetHostname() string {
        hostname,err := os.Hostname()

        if err != nil {
                hostname = "127.0.0.1" 
        }
        return hostname
}