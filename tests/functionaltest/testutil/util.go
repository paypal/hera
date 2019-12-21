package testutil

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"time"
	"strings"
	"testing"
	_"github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/utility/logger"
)

var (
	INCOMPLETE = errors.New("Incomplete row")
)

func statelogGetField(pos int) (int, error) {
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

func RunSelect(query string) {
        hostname,_ := os.Hostname()
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
        stmt, _ := conn.PrepareContext(ctx, query)
        defer stmt.Close()
        stmt.Query()
}


func Fetch (query string) (int) {
        count := 0;
        hostname,_ := os.Hostname()
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
        for rows.Next() {
                count++;
        }
        return count;
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
	// cleanup and insert one row in the table
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
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
        hostname,_ := os.Hostname()
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

        //stmt.Close()
        return nil
}

func RunDMLCommitLater(dml string, wait_second int) error {
        hostname,_ := os.Hostname()
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
        hostname,_ := os.Hostname()
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

        //Insert a rac maintenenace row
        txn, _ := conn.BeginTx(ctx, nil)
        stmt, _ := txn.PrepareContext(ctx, "/*cmd*/insert into hera_maint (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
         _, err = stmt.Exec(0, status, time.Now().Unix()+diff_time, module , hostname)

	err = txn.Commit()
        if err != nil {
                return err
        }


        stmt.Close()
        cancel()
        conn.Close()
        return nil

}

func SetRacNodeStatus1 (status string, module string, diff_time int64)  error {
        hostname,_ := os.Hostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("occ", hostname + ":31002")
        if err != nil {
                fmt.Errorf ("Error connection to go mux: %s", err)
                return err
        }
        db.SetMaxIdleConns(0)
        defer db.Close()

        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                fmt.Errorf ("Error getting connection %s\n", err.Error())
                return err
        }

        //Insert a rac maintenenace row
        txn, _ := conn.BeginTx(ctx, nil)
        stmt, _ := txn.PrepareContext(ctx, "/*cmd*/ DELETE from occ_maint")
        _, err = stmt.Exec()
        if err != nil {
                fmt.Errorf ("Error DELETE from occ_maint %s\n", err.Error())
                return err
        }
        stmt, _ = txn.PrepareContext(ctx, "/*cmd*/insert into occ_maint (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
         _, err = stmt.Exec(0, status, time.Now().Unix()+diff_time, module , hostname)
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

func ExtractClientId (logfile string, client_num string) string {
    cmd := "grep 'server: accepted from' " +  " " + runFolder + "/" + logfile + " | awk -F\":\" '{print $7}' | awk 'NR==" + client_num + "'"
    fmt.Println ("Grep command in ExtractClientId: ", cmd)
    out, err := BashCmd(cmd)
    if (err != nil) {
        fmt.Errorf("Error occur when extracting client id: %s", out)
    }
    client := strings.TrimSpace (string(out))
    return client
}

func VerifyKilledClient (t *testing.T, client_num string){
    client_id := ExtractClientId ("hera.log", client_num)
    search_str := "'" + client_id + ".*use of closed network connection" + "'";
    fmt.Println ("Search String in VerifyKilledClient(): ", search_str)
    count := RegexCount (search_str)
    if (count < 1) {
	t.Fatalf ("Error: count: %d", count);
    }
}

