package testutil

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/paypal/hera/utility/logger"
)

var (
	INCOMPLETE = errors.New("Incomplete row")
)

func StatelogGetField(pos int, pattern ...string) (int, error) {
	cmd := "/usr/bin/tail -n 1 state.log"
	if len(pattern) > 0 {
		cmd = fmt.Sprintf("/usr/bin/tail -n 1 state.log | grep '%s'", pattern[0])
		// fmt.Println("cmd:", cmd)
	}
	out, err := exec.Command("/bin/bash", "-c", cmd).Output()
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

func Copy(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	buf := make([]byte, 1024*1024)
	for {
		numBytes, _ := in.Read(buf)
		out.Write(buf[:numBytes])
		if numBytes < len(buf) {
			break
		}
	}
	return out.Close()
}

func BackupAndClear(logbasename, grpName string) {
	num := 2
	bakname := ""
	for {
		bakname = fmt.Sprintf("%s%d.log", logbasename, num)
		num += 1
		_, err := os.Stat(bakname)
		if os.IsNotExist(err) {
			break
		}
	}
	logname := logbasename + ".log"
	/* nowStr := time.Now().Format("15:04:05.000000")
	f, err := os.OpenFile(logname, os.O_APPEND, 0666)
	if err == nil {
		msg := fmt.Sprintf("%s %s BackupAndClear() %s %d=oldFileNum\n", nowStr, grpName, logbasename, num)
		f.WriteString(msg)
		f.Close()
	}
	time.Sleep(10 * time.Millisecond) // */
	Copy(logname, bakname)
	os.Truncate(logname, 0)
	/* time.Sleep(10 * time.Millisecond)
	fh, err := os.OpenFile(logname, os.O_APPEND, 0666)
	if err == nil {
		msg := fmt.Sprintf("%s %s BackupAndClear() %s %d=oldFileNum, now newFile\n", nowStr, grpName, logbasename, num)
		fh.WriteString(msg)
		fh.Close()
	} // */
}

func RunMysql(sql string) (string, error) {
	cmd := exec.Command("mysql", "-h", os.Getenv("mysql_ip"), "-p1-testDb", "-uroot", "heratestdb")
	cmd.Stdin = strings.NewReader(sql)
	var cmdOutBuf bytes.Buffer
	cmd.Stdout = &cmdOutBuf
	err := cmd.Run()
	if err != nil {
		logger.GetLogger().Log(logger.Debug, "RunMysql", "sql=", sql, "err=", err, "out=", cmdOutBuf.String())
	}
	return cmdOutBuf.String(), err
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

func Fatal(msg ...interface{}) {
	fmt.Println(msg...)
	os.Exit(2)
}
func Fatalf(str string, msg ...interface{}) {
	fmt.Printf(str, msg...)
	os.Exit(1)
}

func ClearLogsData() {
	heraFile, err := os.OpenFile(runFolder+"/hera.log", os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("failed to clear Hera log file")
		return
	}
	defer heraFile.Close()

	statelogFile, err := os.OpenFile(runFolder+"/state.log", os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("failed to clear Hera log file")
		return
	}
	defer statelogFile.Close()

	calLogFile, err := os.OpenFile(runFolder+"/cal.log", os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("failed to clear Hera log file")
		return
	}
	defer calLogFile.Close()
}

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
