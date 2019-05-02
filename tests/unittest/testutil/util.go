package testutil

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os/exec"
	"time"
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

func RunDML(dml string) error {
	db, err := sql.Open("occloop", fmt.Sprintf("%d:0:0", 0))
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
