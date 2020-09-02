package  util 
import (
	"context"
        "database/sql"
	"fmt"
	"os"
	//"testing"
	"time"
        _"github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/functionaltest/testutil"
)

/*
The test will start Mysql server docker and Hera connects to this Mysql DB docker
No setup needed

*/

var mx testutil.Mux

/**-----------------------------------------
   Helper function to insert a row to test_simple_table_1 with delay
--------------------------------------------*/
func InsertBinding (id string, wait_second int) error {
        fmt.Println ("Insert a row, commit later")
	status := 9999 
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
        conn, err := db.Conn(ctx)
        if err != nil {
                return err
        }
        defer conn.Close()
        defer cancel()
        tx, _ := conn.BeginTx(ctx, nil)
        stmt, _ := tx.PrepareContext(ctx, "insert into test_simple_table_1 (ID, Name, Status) VALUES(:ID, :Name, :Status)")
        if err != nil {
                fmt.Println("Error Preparing context:", err)
        }
        defer stmt.Close()
	_, err = stmt.Exec(sql.Named("ID", id), sql.Named("Name", "Lee"), sql.Named("Status", status))
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

/**-----------------------------------------
   Helper function to update a row in test_simple_table_1 with delay
--------------------------------------------*/
func UpdateBinding (id string, wait_second int) error {
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
        conn, err := db.Conn(ctx)
        if err != nil {
                return err
        }
        defer conn.Close()
        defer cancel()
        tx, _ := conn.BeginTx(ctx, nil)
        stmt, _ := tx.PrepareContext(ctx, "update test_simple_table_1 set Name='Steve' where ID=:ID")
	if err != nil {
                fmt.Println("Error Pereparing context:", err)
        }
        defer stmt.Close()
        _, err = stmt.Exec(sql.Named("ID", id))
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

/**-----------------------------------------
   Helper function to fetch a row in test_simple_table_1 and return row count
--------------------------------------------*/
func FetchBinding (id string, forUpdate string) (int) {
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
        defer cancel()
	query := "Select Name from test_simple_table_1 where ID = :ID " + forUpdate;
        stmt, _ := conn.PrepareContext(ctx, query)
	if err != nil {
                fmt.Println("Error Pereparing context:", err)
        	return count;
        }
        defer stmt.Close()
        rows, _ := stmt.Query(sql.Named("ID", id))
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
