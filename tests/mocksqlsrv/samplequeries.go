package main

/*
* Several test queries against mock MySQL server.
*/

/*=== IMPORTS ================================================================*/
import (
     "database/sql"
     "fmt"
     "log"
     "time"
     "context"

     _ "github.com/go-sql-driver/mysql"
)


/*=== FUNCTIONS ==============================================================*/

/*
* logs errors if there is an error.
*/
func log_error(err error) {
     if err != nil {
          log.Fatal(err)
     }
}

/* Create a user and grant access permissions. */
func createuser(db *sql.DB, name string, pw string) {

     _, err := db.Exec("drop user " + name + "@localhost;")
     log_error(err)
     _, err = db.Exec("flush privileges;")
     log_error(err)

     stmt := fmt.Sprintf("CREATE USER '%s'@'localhost'" +
          " IDENTIFIED BY '%s';", name, pw)
     _, err = db.Exec(stmt)
     log_error(err)

     _, err = db.Exec("GRANT SELECT ON testdb.* TO '" + name + "'@'localhost';")
     log_error(err)
}

func drop(db *sql.DB, name string) {
     _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s;", name))
     log_error(err)
}

/* Create a sample table with input name and input schema. */
func table(db *sql.DB, name string, schema string) {
     drop(db, name)
     _, err := db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS " +
          "%s (%s);", name, schema))
     log_error(err)
}

/* Create a database. Automatically uses the database in the session. */
func createdb(db *sql.DB, name string) {
     _, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", name))
     log_error(err)
     _, err = db.Exec(fmt.Sprintf("USE %s;", name))
     log_error(err)
}

/* Performs select operation from given query. Returns all the rows
* from the select.
*/
func selects(db *sql.DB, query string) {

     var (
          i1 string
     )

     rows, err := db.Query(query)
     log_error(err)
     defer rows.Close()
     for rows.Next() {
          err := rows.Scan(&i1)
          log_error(err)
          fmt.Println(i1)
     }
     err = rows.Err()
     log_error(err)
}

/* Performs update/insert/delete operations. */
func modify(db *sql.DB, query string) {
     _, err := db.Exec(query)
     log_error(err)
}


/* Sample queries. */
func main() {
     fmt.Println("Running full test.")

     // Root user access to DB to start connection testing.
     DSN := "root:hocctest@tcp(localhost:3333)/"
     db, err := sql.Open("mysql", DSN)
     log_error(err)
     defer db.Close()

     // Create a new user.
     user := "test_user"
     pw := "test_password"
     createuser(db, user, pw)
     fmt.Println("Created new test_user.")

     // Create a new db.
     dbname := "testdb"
     createdb(db, dbname)
     fmt.Println("Created database testdb.")

     // Create a new table.
     schema := "i1 INT DEFAULT 0, i2 INT, i3 INT, dscr VARCHAR(255)"
     table(db, "tb", schema)
     fmt.Println(fmt.Sprintf("Created table tb with schema (%s).", schema))

     // Test inserts.
     fmt.Println("Inserting into table tb...")
     modify(db, "INSERT INTO tb VALUES ('2');")
     modify(db, "INSERT INTO tb VALUES ('beepbeepboopboop')")

     fmt.Println("Updating table tb...")
     // Test updates.
     modify(db, "UPDATE tb SET dscr = '0' WHERE i1 = 1")

     disp := "SELECT * FROM tb WHERE i1 = ?"
     // Test selects.
     selects(db, disp)

     ctx, _ /*cancel*/ := context.WithTimeout(context.Background(), 10*time.Second)
     // ctx := context.Background()
	conn, err := db.Conn(ctx)

     stmt, err := conn.PrepareContext(ctx, "select @@global.read_only")
	if err != nil {
		log.Fatal(err)
	}

     _, err = stmt.Query()
	if err != nil {
		log.Fatal(err)
	}

     fmt.Println("Deleting from table tb...")
     // Test deletes.
     modify(db, "DELETE FROM tb WHERE i2 = 3")

     // Shutdown database
     fmt.Println("Testing complete.")
     db.Exec("QUIT;")
}
