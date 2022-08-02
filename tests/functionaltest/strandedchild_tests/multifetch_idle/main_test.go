package main 
import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
	"strconv"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
)

/*

The test will start Mysql server docker and Hera server connects to this Mysql DB docker
No setup needed

*/


var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["rac_sql_interval"] = "0"
	appcfg["idle_timeout_ms"] = "2000"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["database_type"] = "mysql"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "2"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
	testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_1")
	return testutil.RunDML("CREATE TABLE test_simple_table_1 (ID INT PRIMARY KEY, NAME VARCHAR(128), STATUS INT, PYPL_TIME_TOUCHED INT)")
}


func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}


/* ##########################################################################################
   # Fetch some rows and stay idle longer than idle_timeout 
   # Verify the server will NOT close the connection
   ##########################################################################################
*/

func TestMultiFetchIdle(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestMultiFetchIdle begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Println ("Insert 5 rows");
	var id int = 123;
	for i:=0; i < 5; i++ {
		id_str := strconv.Itoa (id) 
		testutil.RunDML("insert into test_simple_table_1 (ID, Name, Status) VALUES (" + id_str + ", 'Smith', 111)")
		id =  id+1
	}


	hostname := testutil.GetHostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
        if err != nil {
                t.Fatal("Error starting Mux:", err)
                return
        }

	db.SetMaxIdleConns(0)
	defer db.Close()

        fmt.Println ("Open new connection");
        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                t.Fatalf("Error getting connection %s\n", err.Error())
        }
	defer conn.Close()
	defer cancel()
	stmt, _ := conn.PrepareContext(ctx, "/*cmd*/Select ID, name from test_simple_table_1")
	defer stmt.Close()
        rows, _ := stmt.Query()
	rows.Next()
	rows.Next()
	// sleep more than the idle timeout of the server, 
	// the server should NOT close the connection and free the worker.
	time.Sleep(4 * time.Second);
	fmt.Println ("Verify connection is not closed");
	if ( testutil.RegexCount("begin recover worker:") > 0) {
           t.Fatalf ("Error: Should NOT see worker recover");
        }

        count := testutil.RegexCountFile ("E.*OCCMUX.*idle_timeout_2000", "cal.log")
	if (count > 0 ) {
	    t.Fatalf ("Error: should NOT see idle_timeout event in CAL");
	}
	rows.Close();
	stmt.Close()
	cancel()
	conn.Close()
	testutil.DoDefaultValidation(t)

	logger.GetLogger().Log(logger.Debug, "TestMultiFetchIdle done  -------------------------------------------------------------")
}

