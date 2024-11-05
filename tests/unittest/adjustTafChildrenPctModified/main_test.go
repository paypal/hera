package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

var mx testutil.Mux
//var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {
	fmt.Println ("setup() begin")
	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["enable_taf"] = "true"
	appcfg["taf_children_pct"] = "20"
	appcfg["opscfg.default.server.max_connections"] = "10"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.log_level"] = "5"

	if os.Getenv("WORKER") == "postgres" {
		return appcfg, opscfg, testutil.PostgresWorker
	}
	return appcfg, opscfg, testutil.MySQLWorker
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, nil))
}

/*
TAF pool should be 1/5th of the primary pool
11/04/2024 14:59:03: hera.taf        0     2     0     0     0     0     0     0     1     0     0
11/04/2024 14:59:04: hera            0    10     0     0     0     0     0     0     1     0     0
11/04/2024 14:59:04: hera.taf        0     2     0     0     0     0     0     0     1     0     0
11/04/2024 14:59:05: hera            0    10     0     0     0     0     0     0     1     0     0
11/04/2024 14:59:05: hera.taf        0     2     0     0     0     0     0     0     1     0     0
11/04/2024 14:59:06: hera            0    10     0     0     0     0     0     0     1     0     0
11/04/2024 14:59:06: hera.taf        0     2     0     0     0     0     0     0     1     0     0
11/04/2024 14:59:07: hera            0    10     0     0     0     0     0     0     1     0     0
11/04/2024 14:59:07: hera.taf        0     2     0     0     0     0     0     0     1     0     0
11/04/2024 14:59:08: hera            0     5     0     0     0     0     0     0     1     0     0
11/04/2024 14:59:08: hera.taf        0     1     0     0     0     0     0     0     1     0     0
11/04/2024 14:59:09: hera            0     5     0     0     0     0     0     0     1     0     0
11/04/2024 14:59:09: hera.taf        0     1     0     0     0     0     0     0     1     0     0
11/04/2024 14:59:10: hera            0     5     0     0     0     0     0     0     1     0     0
11/04/2024 14:59:10: hera.taf        0     1     0     0     0     0     0     0     1     0     0
*/

func TestAdjustTafChildrenPctModified(t *testing.T) {

	logger.GetLogger().Log(logger.Debug, "TestAdjustTafChildrenPctModified begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	defer conn.Close()

	rows, _ := conn.QueryContext(ctx, "SELECT version()")

	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}
	rows.Close()

	acpt, err := testutil.StatelogGetField(2, "hera.taf")
	if err != nil {
		t.Fatalf("Error reading state log: %s\n", err.Error())
	}

	if acpt != 2 {
		t.Fatalf("Expected TAF pool size: 2, Actual %d\n", acpt)
	}

	fmt.Println ("We now change max connections at runtime");
	testutil.ModifyOpscfgParam (t, "hera.txt", "max_connections", "5")
	//Wait for opsfcg change to take effect
	time.Sleep(45 * time.Second)

	acpt, _ = testutil.StatelogGetField(2, "hera.taf")

	if acpt != 1 {
		t.Fatalf("Expected TAF pool size: 1, Actual %d\n", acpt)
	}

	logger.GetLogger().Log(logger.Debug, "TestAdjustTafChildrenPctModified done  -------------------------------------------------------------")
}
