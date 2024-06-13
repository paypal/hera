package main

import (
	"context"
	"database/sql"

	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	//"github.com/paypal/hera/client/gosqldriver"
	_ "github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31003"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["enable_sharding"] = "true"
	appcfg["num_shards"] = "3"
	appcfg["sharding_algo"] = "mod"
	appcfg["shard_key_name"] = "id"
	appcfg["config_logging_reload_time_hours"] = "1"
	pfx := os.Getenv("MGMT_TABLE_PREFIX")
	if pfx != "" {
		appcfg["management_table_prefix"] = pfx
	}
	appcfg["sharding_cfg_reload_interval"] = "3600"
	appcfg["rac_sql_interval"] = "0"
	//appcfg["readonly_children_pct"] = "40"

	appcfg["soft_eviction_effective_time"] = "10000"
	appcfg["bind_eviction_threshold_pct"] = "60"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"
	opscfg["opscfg.default.server.saturation_recover_throttle_rate"] = "30"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupShardMap() {
	twoTask := os.Getenv("TWO_TASK")
	if !strings.HasPrefix(twoTask, "tcp") {
		// not mysql
		return
	}
	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		testutil.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		testutil.Fatalf("Error getting connection %s\n", err.Error())
	}
	defer conn.Close()

	testutil.RunDML("create table hera_shard_map ( scuttle_id smallint not null, shard_id tinyint not null, status char(1) , read_status char(1), write_status char(1), remarks varchar(500))")

	for i := 0; i < 1024; i++ {
		shard := 0
		if i <= 8 {
			shard = i % 3
		}
		testutil.RunDML(fmt.Sprintf("insert into hera_shard_map ( scuttle_id, shard_id, status, read_status, write_status ) values ( %d, %d, 'Y', 'Y', 'Y' )", i, shard))
	}
}

func before() error {
	tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		tableName = "jdbc_hera_test"
	}
	if strings.HasPrefix(os.Getenv("TWO_TASK"), "tcp") {
		// mysql
		testutil.RunDML("create table jdbc_hera_test ( ID BIGINT, INT_VAL BIGINT, STR_VAL VARCHAR(500))")
	}
	return nil
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func cleanup(ctx context.Context, conn *sql.Conn) error {
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, "/*Cleanup*/delete from "+tableName+" where id != :id")
	_, err := stmt.Exec(sql.Named("id", -123))
	if err != nil {
		return err
	}
	err = tx.Commit()
	return nil
}

func TestConfigLogging(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestConfigLogging setup")
	setupShardMap()
	logger.GetLogger().Log(logger.Debug, "TestConfigLogging begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	hostname, _ := os.Hostname()
	db, err := sql.Open("hera", hostname+":31003")
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	time.Sleep(5 * time.Second)
	if testutil.RegexCountFile("OCC_CONFIG\tSHARDING", "cal.log") < 1 {
		t.Fatalf("Can't find OCC_CONFIG cal event for SHARDING")
	}

	if testutil.RegexCountFile("OCC_CONFIG\tBACKLOG", "cal.log") < 1 {
		t.Fatalf("All or some BACKLOG config-logs are missing")
	}

	if testutil.RegexCountFile("OCC_CONFIG\tTAF", "cal.log") > 0 {
		t.Fatalf("TAF is not enabled so we should not see TAF config logging")
	}

	if testutil.RegexCountFile("OCC_CONFIG\tR-W-SPLIT", "cal.log") > 0 {
		t.Fatalf("All or some R-W-SPLIT config-logs are missing")
	}

	if testutil.RegexCountFile("OCC_CONFIG\tSOFT-EVICTION", "cal.log") < 1 {
		t.Fatalf("All or some SOFT-EVICTION config-logs are missing")
	}

	if testutil.RegexCountFile("OCC_CONFIG\tBIND-EVICTION", "cal.log") < 1 {
		t.Fatalf("All or some BIND-EVICTION config-logs are missing")
	}
	logger.GetLogger().Log(logger.Debug, "TestShardingMod done  -------------------------------------------------------------")
}
