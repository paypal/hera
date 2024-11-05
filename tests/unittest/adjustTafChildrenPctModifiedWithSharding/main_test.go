package main

import (
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
	appcfg["enable_sharding"] = "true"
	appcfg["shard_key_name"] = "email_addr"
	appcfg["shard_key_value_type_is_string"] = "true"
	appcfg["num_shards"] = "2"
	appcfg["sharding_cfg_reload_interval"] = "0"

	appcfg["opscfg.default.server.max_connections"] = "20"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.log_level"] = "5"

	if os.Getenv("WORKER") == "postgres" {
		return appcfg, opscfg, testutil.PostgresWorker
	}
	return appcfg, opscfg, testutil.MySQLWorker
}

func setupShardMap() {
	testutil.RunDML("DROP TABLE IF EXISTS test_str_sk")
	testutil.RunDML("create table test_str_sk (email_addr varchar(64), note varchar(64))")
	testutil.RunDML("DROP TABLE IF EXISTS hera_shard_map")
	testutil.RunDML("create table hera_shard_map ( scuttle_id smallint not null, shard_id smallint not null, status char(1) , read_status char(1), write_status char(1), remarks varchar(500))")
	for i := 0; i < 1024; i++ {
		testutil.RunDML(fmt.Sprintf("insert into hera_shard_map ( scuttle_id, shard_id, status, read_status, write_status ) values ( %d, 0, 'Y', 'Y', 'Y' )", i) )
	}
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, nil))
}

/*
11/04/2024 16:42:44: hera.sh1        0    20     0     0     0     0     0     1     0     0     0
11/04/2024 16:42:44: hera.taf.sh1     0     4     0     0     0     0     0     1     0     0     0
11/04/2024 16:42:45: hera.sh0        0    19     0     1     0     0     0     1     0     0     0
11/04/2024 16:42:45: hera.taf.sh0     0     4     0     0     0     0     0     1     0     0     0
11/04/2024 16:42:45: hera.sh1        0    20     0     0     0     0     0     1     0     0     0
11/04/2024 16:42:45: hera.taf.sh1     0     4     0     0     0     0     0     1     0     0     0
11/04/2024 16:42:46: hera.sh0        0     5     0     0     0     0     0     0     0     0     0
11/04/2024 16:42:46: hera.taf.sh0     0     1     0     0     0     0     0     0     0     0     0
11/04/2024 16:42:46: hera.sh1        0     5     0     0     0     0     0     0     0     0     0
11/04/2024 16:42:46: hera.taf.sh1     0     1     0     0     0     0     0     0     0     0     0
11/04/2024 16:42:47: hera.sh0        0     5     0     0     0     0     0     0     0     0     0
11/04/2024 16:42:47: hera.taf.sh0     0     1     0     0     0     0     0     0     0     0     0
11/04/2024 16:42:47: hera.sh1        0     5     0     0     0     0     0     0     0     0     0
11/04/2024 16:42:47: hera.taf.sh1     0     1     0     0     0     0     0     0     0     0     0
*/

func TestAdjustTafChildrenPctWithSharding(t *testing.T) {
	setupShardMap()
	logger.GetLogger().Log(logger.Debug, "TestAdjustTafChildrenPctWithSharding begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	acpt, err := testutil.StatelogGetField(2, "hera.taf.sh0")
	if err != nil {
		t.Fatalf("Error reading state log: %s\n", err.Error())
	}

	if acpt != 4 {
		t.Fatalf("Expected TAF sh0 pool size: 4, Actual %d\n", acpt)
	}

	acpt, _ = testutil.StatelogGetField(2, "hera.taf.sh1")

	if acpt != 4 {
		t.Fatalf("Expected TAF sh1 pool size: 4, Actual %d\n", acpt)
	}

	fmt.Println ("We now change max connections at runtime");
	testutil.ModifyOpscfgParam (t, "hera.txt", "max_connections", "5")
	//Wait for opsfcg change to take effect
	time.Sleep(45 * time.Second)

	acpt, _ = testutil.StatelogGetField(2, "hera.taf.sh0")

	if acpt != 1 {
		t.Fatalf("Expected TAF sh0 pool size: 1, Actual %d\n", acpt)
	}

	acpt, _ = testutil.StatelogGetField(2, "hera.taf.sh1")

	if acpt != 1 {
		t.Fatalf("Expected TAF sh1 pool size: 1, Actual %d\n", acpt)
	}


	logger.GetLogger().Log(logger.Debug, "TestAdjustTafChildrenPctWithSharding done  -------------------------------------------------------------")
}