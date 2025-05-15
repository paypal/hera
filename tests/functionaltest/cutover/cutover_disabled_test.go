package main

import (
	"github.com/paypal/hera/tests/util"
	"github.com/paypal/hera/utility/logger"
	"sync"
	"testing"
	"time"
)

/*
ENABLE CUT-OVER
  1 Stop occ docker
  2 Push tns with two task for cut-over db
  3 Set env variable TWO_TASK_CUTOVER with the proper two task key
  4 Start occ docker
*/

func TestCutOverDisabled(t *testing.T) {
	_, logFile := util.Setup(t)

	var wg sync.WaitGroup
	respChan, dumpChan, RunMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RunMsg, logFile)

	beforeStart := time.Now().Unix() + 2
	logger.GetLogger().Log(logger.Alert, "Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)
	afterComplete := time.Now().Unix() - 3
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RunMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, beforeStart, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, beforeStart, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, beforeStart, afterComplete, 1, 2)

	stateLog := make(map[string]int)
	stateLog["occ"] = 25
	util.ValidateStateLog(t, stateLog, true)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 0, t)

	util.CT.StopClientTraffic(respChan, RunMsg)
}

/*
TEST:
 1. Before enabling cut-over all traffic should go to primary database
 2. Primary database should have 25 valid connections and cut-over database should have 0
 3. ENABLE CUT-OVER
 4. Shutdown primary service in primary database and make sure cut-over database service is up and running
 5. Create cut-over management table and insert records for cut-over enabled phase
 6. Restart OCC (inside container by issuing SIGHUP)

VALIDATE
 1. No connection for primary database
 2. 12 connection to cut-over database
 3. occ is up and running with ORA error for primary database

OCC should exit - but as of now it is not
TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledPrimaryDBDown(t *testing.T) {
	_, log_File := util.Setup(t)

	var wg sync.WaitGroup
	respChan, dumpChan, RunMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RunMsg, log_File)

	beforeStart := time.Now().Unix() + 2
	logger.GetLogger().Log(logger.Alert, "Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)
	afterComplete := time.Now().Unix() - 3
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RunMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, beforeStart, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, beforeStart, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, beforeStart, afterComplete, 1, 2)

	stateLog := make(map[string]int)
	stateLog["occ"] = 25
	util.ValidateStateLog(t, stateLog, true)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 0, t)

	util.ShutDownDBService("HERADB_ONE", "herabox_primary_srv", t)

	// enable cut over env and tns changes
	util.EnableCutOver(t, false, false)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnable, true, true)
	util.RestartOCC(t, 60)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, -1, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 12, t)

	// TODO validate main DB failure ORA error
	// TODO validate listener is not enabled for traffic
	stateLog["occ.live1"] = 12
	util.ValidateStateLog(t, stateLog, true)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}

	util.CT.StopClientTraffic(respChan, RunMsg)
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase
3. Delete the table only in primary database
4. Restart OCC (SIGHUP)
5. Wait for 30 seconds before starting validation

VALIDATION
1. OCC should not be up and running

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledTableMissingPrimary(t *testing.T) {
	_, logFile := util.Setup(t)

	var wg sync.WaitGroup
	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	util.EnableCutOver(t, false, false)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnable, true, true)
	util.MoveCutOverPhase(t, util.DeleteCutOverTable, true, false)
	util.RestartOCC(t, 90)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}

	util.CT.StopClientTraffic(respChan, RespMsg)
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table
3. Insert only one row into both the database (actually code should expect two rows)
4. Restart OCC (SIGHUP)
5. Wait for 30 seconds before starting validation

VALIDATION
1. OCC should not be up and running
TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledInvalidNumOfRows(t *testing.T) {
	_, logFile := util.Setup(t)

	var wg sync.WaitGroup
	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	util.EnableCutOver(t, false, false)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableInvalidNumRows, true, false)
	util.RestartOCC(t, 90)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}

	util.CT.StopClientTraffic(respChan, RespMsg)
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase
3. Enable sharding by making sure management table has right entries and the cdb values are set for sharding
4. Restart OCC (SIGHUP)
5. Wait for 30 seconds before starting validation

VALIDATION
1. OCC should ignore cut-over flags and continue to serve shard traffic
2. Validate state logs for 25 workers in each shard
3. Validate database connections as 25 for each shard

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledShardedDataBase(t *testing.T) {
	util.Setup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnable, true, true)
	util.EnableSharding(t)
	util.RestartOCC(t, 90)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC should be up and running")
	}

	stateLog := make(map[string]int)
	stateLog["occ.sh0"] = 25
	stateLog["occ.sh1"] = 25
	util.ValidateStateLog(t, stateLog, true)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)

}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with incorrect occ name
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should not be up and running

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledWrongOCCName(t *testing.T) {
	util.Setup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableWrongOCC, true, true)
	util.RestartOCC(t, 90)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with incorrect db unique name
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should be up and running
2. Validate state logs for 25 connections to primary database and 1 for cut-over database
3. Validate the same in database connections

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledInvalidUniqueID(t *testing.T) {
	util.Setup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableInvalidUniqueName, true, true)
	util.RestartOCC(t, 90)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC should be up but looks like it is down")
	}
	stateLog := make(map[string]int)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 2
	util.ValidateStateLog(t, stateLog, true)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 2, t)

}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with incorrect two task key
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should not be up and running

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledInvalidTNS(t *testing.T) {
	util.Setup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableInvalidTNS, true, true)
	util.RestartOCC(t, 90)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with incorrect cut-over phase
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should not be up and running

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledInvalidPhase(t *testing.T) {
	util.Setup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableInvalidPhase, true, true)
	util.RestartOCC(t, 90)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with incorrect read status (other than 'Y'/'N')
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should not be up and running

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledInvalidRead(t *testing.T) {
	util.Setup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableInvalidRead, true, true)
	util.RestartOCC(t, 90)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with incorrect write status (other than 'Y'/'N')
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should not be up and running

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledInvalidWrite(t *testing.T) {
	util.Setup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableInvalidWrite, true, true)
	util.RestartOCC(t, 90)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with write enable but not read
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should be up and running
2. Write and Transaction Traffic should be success and go to the configured database
3. Read traffic should fail
4. Validate state logs for 25 connections to main database and 1 connection to cut-over database
5. validate the database connections for the same

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnableWriteEnabledButNotRead(t *testing.T) {
	_, logFile := util.Setup(t)
	var wg sync.WaitGroup
	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableWriteNoRead, true, true)
	util.RestartOCC(t, 90)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is Down - which is not expected")
	}
	start := time.Now().Unix() + 2
	logger.GetLogger().Log(logger.Alert, "Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)
	afterComplete := time.Now().Unix() - 3
	util.CT.StopClientTraffic(respChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, start, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, start, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start, afterComplete, 1, 2)

	stateLog := make(map[string]int)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 2
	util.ValidateStateLog(t, stateLog, true)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 2, t)
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with write enabled for both database
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should not be up and running

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledDualWrite(t *testing.T) {
	util.Setup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableDualWrite, true, true)
	util.RestartOCC(t, 90)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with read enabled for both database
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should not be up and running

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledDualRead(t *testing.T) {
	util.Setup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableDualRead, true, true)
	util.RestartOCC(t, 90)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}
}
