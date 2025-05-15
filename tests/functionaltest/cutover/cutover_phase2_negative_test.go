package main

import (
	"github.com/paypal/hera/tests/util"
	logger2 "github.com/paypal/hera/utility/logger"
	"os"
	"sync"
	"testing"
	"time"
)

func moveToCutOverPhaseI(t *testing.T) (chan map[int64]util.ClientTrafficStats, chan map[int64]util.ClientTrafficStats, chan string, *os.File) {
	_, logFile := util.Setup(t)

	stateLog := make(map[string]int)
	stateLog["occ"] = 25
	util.ValidateStateLog(t, stateLog, true)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 0, t)

	// enable cut over env and tns changes
	util.EnableCutOver(t, false, false)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnable, true, true)
	util.RestartOCC(t, 60)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 2, t)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 2
	util.ValidateStateLog(t, stateLog, true)

	var wg sync.WaitGroup
	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)

	beforeStart := time.Now().Unix() + 2
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)
	afterComplete := time.Now().Unix() - 3
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, beforeStart, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, beforeStart, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, beforeStart, afterComplete, 1, 2)

	startClientTraffic := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Flexup state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.FlexUp, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	cutOverPreState := time.Now().Unix()
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic+3, cutOverPreState-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic+3, cutOverPreState-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic+3, cutOverPreState-3, 1, 2)

	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to cut over phase 1 state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.CutOverPhaseI, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	cutOverPhase1State := time.Now().Unix()
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	cutOverPhase1End := time.Now().Unix()
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, cutOverPhase1State+3, cutOverPhase1End-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, cutOverPhase1State+3, cutOverPhase1End-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, cutOverPhase1State+3, cutOverPhase1End-3)

	return dumpChan, respChan, RespMsg, logFile
}

/*
FLEXUP-SETUP
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver2InvalidNoOfRow
**************************************
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
--------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy | TODO Failing
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO             | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be down
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOver2InvalidNoOfRow(t *testing.T) {
	dumpChan, respChan, RespChan, logFile := moveToCutOverPhaseI(t)
	defer util.TearDown(t, dumpChan, respChan, RespChan, logFile)

	stateLog := make(map[string]int)

	startPhaseII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIInvalidRowCount, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespChan)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseII, invalidPhaseII-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseII, invalidPhaseII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseII, invalidPhaseII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)

	util.RestartOCC(t, 60)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	afterRestart := time.Now().Unix()
	time.Sleep(25 * time.Second)

	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RespChan)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}

	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterRestart+3, trafficStopped-3)
}

/*
FLEXUP-SETUP
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver2InvalidDBUniqueName
**************************************
----------------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname           | r_status | w_status | phase    |
----------------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE_INVALID | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO_INVALID | Y        | N        | CUTOVER  |
----------------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO             | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be down
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOver2InvalidDBUniqueName(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseI(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIInvalidDBUniqName, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 0
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 0, t)
	invalidPhaseII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateFailureTraffic(t, trafficStats, util.READ, startPhaseII+5, invalidPhaseII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseII+5, invalidPhaseII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseII+5, invalidPhaseII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 0
	util.ValidateStateLog(t, stateLog, true)

	util.RestartOCC(t, 60)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	afterRestart := time.Now().Unix()
	time.Sleep(25 * time.Second)

	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}

	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterRestart+3, trafficStopped-3)
}

/*
FLEXUP-SETUP
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver2InvalidOCCName
**************************************
-----------------------------------------------------------------------------------
| ROWS | occ_name    | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-----------------------------------------------------------------------------------
| Row1 | occ-invalid | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ-invalid | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
-----------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO             | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy | TODO Failing
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be down
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOver2InvalidOCCName(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseI(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIInvalidOCCName, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseII, invalidPhaseII-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseII, invalidPhaseII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseII, invalidPhaseII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)

	util.RestartOCC(t, 60)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	afterRestart := time.Now().Unix()
	time.Sleep(25 * time.Second)

	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}

	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterRestart+3, trafficStopped-3)
}

/*
FLEXUP-SETUP
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver2InvalidTwoTask
**************************************
-------------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS     | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------------
| Row1 | occ      | TWO_TASK_INVALID | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | TWO_TASK_INVALID | HERADB_TWO | Y        | N        | CUTOVER  |
-------------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO             | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy | TODO Failing
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be down
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOver2InvalidTwoTask(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseI(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIInvalidTwoTask, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseII, invalidPhaseII-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseII, invalidPhaseII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseII, invalidPhaseII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)

	util.RestartOCC(t, 60)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	afterRestart := time.Now().Unix()
	time.Sleep(25 * time.Second)

	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}

	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterRestart+3, trafficStopped-3)
}

/*
FLEXUP-SETUP
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver2InvalidCutOverPhase
**************************************
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | invalid  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | invalid  |
--------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO             | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be down
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOver2InvalidCutOverPhase(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseI(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIInvalidPhase, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseII, invalidPhaseII-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseII, invalidPhaseII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseII, invalidPhaseII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)

	util.RestartOCC(t, 60)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	afterRestart := time.Now().Unix()
	time.Sleep(25 * time.Second)

	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}

	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterRestart+3, trafficStopped-3)
}

/*
FLEXUP-SETUP
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver2TargetDBDown
**************************************
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------

* shutdown target DB

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | down   |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE             | down   |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | down   |
    | TXN          |             | HERADB_ONE, HERADB_TWO | down   |
    ----------------------------------------------------------------
 4. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be up and failing to connect to target db
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | down   |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | down   |
    | TXN          |             | HERADB_ONE, HERADB_TWO | down   |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOver2TargetDBDown(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseI(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	util.ShutDownDBService("HERADB_TWO", "herabox_secondary_srv", t)
	stateLog := make(map[string]int)

	startPhaseII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase II: ", startPhaseII)
	util.MoveCutOverPhase(t, util.CutOverPhaseII, true, true)
	startPhaseII += 10
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", false, 25, t)
	invalidPhaseII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseII+3, invalidPhaseII-3, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseII+3, invalidPhaseII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseII+3, invalidPhaseII-3)

	util.RestartOCC(t, 60)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	afterRestart := time.Now().Unix()
	time.Sleep(25 * time.Second)

	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}

	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterRestart+3, trafficStopped-3)
}

/*
FLEXUP-SETUP
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver2SourceDBDown
**************************************
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------

* shutdown source DB

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | down   |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE             | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  0             | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be up and failing to connect to target db
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOver2SourceDBDown(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseI(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase II: ", startPhaseII)
	util.MoveCutOverPhase(t, util.CutOverPhaseII, true, true)
	startPhaseII += 10
	util.ShutDownDBService("HERADB_ONE", "herabox_primary_srv", t)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", false, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseII+3, invalidPhaseII-3, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseII+3, invalidPhaseII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseII+3, invalidPhaseII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")

	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 0
	stateLog["occ.live1"] = 25

	util.KillSessionAndValidate(t, stateLog, "herabox_primary_srv", false)

	util.ValidateStateLog(t, stateLog, true)
	util.RestartOCC(t, 60)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	afterRestart := time.Now().Unix()
	time.Sleep(25 * time.Second)

	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}

	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterRestart+3, trafficStopped-3)
}

/*
FLEXUP-SETUP
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver2Rollback
**************************************
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE             | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

Rollback to Cutover Phase 1
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | CUTOVER  |
--------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO             | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

Rollback to FLEXUP
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO    | active |
    | WRITE        |  HERADB_ONE | HERADB_TWO    | active |
    | TXN          |  HERADB_ONE | HERADB_TWO    | active |
    -------------------------------------------------------

Rollback to Enable
------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase  |
------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | ENABLE |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO    | active |
    | WRITE        |  HERADB_ONE | HERADB_TWO    | active |
    | TXN          |  HERADB_ONE | HERADB_TWO    | active |
    -------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOver2Rollback(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseI(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase II: ", startPhaseII)
	util.MoveCutOverPhase(t, util.CutOverPhaseII, true, true)
	startPhaseII += 10
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	phaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseII+5, phaseI-3, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseII+5, phaseI-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseII+5, phaseI-3)

	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", phaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseI, true, true)
	phaseI = time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	preMode := time.Now().Unix()

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, phaseI+5, preMode-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, phaseI+5, preMode-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, phaseI+5, preMode-3)

	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover FLEXUP Mode: ", phaseI)
	util.MoveCutOverPhase(t, util.FlexUp, true, true)
	preMode = time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	enableMode := time.Now().Unix()

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, preMode+5, enableMode-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, preMode+5, enableMode-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, preMode+5, enableMode-3, 1, 2)

	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Enable Mode: ", enableMode)
	util.MoveCutOverPhase(t, util.CutOverEnable, true, true)
	enableMode = time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 2
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 2, t)
	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, enableMode+5, trafficStopped-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, enableMode+5, trafficStopped-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, enableMode+5, trafficStopped-3, 1, 2)

}

/*
FLEXUP-SETUP
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver1ClosingPendingTxn
**************************************
1. Sending Long read (long is 15 seconds here)
2. then move to CUTOVER STATE 2
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------
3.

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         | HERADB_TWO  | HERADB_ONE            | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1ClosingPendingRead(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseI(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)
	util.CT.StopClientTraffic(respChan, RespMsg)

	txnStart := time.Now().Unix()
	var wg sync.WaitGroup
	wg.Add(1)
	go util.CT.LongReadTraffic(&wg, respChan, 10, 15, t)
	time.Sleep(time.Second * 5)
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", txnStart)
	util.MoveCutOverPhase(t, util.CutOverPhaseII, true, true)
	txnStart += 10

	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	logger2.GetLogger().Log(logger2.Alert, "Waiting for Stats")
	trafficStats := <-respChan
	wg.Wait()
	txnEnd := time.Now().Unix()

	util.ValidateFailureTraffic(t, trafficStats, util.READ, txnStart, txnEnd)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}
}

func TestReadWriteSplitLongRead(t *testing.T) {

	_, logFile := util.Setup(t)

	// enable cut over env and tns changes
	util.EnableCutOver(t, true, false)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnable, true, true)
	util.RestartOCC(t, 90)

	// send client traffic
	var wg sync.WaitGroup
	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	stateLog := make(map[string]int)
	stateLog["occ.w"] = 13
	stateLog["occ.r"] = 12
	stateLog["occ.w.live1"] = 2
	stateLog["occ.r.live1"] = 2
	util.ValidateStateLog(t, stateLog, true)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 4, t)

	startClientTraffic := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Flexup state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.FlexUp, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ.w"] = 13
	stateLog["occ.r"] = 12
	stateLog["occ.w.live1"] = 13
	stateLog["occ.r.live1"] = 12
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)

	beforeCutOverStart := time.Now().Unix()

	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, beforeCutOverStart-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, beforeCutOverStart-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, beforeCutOverStart-3, 1, 2)

	logger2.GetLogger().Log(logger2.Alert, "Moving from FLEXUP to Cutover state(stopping write in main DB): ", beforeCutOverStart)
	util.MoveCutOverPhase(t, util.CutOverPhaseI, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	afterServiceStop := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moved to Cutover state(stopped write in main db): ", afterServiceStop)
	stateLog["occ.w"] = 13
	stateLog["occ.r"] = 12
	stateLog["occ.w.live1"] = 13
	stateLog["occ.r.live1"] = 12
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)

	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, beforeCutOverStart, afterServiceStop-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterServiceStop, time.Now().Unix()-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterServiceStop, time.Now().Unix()-3)

	logger2.GetLogger().Log(logger2.Alert, "Moving Read to CutOver: ", time.Now().Unix())
	txnStart := time.Now().Unix()

	wg.Add(1)
	go util.CT.LongReadTraffic(&wg, respChan, 12, 15, t)
	time.Sleep(time.Second * 5)
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", txnStart)
	util.MoveCutOverPhase(t, util.CutOverPhaseII, true, true)

	txnStart += 10

	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	logger2.GetLogger().Log(logger2.Alert, "Waiting for Stats")
	trafficStats = <-respChan
	wg.Wait()
	txnEnd := time.Now().Unix()

	util.ValidateFailureTraffic(t, trafficStats, util.READ, txnStart, txnEnd)

}
