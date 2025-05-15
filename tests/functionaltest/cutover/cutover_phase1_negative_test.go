package main

import (
	"github.com/paypal/hera/tests/util"
	logger2 "github.com/paypal/hera/utility/logger"
	"os"
	"sync"
	"testing"
	"time"
)

func moveToFlexUp(t *testing.T) (chan map[int64]util.ClientTrafficStats, chan map[int64]util.ClientTrafficStats, chan string, *os.File) {
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
	util.RestartOCC(t, 30)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 2, t)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 2
	util.ValidateStateLog(t, stateLog, true)

	var wg sync.WaitGroup
	respChan, dumpChan, RunMsg := util.CT.SendClientTraffic(&wg)

	beforeStart := time.Now().Unix() + 2
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)
	afterComplete := time.Now().Unix() - 3
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RunMsg)

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
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RunMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, cutOverPreState-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, cutOverPreState-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, cutOverPreState-3, 1, 2)

	return dumpChan, respChan, RunMsg, logFile
}

/*
FLEXUP-SETUP
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

**************************************
TestCutOver1InvalidNoOfRow
**************************************
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER  |
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
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO    | active |
    | WRITE        |  HERADB_ONE | HERADB_TWO    | active |
    | TXN          |  HERADB_ONE | HERADB_TWO    | active |
    -------------------------------------------------------
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
func TestCutOver1InvalidNoOfRow(t *testing.T) {
	dumpChan, respChan, RunMsg, logFile := moveToFlexUp(t)
	defer util.TearDown(t, respChan, dumpChan, RunMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIInvalidRowCount, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RunMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)

	afterRestart := util.RestartOCC(t, 30)

	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RunMsg)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)
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
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

**************************************
TestCutOver1InvalidUniqName
**************************************
---------------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname           | r_status | w_status | phase   |
---------------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE_INVALID | Y        | Y        | CUTOVER |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO_INVALID | N        | N        | CUTOVER |
---------------------------------------------------------------------------------------

After validation
----------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname      | r_status | w_status | phase   |
----------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE    | Y        | Y        | CUTOVER |
| Row2 | occ      | CLOC_STG1 | HERADB_ONE    | N        | N        | CUTOVER |
----------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  0             | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  0              | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    -----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB           | state  |
    -----------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO              | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO  | active |
    | TXN          |             | HERADB_TWO, HERADB_ONE  | active |
    -----------------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  0             | accept+wait+busy |
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
func TestCutOver1InvalidUniqName(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToFlexUp(t)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIInvalidUniqName, true, true)
	startPhaseI += 10
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 0
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 0, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI+3, invalidPhaseI-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseI+3, invalidPhaseI-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseI+3, invalidPhaseI-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 0
	util.ValidateStateLog(t, stateLog, true)

	util.RestartOCC(t, 60)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 30 seconds")
	afterRestart := time.Now().Unix()
	time.Sleep(30 * time.Second)

	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, afterRestart+3, trafficStopped-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterRestart+3, trafficStopped-3)

	util.MoveCutOverPhase(t, util.CutOverPhaseICorrectUniqName, true, true)
	validPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", validPhaseI)

	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	stopTraffic := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, validPhaseI+3, stopTraffic-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, validPhaseI+3, stopTraffic-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, validPhaseI+3, stopTraffic-3)

}

/*
FLEXUP-SETUP
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

**************************************
TestCutOver1InvalidTwoTask
**************************************
-----------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS     | db_uname   | r_status | w_status | phase   |
-----------------------------------------------------------------------------------
| Row1 | occ      | TWO_TASK_INVALID | HERADB_ONE | Y        | Y        | CUTOVER |
| Row2 | occ      | TWO_TASK_INVALID | HERADB_TWO | N        | N        | CUTOVER |
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
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO    | active |
    | WRITE        |  HERADB_ONE | HERADB_TWO    | active |
    | TXN          |  HERADB_ONE | HERADB_TWO    | active |
    -------------------------------------------------------
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
func TestCutOver1InvalidTwoTask(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToFlexUp(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIInvalidTwoTask, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

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
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

**************************************
TestCutOver1InvalidOCCName
**************************************
----------------------------------------------------------------------------------
| ROWS | occ_name    | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase   |
----------------------------------------------------------------------------------
| Row1 | occ-invalid | CLOC         | HERADB_ONE | Y        | Y        | CUTOVER |
| Row2 | occ-invalid | CLOC_STG1 | HERADB_TWO | N        | N        | CUTOVER |
----------------------------------------------------------------------------------

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
func TestCutOver1InvalidOCCName(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToFlexUp(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIInvalidOCCName, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

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
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

**************************************
TestCutOver1InvalidPhase
**************************************
-------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | invalid |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | invalid |
-------------------------------------------------------------------------------

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
func TestCutOver1InvalidPhase(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToFlexUp(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIInvalidPhase, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

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
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

**************************************
TestCutOver1InvalidWriteStatus
**************************************
-------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | X        | CUTOVER |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | X        | CUTOVER |
-------------------------------------------------------------------------------

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
func TestCutOver1InvalidWriteStatus(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToFlexUp(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIInvalidWriteStatus, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

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
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

**************************************
TestCutOver1InvalidReadStatus
**************************************
-------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | X        | N        | CUTOVER |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | X        | N        | CUTOVER |
-------------------------------------------------------------------------------

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
func TestCutOver1InvalidReadStatus(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToFlexUp(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIInvalidReadStatus, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

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
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

**************************************
TestCutOver1DualWrite
**************************************
-------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | CUTOVER |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | Y        | CUTOVER |
-------------------------------------------------------------------------------

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
func TestCutOver1DualWrite(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToFlexUp(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIDualWrite, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

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
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

**************************************
TestCutOver1DualRead
**************************************
-------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER |
-------------------------------------------------------------------------------

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
func TestCutOver1DualRead(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToFlexUp(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIDualRead, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

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
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

**************************************
TestCutOver1ReadOff
**************************************
-------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | CUTOVER |
-------------------------------------------------------------------------------

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
    | READ         |             | HERADB_ONE, HERADB_TWO | active |
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
    5.1 OCC Container should be up
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1ReadOff(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToFlexUp(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIReadOff, true, true)
	startPhaseI += 10
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateFailureTraffic(t, trafficStats, util.READ, startPhaseI+3, invalidPhaseI-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseI+3, invalidPhaseI-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseI+3, invalidPhaseI-3)

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
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}

	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterRestart+3, trafficStopped-3)

}

/*
FLEXUP-SETUP
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

**************************************
TestCutOver1TargetDBDown
**************************************
-------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | CUTOVER |
-------------------------------------------------------------------------------

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
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | down   |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |  HERADB_ONE |  HERADB_TWO            | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  0             | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be up
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |  HERADB_ONE |  HERADB_TWO            | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1TargetDBDown(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToFlexUp(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	start := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", start)
	util.MoveCutOverPhase(t, util.CutOverPhaseI, true, true)
	start += 10
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	end := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start+3, end-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, start+3, end-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, start+3, end-3)

	util.ShutDownDBService("HERADB_TWO", "herabox_secondary_srv", t)
	start = time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	end = time.Now().Unix()
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start+3, end-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, start+3, end-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, start+3, end-3)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 0

	util.KillSessionAndValidate(t, stateLog, "herabox_secondary_srv", true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	start = time.Now().Unix()
	time.Sleep(25 * time.Second)
	end = time.Now().Unix()
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start+3, end-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, start+3, end-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, start+3, end-3)

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
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

**************************************
TestCutOver1SourceDBDown
**************************************
-------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | CUTOVER |
-------------------------------------------------------------------------------

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
    | HERADB_ONE     |  25             | herabox_primary_srv   | down   |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |  HERADB_ONE |  HERADB_TWO            | down   |
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
    5.1 OCC Container should be up
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |             | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1SourceDBDown(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToFlexUp(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	start := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", start)
	util.MoveCutOverPhase(t, util.CutOverPhaseI, true, true)
	start += 10
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	end := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start+3, end-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, start+3, end-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, start+3, end-3)

	util.ShutDownDBService("HERADB_ONE", "herabox_primary_srv", t)
	start = time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", false, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	end = time.Now().Unix()
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start+3, end-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, start+3, end-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, start+3, end-3)

	stateLog["occ"] = 0
	stateLog["occ.live1"] = 25

	util.KillSessionAndValidate(t, stateLog, "herabox_primary_srv", false)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	start = time.Now().Unix()
	time.Sleep(25 * time.Second)
	end = time.Now().Unix()
	util.ValidateFailureTraffic(t, trafficStats, util.READ, start+3, end-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, start+3, end-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, start+3, end-3)

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
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

**************************************
TestCutOver1Rollback
**************************************
-------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | CUTOVER |
-------------------------------------------------------------------------------

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
    | READ         |  HERADB_ONE |  HERADB_TWO            | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

Moving to FLEXUP PHASE
-------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP     |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP     |
-------------------------------------------------------------------------------

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

Moving to ENABLE PHASE
-------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | ENABLE  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | ENABLE  |
-------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  1             | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  1              | herabox_secondary_srv | active |
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
func TestCutOver1Rollback(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToFlexUp(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	start := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", start)
	util.MoveCutOverPhase(t, util.CutOverPhaseI, true, true)
	start += 10
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	end := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start+3, end-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, start+3, end-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, start+3, end-3)

	util.MoveCutOverPhase(t, util.FlexUp, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	start = time.Now().Unix()
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	end = time.Now().Unix()

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start+3, end-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, start+3, end-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, start+3, end-3, 1, 2)

	util.MoveCutOverPhase(t, util.CutOverEnable, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	start = time.Now().Unix()
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 2
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 2, t)
	end = time.Now().Unix()

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start+3, end-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, start+3, end-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, start+3, end-3, 1, 2)

	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}

}

/*
FLEXUP-SETUP
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

**************************************
TestCutOver1ClosingPendingTxn
**************************************
1. Sending Long read and Long Txn (long is 15 seconds here)
2. then move to CUTOVER STATE
-------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | CUTOVER |
-------------------------------------------------------------------------------
3. Validate txns throws error with - OCC-101: saturation kill

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
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1ClosingPendingTxn(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToFlexUp(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	util.CT.StopClientTraffic(respChan, RespMsg)

	txnStart := time.Now().Unix()
	var wg sync.WaitGroup
	wg.Add(1)
	go util.CT.LongTxnTraffic(&wg, respChan, RespMsg, 10, 15, t)
	logger2.GetLogger().Log(logger2.Alert, "Main Waiting for Lock ")
	<-RespMsg
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", txnStart)
	util.MoveCutOverPhase(t, util.CutOverPhaseI, true, true)

	trafficStats := <-respChan
	wg.Wait()
	txnEnd := time.Now().Unix()

	util.ValidateFailureTraffic(t, trafficStats, util.TXN, txnStart, txnEnd)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}
}
