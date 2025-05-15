package main

import (
	"github.com/paypal/hera/tests/util"
	logger2 "github.com/paypal/hera/utility/logger"
	"os"
	"sync"
	"testing"
	"time"
)

func moveToCutOverPhaseII(t *testing.T) (chan map[int64]util.ClientTrafficStats, chan map[int64]util.ClientTrafficStats, chan string, *os.File) {
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
	cutOverFlexupState := time.Now().Unix()
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic+3, cutOverFlexupState-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic+3, cutOverFlexupState-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic+3, cutOverFlexupState-3, 1, 2)

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

	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to cut over phase 2 state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.CutOverPhaseII, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	cutOverPhase2State := time.Now().Unix()
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	cutOverPhase2End := time.Now().Unix()
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, cutOverPhase2State+3, cutOverPhase2End-3, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, cutOverPhase2State+3, cutOverPhase2End-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, cutOverPhase2State+3, cutOverPhase2End-3)

	return dumpChan, respChan, RespMsg, logFile
}

/*
FLEXUP-SETUP
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver3InvalidNoOfRow
**************************************
1. Delete HERADB_ONE row when moving from cutover phase 2 to 3

--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | Y        | CUTOVER  |
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
    ---------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB         | state  |
    ---------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO            | active |
    | WRITE        |             | HERADB_TWO,HERADB_ONE | active |
    | TXN          |             | HERADB_TWO,HERADB_ONE | active |
    ---------------------------------------------------------------

Worker validation after killing random sessions

 1. Worker validation after killing random sessions
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after killing random sessions
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------

3. OCC should be down after restarting OCC and all client calls should fail

TODO: Need to add logs and CAL log verification
*/
func TestCutOver3InvalidNoOfRow(t *testing.T) {
	dumpChan, respChan, RespChan, logFile := moveToCutOverPhaseII(t)
	defer util.TearDown(t, dumpChan, respChan, RespChan, logFile)

	stateLog := make(map[string]int)

	startPhaseIII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase III: ", startPhaseIII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIIInvalidRowCount, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseIII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespChan)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseIII, invalidPhaseIII-3, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseIII, invalidPhaseIII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseIII, invalidPhaseIII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)

	afterRestart := util.RestartOCC(t, 90)

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
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver3InvalidDBUniqueName
**************************************
1. update db_uname with invalid value when moving from cutover phase 2 to 3
----------------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname           | r_status | w_status | phase    |
----------------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE_INVALID | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO_INVALID | Y        | Y        | CUTOVER  |
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
    ---------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB         | state  |
    ---------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO            | active |
    | WRITE        |             | HERADB_TWO,HERADB_ONE | active |
    | TXN          |             | HERADB_TWO,HERADB_ONE | active |
    ---------------------------------------------------------------

Worker validation after killing random sessions

 1. Worker validation after killing random sessions
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after killing random sessions
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------

3. OCC should be down after restarting OCC and all client calls should fail

TODO: Need to add logs and CAL log verification
*/
func TestCutOver3InvalidDBUniqueName(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseIII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase III: ", startPhaseIII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIIInvalidDBUniqName, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 30 seconds")
	time.Sleep(30 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 0
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 0, t)
	invalidPhaseIII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateFailureTraffic(t, trafficStats, util.READ, startPhaseIII+15, invalidPhaseIII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseIII+15, invalidPhaseIII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseIII+15, invalidPhaseIII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 0
	util.ValidateStateLog(t, stateLog, true)

	afterRestart := util.RestartOCC(t, 90)

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
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver3InvalidOCCName
**************************************
1. update occ_name with invalid value when moving from cutover phase 2 to 3
-----------------------------------------------------------------------------------
| ROWS | occ_name     | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
-----------------------------------------------------------------------------------
| Row1 | occ-invalid  | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ-invalid  | CLOC_STG1 | HERADB_TWO | Y        | Y        | CUTOVER  |
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
    ---------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB         | state  |
    ---------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO            | active |
    | WRITE        |             | HERADB_TWO,HERADB_ONE | active |
    | TXN          |             | HERADB_TWO,HERADB_ONE | active |
    ---------------------------------------------------------------

Worker validation after killing random sessions

 1. Worker validation after killing random sessions
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after killing random sessions
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------

3. OCC should be down after restarting OCC and all client calls should fail

TODO: Need to add logs and CAL log verification
*/
func TestCutOver3InvalidOCCName(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseIII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase III: ", startPhaseIII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIIInvalidOCCName, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseIII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseIII, invalidPhaseIII-3, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseIII, invalidPhaseIII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseIII, invalidPhaseIII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)

	afterRestart := util.RestartOCC(t, 90)

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
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver3InvalidTwoTask
**************************************
1. update OCC_TNS_ALIAS with invalid value when moving from cutover phase 2 to 3
-----------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS     | db_uname   | r_status | w_status | phase    |
-----------------------------------------------------------------------------------
| Row1 | occ      | TWO_TASK_INVALID | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | TWO_TASK_INVALID | HERADB_TWO | Y        | Y        | CUTOVER  |
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
    ---------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB         | state  |
    ---------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO            | active |
    | WRITE        |             | HERADB_TWO,HERADB_ONE | active |
    | TXN          |             | HERADB_TWO,HERADB_ONE | active |
    ---------------------------------------------------------------

Worker validation after killing random sessions

 1. Worker validation after killing random sessions
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after killing random sessions
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------

3. OCC should be down after restarting OCC and all client calls should fail

TODO: Need to add logs and CAL log verification
*/
func TestCutOver3InvalidTwoTask(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseIII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase III: ", startPhaseIII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIIInvalidTwoTask, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseIII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseIII, invalidPhaseIII-3, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseIII, invalidPhaseIII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseIII, invalidPhaseIII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)

	afterRestart := util.RestartOCC(t, 90)

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
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver3InvalidCutOverPhase
**************************************
1. update phase with invalid value when moving from cutover phase 2 to 3
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | invalid  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | Y        | invalid  |
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
    ---------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB         | state  |
    ---------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO            | active |
    | WRITE        |             | HERADB_TWO,HERADB_ONE | active |
    | TXN          |             | HERADB_TWO,HERADB_ONE | active |
    ---------------------------------------------------------------

Worker validation after killing random sessions

 1. Worker validation after killing random sessions
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after killing random sessions
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------

3. OCC should be down after restarting OCC and all client calls should fail

TODO: Need to add logs and CAL log verification
*/
func TestCutOver3InvalidCutOverPhase(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseIII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase III: ", startPhaseIII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIIInvalidPhase, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseIII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseIII, invalidPhaseIII-3, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseIII, invalidPhaseIII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseIII, invalidPhaseIII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)

	afterRestart := util.RestartOCC(t, 90)

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
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver3InvalidReadStatus
**************************************
1. update r_status with invalid value when moving from cutover phase 2 to 3
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | X        | N        | invalid  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | X        | Y        | invalid  |
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
    ---------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB         | state  |
    ---------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO            | active |
    | WRITE        |             | HERADB_TWO,HERADB_ONE | active |
    | TXN          |             | HERADB_TWO,HERADB_ONE | active |
    ---------------------------------------------------------------

Worker validation after killing random sessions

 1. Worker validation after killing random sessions
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after killing random sessions
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------

3. OCC should be down after restarting OCC and all client calls should fail

TODO: Need to add logs and CAL log verification
*/
func TestCutOver3InvalidReadStatus(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseIII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase III: ", startPhaseIII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIIInvalidRead, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseIII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseIII+5, invalidPhaseIII-3, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseIII, invalidPhaseIII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseIII, invalidPhaseIII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)

	afterRestart := util.RestartOCC(t, 90)

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
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver3InvalidWriteStatus
**************************************
1. update w_status with invalid value when moving from cutover phase 2 to 3
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | X        | invalid  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | X        | invalid  |
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
    ---------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB         | state  |
    ---------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO            | active |
    | WRITE        |             | HERADB_TWO,HERADB_ONE | active |
    | TXN          |             | HERADB_TWO,HERADB_ONE | active |
    ---------------------------------------------------------------

Worker validation after killing random sessions

 1. Worker validation after killing random sessions
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after killing random sessions
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------

3. OCC should be down after restarting OCC and all client calls should fail

TODO: Need to add logs and CAL log verification
*/
func TestCutOver3InvalidWriteStatus(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseIII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase III: ", startPhaseIII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIIInvalidWrite, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseIII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseIII, invalidPhaseIII-3, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseIII, invalidPhaseIII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseIII, invalidPhaseIII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)

	afterRestart := util.RestartOCC(t, 90)

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
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver3DualWriteStatus
**************************************
1. update w_status with Y in both rows when moving from cutover phase 2 to 3
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | Y        | invalid  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | Y        | invalid  |
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
    ---------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB         | state  |
    ---------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO            | active |
    | WRITE        |             | HERADB_TWO,HERADB_ONE | active |
    | TXN          |             | HERADB_TWO,HERADB_ONE | active |
    ---------------------------------------------------------------

Worker validation after killing random sessions

 1. Worker validation after killing random sessions
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after killing random sessions
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------

3. OCC should be down after restarting OCC and all client calls should fail

TODO: Need to add logs and CAL log verification
*/
func TestCutOver3DualWriteStatus(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseIII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase III: ", startPhaseIII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIIDualWrite, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseIII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseIII, invalidPhaseIII-3, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseIII, invalidPhaseIII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseIII, invalidPhaseIII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)

	afterRestart := util.RestartOCC(t, 90)

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
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver3DualReadStatus
**************************************
1. update r_status with Y value in both rows when moving from cutover phase 2 to 3
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | invalid  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | Y        | invalid  |
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
    ---------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB         | state  |
    ---------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO            | active |
    | WRITE        |             | HERADB_TWO,HERADB_ONE | active |
    | TXN          |             | HERADB_TWO,HERADB_ONE | active |
    ---------------------------------------------------------------

Worker validation after killing random sessions

 1. Worker validation after killing random sessions
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after killing random sessions
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------

3. OCC should be down after restarting OCC and all client calls should fail

TODO: Need to add logs and CAL log verification
*/
func TestCutOver3DualReadStatus(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseIII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase III: ", startPhaseIII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIIDualRead, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseIII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseIII+5, invalidPhaseIII-3, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseIII, invalidPhaseIII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseIII, invalidPhaseIII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)

	afterRestart := util.RestartOCC(t, 90)

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
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver3ReadOffStatus
**************************************
1. update r_status with N value in both rows when moving from cutover phase 2 to 3
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | invalid  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | Y        | invalid  |
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
    ---------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB         | state  |
    ---------------------------------------------------------------
    | READ         |             | HERADB_ONE,HERADB_TWO | active |
    | WRITE        |             | HERADB_TWO,HERADB_ONE | active |
    | TXN          |             | HERADB_TWO,HERADB_ONE | active |
    ---------------------------------------------------------------

Worker validation after killing random sessions

 1. Worker validation after killing random sessions
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after killing random sessions
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------

3. OCC should be down after restarting OCC and all client calls should fail

TODO: Need to add logs and CAL log verification
*/
func TestCutOver3ReadOffStatus(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseIII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase III: ", startPhaseIII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIIReadOff, true, true)
	startPhaseIII += 10
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseIII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateFailureTraffic(t, trafficStats, util.READ, startPhaseIII+5, invalidPhaseIII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseIII+5, invalidPhaseIII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseIII+5, invalidPhaseIII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)

	afterRestart := util.RestartOCC(t, 90)

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
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver3SourceDBDown
**************************************
1. shutdown source db when moving from cutover phase 2 to 3
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | invalid  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | Y        | invalid  |
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
    ---------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB         | state  |
    ---------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO            | active |
    | WRITE        |             | HERADB_TWO,HERADB_ONE | active |
    | TXN          |             | HERADB_TWO,HERADB_ONE | active |
    ---------------------------------------------------------------

Worker validation after killing random sessions

 1. Worker validation after killing random sessions
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after killing random sessions
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------

3. OCC should be up after restarting OCC and all client calls should fail

TODO: Need to add logs and CAL log verification
*/
func TestCutOver3SourceDBDown(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	util.ShutDownDBService("HERADB_ONE", "herabox_primary_srv", t)
	stateLog := make(map[string]int)

	startPhaseIII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase III: ", startPhaseIII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIII, true, true)
	startPhaseIII += 10
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", false, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseIII+5, invalidPhaseII-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseIII+5, invalidPhaseII-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseIII+5, invalidPhaseII-3, 2, 1)

	util.KillSessions(t, true, "herabox_primary_srv")

	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)

	afterRestart := util.RestartOCC(t, 90)

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
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------

**************************************
TestCutOver3TargetDBDown
**************************************
1. shutdown target db when moving from cutover phase 2 to 3
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | invalid  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | Y        | invalid  |
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
    ---------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB         | state  |
    ---------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO            | active |
    | WRITE        |             | HERADB_TWO,HERADB_ONE | active |
    | TXN          |             | HERADB_TWO,HERADB_ONE | active |
    ---------------------------------------------------------------

Worker validation after killing all sessions

 1. Worker validation after killing all sessions
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  0             | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after killing random sessions
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  0              | herabox_secondary_srv | active |
    ---------------------------------------------------------------------

3. OCC should be up after restarting OCC and all client calls should fail

TODO: Need to add logs and CAL log verification
*/
func TestCutOver3TargetDBDown(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	util.ShutDownDBService("HERADB_TWO", "herabox_secondary_srv", t)
	stateLog := make(map[string]int)

	startPhaseIII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase III: ", startPhaseIII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIII, true, true)
	startPhaseIII += 10
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", false, 25, t)
	invalidPhaseII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseIII+5, invalidPhaseII-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseIII+5, invalidPhaseII-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseIII+5, invalidPhaseII-3, 2, 1)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 0

	util.KillSessionAndValidate(t, stateLog, "herabox_secondary_srv", true)

	afterRestart := util.RestartOCC(t, 90)

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
-------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER |
-------------------------------------------------------------------------------

**************************************
TestCutOver3Rollback
**************************************

--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | Y        | CUTOVER  |
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
    --------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB  | state  |
    --------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE     | active |
    | WRITE        |  HERADB_TWO | HERADB_ONE     | active |
    | TXN          |  HERADB_TWO | HERADB_ONE     | active |
    --------------------------------------------------------

# ROLLBACK TO CUTOVER PHASE II

--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
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
    --------------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB              | state  |
    --------------------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE                 | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO     | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO     | active |
    --------------------------------------------------------------------

# ROLLBACK TO CUTOVER PHASE I

--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
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
    --------------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB              | state  |
    --------------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO                 | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO     | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO     | active |
    --------------------------------------------------------------------

# ROLLBACK TO FLEXUP

--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP      |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP      |
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
    --------------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB  | state  |
    --------------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO     | active |
    | WRITE        |  HERADB_ONE | HERADB_TWO     | active |
    | TXN          |  HERADB_ONE | HERADB_TWO     | active |
    --------------------------------------------------------------------

# ROLLBACK TO ENABLE

--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | ENABLE   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | ENABLE   |
--------------------------------------------------------------------------------

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
    --------------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB  | state  |
    --------------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO     | active |
    | WRITE        |  HERADB_ONE | HERADB_TWO     | active |
    | TXN          |  HERADB_ONE | HERADB_TWO     | active |
    --------------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOver3Rollback(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseIII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase III: ", startPhaseIII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIII, true, true)
	startPhaseIII += 10
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	phaseII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseIII+7, phaseII-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseIII+7, phaseII-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseIII+7, phaseII-3, 2, 1)

	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase II: ", phaseII)
	util.MoveCutOverPhase(t, util.CutOverPhaseII, true, true)
	phaseII = time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	phaseI := time.Now().Unix()

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, phaseII+7, phaseI-3, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, phaseII+7, phaseI-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, phaseII+7, phaseI-3)

	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", phaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseI, true, true)
	phaseI = time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(20 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	flexUpMode := time.Now().Unix()

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, phaseI+7, flexUpMode-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, phaseI+7, flexUpMode-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, phaseI+7, flexUpMode-3)

	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover FLEXUP Mode: ", phaseI)
	util.MoveCutOverPhase(t, util.FlexUp, true, true)
	flexUpMode = time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	enableMode := time.Now().Unix()

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, flexUpMode+7, enableMode-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, flexUpMode+7, enableMode-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, flexUpMode+7, enableMode-3, 1, 2)

	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Enable Mode: ", enableMode)
	util.MoveCutOverPhase(t, util.CutOverEnable, true, true)
	enableMode = time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 2
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 2, t)
	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, enableMode+7, trafficStopped-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, enableMode+7, trafficStopped-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, enableMode+7, trafficStopped-3, 1, 2)

}

/*
FLEXUP-SETUP
-------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | N        | CUTOVER |
-------------------------------------------------------------------------------

**************************************
TestCutOver3RoleOrderChange
**************************************
First update the table and then wait for 10 sec and grant RW role to cutover db
--------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | Y        | Y        | CUTOVER  |
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
    --------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB  | state  |
    --------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE     | active |
    | WRITE        |  HERADB_TWO | HERADB_ONE     | active |
    | TXN          |  HERADB_TWO | HERADB_ONE     | active |
    --------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOver3RoleOrderChange(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseIII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase III: ", startPhaseIII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIIWithoutRole, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	util.GiveRWToSecondary(t)
	roleChange := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Given RW role to secondary table: ", roleChange)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	trafficStop := time.Now().Unix()
	trafficStats := util.CT.StopClientTraffic(respChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, roleChange+3, trafficStop-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, roleChange+3, trafficStop-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, roleChange+3, trafficStop-3, 2, 1)

}
