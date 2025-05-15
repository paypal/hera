package main

import (
	"github.com/paypal/hera/tests/util"
	"github.com/paypal/hera/utility/logger"
	"os"
	"sync"
	"testing"
	"time"
)

func moveToEnableState(t *testing.T) (chan map[int64]util.ClientTrafficStats, chan map[int64]util.ClientTrafficStats, chan string, *os.File) {
	_, logFile := util.Setup(t)
	stateLog := make(map[string]int)

	// enable cut over env and tns changes
	util.EnableCutOver(t, false, false)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnable, true, true)
	util.RestartOCC(t, 90)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 2, t)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 2
	util.ValidateStateLog(t, stateLog, true)
	var wg sync.WaitGroup
	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)

	return dumpChan, respChan, RespMsg, logFile
}

/*
FLEXUP-SETUP
------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase  |
------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | ENABLE |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | ENABLE |
------------------------------------------------------------------------------

**************************************
TestFlexUpSourceDBDown
**************************************
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

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
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO    | down   |
    | WRITE        |  HERADB_ONE | HERADB_TWO    | down   |
    | TXN          |  HERADB_ONE | HERADB_TWO    | down   |
    -------------------------------------------------------
 4. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be up and failing to connect
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestFlexUpSourceDBDown(t *testing.T) {
	_, logFile := util.Setup(t)

	var wg sync.WaitGroup
	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	beforeStart := time.Now().Unix() + 2
	logger.GetLogger().Log(logger.Alert, "Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)
	afterComplete := time.Now().Unix() - 3
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, beforeStart, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, beforeStart, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, beforeStart, afterComplete, 1, 2)

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

	startClientTraffic := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Moving from Enable to Flexup state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.FlexUp, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	beforeSourceDBGoesDown := time.Now().Unix()
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)

	logger.GetLogger().Log(logger.Alert, "Shutting down source database")
	util.ShutDownDBService("HERADB_ONE", "herabox_primary_srv", t)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 20 seconds")
	shutdownPrimary := time.Now().Unix()
	time.Sleep(20 * time.Second)
	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}
	afterSourceDBGoesDown := time.Now().Unix()

	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)

	util.KillSessions(t, false, "herabox_primary_srv")
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	afterKill := time.Now().Unix()
	time.Sleep(15 * time.Second)

	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterKill+3, afterSourceDBGoesDown-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterKill+3, afterSourceDBGoesDown-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterKill+3, afterSourceDBGoesDown-3)

	util.RestartOCC(t, 90)

	occStatus = util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}

}

/*
FLEXUP-SETUP
------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase  |
------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | ENABLE |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | ENABLE |
------------------------------------------------------------------------------

**************************************
TestFlexUpTargetDBDown
**************************************
-----------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP   |
-----------------------------------------------------------------------------

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
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO    | active |
    | WRITE        |  HERADB_ONE | HERADB_TWO    | active |
    | TXN          |  HERADB_ONE | HERADB_TWO    | active |
    -------------------------------------------------------
 4. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be up
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO    | active |
    | WRITE        |  HERADB_ONE | HERADB_TWO    | active |
    | TXN          |  HERADB_ONE | HERADB_TWO    | active |
    -------------------------------------------------------

* start target DB

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

TODO: Need to add logs and CAL log verification
*/
func TestFlexUpTargetDBDown(t *testing.T) {
	_, logFile := util.Setup(t)

	var wg sync.WaitGroup

	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

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
	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	logger.GetLogger().Log(logger.Alert, "Sleeping for 5 seconds")
	time.Sleep(5 * time.Second)
	startClientTraffic := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Moving from Enable to Flexup state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.FlexUp, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	beforeSourceDBGoesDown := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)

	logger.GetLogger().Log(logger.Alert, "Shutting down target database")
	util.ShutDownDBService("HERADB_TWO", "herabox_secondary_srv", t)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 20 seconds")
	shutdownPrimary := time.Now().Unix()
	time.Sleep(20 * time.Second)
	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}
	afterSourceDBGoesDown := time.Now().Unix()

	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)

	beforeKill := time.Now().Unix()

	util.KillSessions(t, true, "herabox_secondary_srv")
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 0
	util.KillSessionAndValidate(t, stateLog, "herabox_secondary_srv", true)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, -1, t)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, afterSourceDBGoesDown, beforeKill-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, afterSourceDBGoesDown, beforeKill-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, afterSourceDBGoesDown, beforeKill-3, 1, 2)

	logger.GetLogger().Log(logger.Alert, "Starting down target database")
	util.StartDBService("HERADB_TWO", "herabox_secondary_srv", t)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)

}

/*
FLEXUP-SETUP
------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase  |
------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | ENABLE |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | ENABLE |
------------------------------------------------------------------------------

**************************************
TestFlexUpUniqNameInCorrect
**************************************
-------------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname           | r_status | w_status | phase |
-------------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE_INVALID | Y        | Y        | FLEXUP   |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO_INVALID | N        | N        | FLEXUP   |
-------------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_STG1 |  25            | init+schd        |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  0              | herabox_secondary_srv | active |
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
    | CLOC_STG1 |  25            | init+schd        |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be up and failing to connect
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active | TODO Failing
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active | TODO Failing
    | TXN          |             | HERADB_ONE, HERADB_TWO | active | TODO Failing
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestFlexUpUniqNameInCorrect(t *testing.T) {
	_, logFile := util.Setup(t)

	var wg sync.WaitGroup

	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

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
	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	logger.GetLogger().Log(logger.Alert, "Sleeping for 5 seconds")
	time.Sleep(5 * time.Second)
	startClientTraffic := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Moving from Enable to Flexup state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.CutOverFlexUpInValidUniqName, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1.not_connected"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 0, t)
	beforeSessionKill := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, beforeSessionKill-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, beforeSessionKill-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, beforeSessionKill-3, 1, 2)

	stateLog["occ"] = 25
	stateLog["occ.live1.not_connected"] = 25
	util.RestartOCC(t, 90)
	afterRestart := time.Now().Unix()

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}
	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)
	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterRestart+3, trafficStopped-3)
}

/*
FLEXUP-SETUP
------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase  |
------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | ENABLE |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | ENABLE |
------------------------------------------------------------------------------

**************************************
TestFlexUpUniqNameInCorrectDestDB
**************************************
only in secondary table
--------------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname           | r_status | w_status | phase  |
--------------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE_INVALID | Y        | Y        | ENABLE |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO_INVALID | N        | N        | ENABLE |
--------------------------------------------------------------------------------------

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
    5.1 OCC Container should be up and failing to connect
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         | HERADB_ONE	 | HERADB_TWO    | active |
    | WRITE        | HERADB_ONE  | HERADB_TWO    | active |
    | TXN          | HERADB_ONE  | HERADB_TWO    | active |
    -------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestFlexUpUniqNameInCorrectDestDB(t *testing.T) {
	_, logFile := util.Setup(t)

	var wg sync.WaitGroup

	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

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
	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	logger.GetLogger().Log(logger.Alert, "Sleeping for 5 seconds")
	time.Sleep(5 * time.Second)
	startClientTraffic := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Moving from Enable to FLEXUP Cutover state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.CutOverFlexUpInValidUniqName, false, true)
	util.MoveCutOverPhase(t, util.FlexUp, true, false)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	beforeSessionKill := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, beforeSessionKill-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, beforeSessionKill-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, beforeSessionKill-3, 1, 2)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.KillSessionAndValidate(t, stateLog, "herabox_primary_srv", false)
	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}

	util.CT.StopClientTraffic(respChan, RespMsg)
}

/*
FLEXUP-SETUP
------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase  |
------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | ENABLE |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | ENABLE |
------------------------------------------------------------------------------

**************************************
TestFlexUpUniqNameInCorrectDestDB
**************************************
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

Rollback to Enable mode
------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase  |
------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | ENABLE |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | ENABLE |
-----------------------------------------------------------------------------

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
func TestFlexUpRollback(t *testing.T) {
	_, logFile := util.Setup(t)

	var wg sync.WaitGroup

	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

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

	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	logger.GetLogger().Log(logger.Alert, "Sleeping for 5 seconds")
	time.Sleep(5 * time.Second)
	startClientTraffic := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Moving from Enable to FLEXUP Cutover state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.FlexUp, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	beforeRollback := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, beforeRollback-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, beforeRollback-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, beforeRollback-3, 1, 2)

	util.MoveCutOverPhase(t, util.CutOverEnable, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 2, t)
	stateLog["occ"] = 25
	stateLog["occ.live1"] = 2
	util.ValidateStateLog(t, stateLog, true)

	util.ResetOCCDocker(t)
	util.OCCConfig(t, "readonly_children_pct", "0", "/x/web/LIVE/occ/occ.cdb")
	util.RestartOCC(t, 60)

	stateLog = make(map[string]int)
	stateLog["occ"] = 25
	util.ValidateStateLog(t, stateLog, true)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 0, t)

	util.CT.StopClientTraffic(respChan, RespMsg)
}

/*
FLEXUP-SETUP
-------------------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    | wisb_role|
-------------------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP      | CLOC_RW  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP      | CLOC_RO  |
-------------------------------------------------------------------------------------------

**************************************
TestCutOver1ClosingPendingTxn
**************************************
1. Change the Role in table and remove RW give RO role
2. Check of the write traffic stated failing
-------------------------------------------------------------------------------------------
| ROWS | occ_name | OCC_TNS_ALIAS | db_uname   | r_status | w_status | phase    | wisb_role|
-------------------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | FLEXUP      | CLOC_RO  |
| Row2 | occ      | CLOC_STG1 | HERADB_TWO | N        | N        | FLEXUP      | CLOC_RO  |
-------------------------------------------------------------------------------------------

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
func TestFlexUpValidatingRORoleCheck(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToEnableState(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)
	util.MoveCutOverPhase(t, util.CutOverFlexUpInCorrectRole, true, true)
	afterRoleChange := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	stateLog := make(map[string]int)

	stateLog["occ"] = 25
	stateLog["occ.live1"] = 25
	util.ValidateStateLog(t, stateLog, true)
	stopTime := time.Now().Unix()
	trafficStats := util.CT.StopClientTraffic(respChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, afterRoleChange+3, stopTime-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, afterRoleChange+3, stopTime-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, afterRoleChange+3, stopTime-3, 1, 2)

}
