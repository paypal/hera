package main

import (
	"github.com/paypal/hera/tests/util"
	"github.com/paypal/hera/utility/logger"
	"sync"
	"testing"
	"time"
)

func TestCutOverWithReadWriteSplit(t *testing.T) {

	// bring the setup to initial state
	//OCC running with only one db in TNS, no env set for cut over, cut over table is empty
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
	logger.GetLogger().Log(logger.Alert, "Moving from Enable to Flexup state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.FlexUp, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
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

	logger.GetLogger().Log(logger.Alert, "Moving from Pre to Cutover state(stopping write in main DB): ", beforeCutOverStart)
	util.MoveCutOverPhase(t, util.CutOverPhaseI, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	afterServiceStop := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Moved to Cutover state(stopped write in main db): ", afterServiceStop)
	stateLog["occ.w"] = 13
	stateLog["occ.r"] = 12
	stateLog["occ.w.live1"] = 13
	stateLog["occ.r.live1"] = 12
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, beforeCutOverStart, afterServiceStop-3, 1, 2)

	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterServiceStop, time.Now().Unix()-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterServiceStop, time.Now().Unix()-3)

	logger.GetLogger().Log(logger.Alert, "Moving Read to CutOver: ", time.Now().Unix())
	util.MoveCutOverPhase(t, util.CutOverPhaseII, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	afterReadCutOver := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Moved Read to Cutover database: ", afterReadCutOver)
	stateLog["occ.w"] = 13
	stateLog["occ.r"] = 12
	stateLog["occ.w.live1"] = 13
	stateLog["occ.r.live1"] = 12
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	readCutOverValidation := time.Now().Unix() - 3
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, afterReadCutOver, readCutOverValidation, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterServiceStop, readCutOverValidation)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterServiceStop, readCutOverValidation)

	logger.GetLogger().Log(logger.Alert, "Moving Write to CutOver: ", time.Now().Unix())
	util.MoveCutOverPhase(t, util.CutOverPhaseIII, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	afterWriteCutOver := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Moved Write to Cutover database: ", afterWriteCutOver)
	stateLog["occ.w"] = 13
	stateLog["occ.r"] = 12
	stateLog["occ.w.live1"] = 13
	stateLog["occ.r.live1"] = 12
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	writeCutOverValidation := time.Now().Unix() - 3
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, readCutOverValidation, writeCutOverValidation, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, afterWriteCutOver, writeCutOverValidation, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, afterWriteCutOver, writeCutOverValidation, 2, 1)

	util.CT.StopClientTraffic(respChan, RespMsg)
}
