// Copyright 2019 PayPal Inc.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lib

import (
	"context"
	"flag"
	"fmt"
	otellogger "github.com/paypal/hera/utility/logger/otel"
	otelconfig "github.com/paypal/hera/utility/logger/otel/config"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
)

// Run is practically the main function of the mux. It performs various the intializations, spawns server.Run -
// the "infinite loop" as a goroutine and waits on the worker broker channel for the signal to exit
func Run() {
	signal.Ignore(syscall.SIGPIPE)
	mux_process_id := syscall.Getpid()

	namePtr := flag.String("name", "", "module name in v$session table")
	flag.Parse()

	/* Don't log.
	We haven't configured log level, so lots goes to stdout/err log. */
	if len(*namePtr) == 0 {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "missing --name parameter")
		}
		FullShutdown()
	}

	rand.Seed(time.Now().Unix())

	err := InitConfig(*namePtr)
	if err != nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "failed to initialize configuration:", err.Error())
		}
		FullShutdown()
	}
	pidfile, err := os.Create(GetConfig().MuxPidFile)
	if err != nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "Can't open", GetConfig().MuxPidFile, err.Error())
		}
		FullShutdown()
	} else {
		pidfile.WriteString(fmt.Sprintf("%d\n", os.Getpid()))
	}
	MkErr(GetConfig().ErrorCodePrefix)

	os.Setenv("MUX_START_TIME_SEC", fmt.Sprintf("%d", time.Now().Unix()))
	os.Setenv("MUX_START_TIME_USEC", "0")

	//
	// worker also initialize a calclent with the same poolname using threadid==0 in
	// its bootstrap label message. if we let worker fire off its msg first, all proxy
	// messages will end up in the same swimminglane since that is what id(0) does.
	// so, let's send the bootstrap label message from proxy first using threadid==1.
	// that way, calmsgs with different threadids can end up in different swimminglanes,
	//
	caltxn := cal.NewCalTransaction(cal.TransTypeAPI, "mux-go", cal.TransOK, "", cal.DefaultTGName)
	caltxn.SetCorrelationID("abc")
	calclient := cal.GetCalClientInstance()
	if calclient != nil {
		release := calclient.GetReleaseBuildNum()
		if release != "" {
			evt := cal.NewCalEvent("VERSION", release, "0", "")
			evt.Completed()
		}
	}
	caltxn.Completed()

	//
	// create singleton broker and start worker/pools
	//
	nameForTns := *namePtr
	CfgFromTns(nameForTns)
	if (GetWorkerBrokerInstance() == nil) || (GetWorkerBrokerInstance().RestartWorkerPool(*namePtr) != nil) {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "failed to start hera worker")
		}
		FullShutdown()
	}

	caltxn = cal.NewCalTransaction(cal.TransTypeAPI, "mux-go-start", cal.TransOK, "", cal.DefaultTGName)
	caltxn.SetCorrelationID("runtxn")
	caltxn.Completed()

	//Initialize OTEL
	if otelconfig.OTelConfigData.Enabled {
		shutdownFunc, err := otellogger.Init(context.Background())
		if err != nil {
			logger.GetLogger().Log(logger.Alert, fmt.Sprintf("failed to initialize OTEL, err: %v", err))
			evt := cal.NewCalEvent("OTEL_INIT", *namePtr, "2", fmt.Sprintf("erro: %v", err))
			evt.Completed()
			if otelconfig.OTelConfigData.SkipCalStateLog {
				logger.GetLogger().Log(logger.Alert, fmt.Sprintf("OTEL initialization failed. Only the OTEL state-log has been enabled. It is not safe to start the server"))
				FullShutdown()
			}
		}
		GetStateLog().SetStartTime(time.Now())
		defer otellogger.StopMetricCollection()  //Stop sending metrics data
		defer shutdownFunc(context.Background()) //During exit from mux, this will takecare of OTEL providers clean-up
	} else {
		GetStateLog().SetStartTime(time.Now())
	}

	go func() {
		sleep := time.Duration(GetConfig().ConfigReloadTimeMs)
		for {
			time.Sleep(time.Millisecond * sleep)
			CheckOpsConfigChange()
		}
	}()

	CheckEnableProfiling()
	GoStats()

	RegisterLoopDriver(HandleConnection)
	if GetConfig().EnableQueryBindBlocker {
		InitQueryBindBlocker(*namePtr)
	}

	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, "Waiting for at least one database connection")
	}

	pool, err := GetWorkerBrokerInstance().GetWorkerPool(wtypeRW, 0, 0)
	if err != nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "failed to get pool WTYPE_RW, 0, 0:", err)
		}
		FullShutdown()
	}
	for {
		if pool.GetHealthyWorkersCount() > 0 {
			break
		} else {
			if GetConfig().EnableTAF {
				fallbackPool, err := GetWorkerBrokerInstance().GetWorkerPool(wtypeStdBy, 0, 0)
				if (err == nil) && (fallbackPool.GetHealthyWorkersCount() > 0) {
					break
				}
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
	var lsn Listener
	if GetConfig().KeyFile != "" {
		lsn = NewTLSListener(fmt.Sprintf("0.0.0.0:%d", GetConfig().Port))
	} else {
		lsn = NewTCPListener(fmt.Sprintf("0.0.0.0:%d", GetConfig().Port))
	}

	if GetConfig().EnableSharding {
		err = InitShardingCfg()
		if err != nil {
			if logger.GetLogger().V(logger.Alert) {
				logger.GetLogger().Log(logger.Alert, "failed to initialize sharding config:", err)
			}
			FullShutdown()
		}
	}
	InitRacMaint(*namePtr)

	srv := NewServer(lsn, HandleConnection)

	go srv.Run()

	//
	// calling releasectxresource right before exit only serves as an example on how
	// to release resources allocated by cal for a given thread group, which in
	// this case is thread group calDefaultThreadGroupName.
	//
	defer func() {
		cal.ReleaseCxtResource()
	}()

	// Defer release resource in case of any abnormal exit of for application
	defer handlePanicAndReleaseResource(mux_process_id)

	<-GetWorkerBrokerInstance().Stopped()
}

/*
 * When mux dies with any reason like death from explicit OS signal or due to any panic errors
 * then it will kills all mux children by using mux process ID and relase CAL resources.
 */
func handlePanicAndReleaseResource(mux_process_id int) {
	// detect if panic occurs or not
	panic_data := recover()
	if panic_data != nil {
		logger.GetLogger().Log(logger.Alert, fmt.Sprintf("Mux process: %d exited with panic: %s, so releasing its children and other resources", mux_process_id, panic_data))
		pgid, err := syscall.Getpgid(mux_process_id)
		if err != nil {
			pgid = mux_process_id
			logger.GetLogger().Log(logger.Alert, "Failed to fetch process group: ", err)
		}
		err = syscall.Kill(-pgid, syscall.SIGTERM)
		if err != nil {
			logger.GetLogger().Log(logger.Alert, fmt.Sprintf("Failed to reeleasing MUX process: %d, and error is: %s", mux_process_id, err))
		}
		logger.GetLogger().Log(logger.Alert, fmt.Sprintf("Successfully released resources related MUX process: %d", mux_process_id))
		<-GetWorkerBrokerInstance().Stopped()

		//
		// calling releasectxresource right before exit only serves as an example on how
		// to release resources allocated by cal for a given thread group, which in
		// this case is thread group calDefaultThreadGroupName.
		//
		cal.ReleaseCxtResource()
		os.Exit(1)
	}
}
