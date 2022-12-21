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
	"flag"
	"fmt"
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
	processPanicSignal := make(chan os.Signal, 1)
	mux_process_id := syscall.Getpid()
	signal.Notify(processPanicSignal, syscall.SIGTERM, syscall.SIGABRT, syscall.SIGHUP, syscall.SIGINT)

	//Go routine will listen on MUX dealth signal and cleanup its child resources
	go func() {
		logger.GetLogger().Log(logger.Alert, fmt.Sprintf("Mux Added Signal listener for MUX process: %d", mux_process_id))
		sig := <-processPanicSignal //When it receives death signal
		logger.GetLogger().Log(logger.Alert, fmt.Sprintf("Receiced terminate signal: %s for MUX: %d", sig.String(), mux_process_id))
		signal.Stop(processPanicSignal)
		err := syscall.Kill(-mux_process_id, syscall.SIGTERM)
		if err != nil {
			logger.GetLogger().Log(logger.Alert, fmt.Sprintf("Failed to reeleasing MUX process: %d, and error is: %s", mux_process_id, err))
		}
	}()

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

	err := InitConfig()
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

	GetStateLog().SetStartTime(time.Now())

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

	<-GetWorkerBrokerInstance().Stopped()

	//
	// calling releasectxresource right before exit only serves as an example on how
	// to release resources allocated by cal for a given thread group, which in
	// this case is thread group calDefaultThreadGroupName.
	//
	cal.ReleaseCxtResource()
}
