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
	namePtr := flag.String("name", "", "module name in v$session table")
	flag.Parse()

	/* Don't log.
	We haven't configured log level, so lots goes to stdout/err log. */
	if len(*namePtr) == 0 {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "missing --name parameter")
		}
		os.Exit(1)
	}

	rand.Seed(time.Now().Unix())

	err := InitConfig()
	if err != nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "failed to initialize configuration:", err.Error())
		}
		os.Exit(1)
	}
	pidfile, err := os.Create(GetConfig().MuxPidFile)
	if err != nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "Can't open", GetConfig().MuxPidFile, err.Error())
		}
		os.Exit(1)
	} else {
		pidfile.WriteString(fmt.Sprintf("%d\n", os.Getpid()))
	}

	os.Setenv("MUX_START_TIME_SEC", fmt.Sprintf("%d", time.Now().Unix()))
	os.Setenv("MUX_START_TIME_USEC", "0")

	//
	// occworker also initialize a calclent with the same poolname using threadid==0 in
	// its bootstrap label message. if we let occworker fire off its msg first, all proxy
	// messages will end up in the same swimminglane since that is what id(0) does.
	// so, let's send the bootstrap label message from proxy first using threadid==1.
	// that way, calmsgs with different threadids can end up in different swimminglanes,
	//
	caltxn := cal.NewCalTransaction(cal.TransTypeAPI, "occmux-go", cal.TransOK, "", cal.DefaultTGName)
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
	if (GetWorkerBrokerInstance() == nil) || (GetWorkerBrokerInstance().RestartWorkerPool(*namePtr) != nil) {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "failed to start occ worker")
		}
		os.Exit(11)
	}

	caltxn = cal.NewCalTransaction(cal.TransTypeAPI, "occmux-go-start", cal.TransOK, "", cal.DefaultTGName)
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

	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, "Waiting for at least one database connection")
	}

	pool, err := GetWorkerBrokerInstance().GetWorkerPool(wtypeRW, 0, 0)
	if err != nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "failed to get pool WTYPE_RW, 0, 0:", err)
		}
		os.Exit(12)
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
			os.Exit(13)
		}
	}
	InitRacMaint()

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
