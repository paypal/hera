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
	"github.com/paypal/hera/utility/logger"
	"net"
	"net/http"
	// for pprof have blank import for their init()
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
)

// CheckEnableProfiling check if "enable_profile" is true in config and enables the profiling:
// - 6060 port is open to stats via http: <hostname>:6060/debug/pprof/
// - 3030 port is open via telnet to manually start and stop CPU profile. For example, before starting some test,
//    write "s cpu.prof" and after finishing the tests you do "e", which will create the profile dump "cpu.prof"
//    (or whatever name via "s" command"). cpu.prof can then be read via the pprof tool
func CheckEnableProfiling() {
	if GetConfig().EnableProfile {
		go func() {
			err := http.ListenAndServe(":"+GetConfig().ProfileHTTPPort, nil)
			if (err != nil) && logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, "Cannot Listen on ", GetConfig().ProfileHTTPPort)
			}
		}()
		go func() {
			lc, err := net.Listen("tcp", ":"+GetConfig().ProfileTelnetPort)
			if err != nil {
				if logger.GetLogger().V(logger.Info) {
					logger.GetLogger().Log(logger.Info, "Cannot Listen on", GetConfig().ProfileTelnetPort)
				}
				return
			}
			for {
				cn, _ := lc.Accept()
				buff := make([]byte, 100)
				var file *os.File
				for {
					cn.Write([]byte("Syntax:\n\t's <profilefilename>' to start CPU profile\n\t'e' end CPU profile\n\t'b <blockprofilerate>' enable/disable block profile\n\tg runtime.GC()\n"))
					n, err := cn.Read(buff)
					if err != nil {
						break
					}
					switch {
					case buff[0] == 's':
						filename := string(buff[2 : n-2])
						file, err = os.Create(filename)
						if (err != nil) && logger.GetLogger().V(logger.Debug) {
							logger.GetLogger().Log(logger.Debug, "Could not create CPU profile: ", err)
						}
						if err := pprof.StartCPUProfile(file); err != nil {
							if logger.GetLogger().V(logger.Debug) {
								logger.GetLogger().Log(logger.Debug, "could not start CPU profile: ", err)
							}
						} else {
							if logger.GetLogger().V(logger.Debug) {
								logger.GetLogger().Log(logger.Debug, "started profile in ", filename)
							}
						}
					case buff[0] == 'e':
						pprof.StopCPUProfile()
						file.Close()
						if logger.GetLogger().V(logger.Debug) {
							logger.GetLogger().Log(logger.Debug, "CPU profile ended")
						}
					case buff[0] == 'b':
						rate, _ := strconv.Atoi(string(buff[2 : n-2]))
						if rate == 0 {
							file, err = os.Create("block.pprof")
							if err == nil {
								err = pprof.Lookup("block").WriteTo(file, 0)
								if err != nil {
									if logger.GetLogger().V(logger.Debug) {
										logger.GetLogger().Log(logger.Debug, "Error writing profile file:", err.Error())
									}
								} else {
									if logger.GetLogger().V(logger.Debug) {
										logger.GetLogger().Log(logger.Debug, "Wrote to profile file block.pprof\n")
									}
								}
								file.Close()
							}
						}
						runtime.SetBlockProfileRate(rate)
						if logger.GetLogger().V(logger.Debug) {
							logger.GetLogger().Log(logger.Debug, "block profile, rate:", rate)
						}
					case buff[0] == 'g':
						if logger.GetLogger().V(logger.Info) {
							logger.GetLogger().Log(logger.Info, "Calling runtime.GC")
						}
						runtime.GC()
					case buff[0] == 't':
						if logger.GetLogger().V(logger.Info) {
							logger.GetLogger().Log(logger.Info, "changing two task")
						}
						os.Setenv("TWO_TASK", string(buff[2:n-2]))
					}
				}
			}
		}()
	}
}
