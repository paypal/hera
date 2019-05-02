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
	"errors"
	"fmt"
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
	"io/ioutil"
	"os"
	"runtime"
	"time"
)

// getInt extract an int from a string of number separated by spaces, pos says which field to extract
func getInt(data []byte, pos int) (int64, []byte, error) {
	ln := len(data)
	i := 0
	cnt := 1
	var ret int64
	found := false
	for i < ln {
		if cnt == pos {
			if data[i] >= '0' && data[i] <= '9' {
				ret = ret*10 + int64(data[i]-'0')
				found = true
			} else {
				break
			}
		} else {
			if data[i] == ' ' {
				cnt++
			}
		}
		i++
	}
	if found {
		if i < ln {
			i++
		}
		return ret, data[i:], nil
	}
	return -1, data, errors.New("Field not found")
}

// GoStats runs in a goroutine and dumps every second stats from /proc<pid>/stat
func GoStats() {
	interval := GetConfig().GoStatsInterval
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "GoStats every", interval)
	}

	if interval > 0 {
		procfile := fmt.Sprintf("/proc/%d/stat", os.Getpid())
		go func() {
			for {
				time.Sleep(time.Second * time.Duration(interval))
				evt := cal.NewCalEvent("GO", "stat", cal.TransOK, "")
				evt.AddDataInt("goroutines", int64(runtime.NumGoroutine()))

				data, err := ioutil.ReadFile(procfile)
				if err != nil {
					evt.Completed()
					continue
				}
				var val int64

				val, data, err = getInt(data, 14)
				if err != nil {
					evt.Completed()
					continue
				}
				evt.AddDataInt("utime", val/100 /*sysconf(_SC_CLK_TCK)*/)

				val, data, err = getInt(data, 1)
				if err != nil {
					evt.Completed()
					continue
				}
				evt.AddDataInt("stime", val/100)

				// five further fields is num threads
				val, data, err = getInt(data, 5)
				if err != nil {
					evt.Completed()
					continue
				}
				evt.AddDataInt("threads", val)

				// three further fields is VSS
				val, data, err = getInt(data, 3)
				if err != nil {
					evt.Completed()
					continue
				}
				evt.AddDataInt("vss", val)

				// one further field is rSS
				val, data, err = getInt(data, 1)
				if err != nil {
					evt.Completed()
					continue
				}
				evt.AddDataInt("rss", val*4 /*4k pages*/)
				evt.Completed()
			}
		}()
	}
}
