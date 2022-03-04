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

// Package logger implements the application logger/tracer. It is mainly a wrapper over the standard logger, adding a special prefix to each line
package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	// for LOG_ALERT
	"github.com/paypal/hera/cal"
)

// Logger is the interface for logging
type Logger interface {
	// Log writes a message with the given severity to the log. If the severity is lower then the Logger severity, the message is ignored
	Log(severity int32, a ...interface{})
	// similar to glog's interface, V reports whether verbosity at the call site is at least the requested level
	V(severity int32) bool
}

type logger struct {
	fileLogger *log.Logger
	severity   int32
	procName   string
}

// logger severity
const (
	Alert   = 0
	Warning = 1
	Info    = 2
	Debug   = 3
	Verbose = 4
)

var (
	sInstance *logger
	prefixStr = [...]string{"alert:", "warn:", "info:", "debug:", "verbose:"}
)

// GetLogger returns the logger instance
func GetLogger() Logger {
	return sInstance
}

func init() {
	createLogger(os.Stdout, "PROXY", Info)
}

/* hera.log pipe stalls on multiple spawn/open with start.sh */
func openFileTimeout(name string, flag int, perm os.FileMode) (*os.File, error) {
	var file *os.File
	var err error
	ch := make(chan bool, 1)
	go func() {
		file, err = os.OpenFile(name, flag, perm)
		ch <- true
	}()
	select {
	case <-ch:
		return file, err
	case <-time.After(1 * time.Second):
		return nil, fmt.Errorf("OpenFileTimeout took too long")
	}
}

// CreateLogger creates a logger which writes to the given fileName. procName is used to prefix the
// messages, usefull when mutiple proceses share the same log file.
func CreateLogger(fileName string, procName string, severity int32) error {
	var file *os.File
	var err error
	file, err = openFileTimeout(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Println("Failed to open log file", err.Error())
		return fmt.Errorf("Failed! open log file")
	}
	// redirect stdout and stderr to this file
	dup(int(file.Fd()))
	createLogger(file, procName, severity)
	return nil
}

func createLogger(file io.Writer, procName string, severity int32) {
	sInstance = &logger{fileLogger: log.New(file, "" /* no general prefix. a severity prefix is attached at the time of each log*/, log.Ltime|log.Lmicroseconds),
		severity: severity, procName: procName}
}

func (logger *logger) Log(severity int32, a ...interface{}) {
	if severity <= logger.getSeverity() {
		_, file, line, ok := runtime.Caller(1)
		if !ok {
			file = "???"
			line = 1
		} else {
			slash := strings.LastIndex(file, "/")
			if slash >= 0 {
				file = file[slash+1:]
			}
		}
		a = append([]interface{}{fmt.Sprintf("%s [%s %s:%d]", prefixStr[severity], logger.procName, file, line)}, a...)
		logger.fileLogger.Println(a...)
		if severity == Alert {
			evt := cal.NewCalEvent("LOGGER", "ALERT", cal.TransOK, "")
			aJoined := fmt.Sprintln(a...)
			evt.AddDataStr("Data", aJoined[:len(aJoined)-1])
			evt.Completed()
		}
	}
}

func (logger *logger) V(severity int32) bool {
	return (severity <= logger.getSeverity())
}

func (logger *logger) getSeverity() int32 {
	return atomic.LoadInt32(&sInstance.severity)
}

func (logger *logger) setSeverity(severity int32) {
	atomic.StoreInt32(&sInstance.severity, severity)
}

// SetLogVerbosity sets the log severity (verbosity)
func SetLogVerbosity(severity int32) {
	if severity > Verbose {
		severity = Verbose
	}
	sInstance.Log(Info, "Change log level to", prefixStr[severity])
	sInstance.setSeverity(severity)
}
