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

package cal

import (
	"log"
	"os"
)

// fileHandler is for the implementation to write to a file
type fileHandler struct {
	//
	// readonly channel for arriving calmessages
	//
	mMsgReadChann <-chan string
	mConfig       *calConfig
	mFileLogger   *log.Logger
}

func (c *fileHandler) init(_config *calConfig, _msgchann <-chan string) error {
	c.mConfig = _config
	c.mMsgReadChann = _msgchann

	filename := "cal.log"
	if _config != nil {
		filename = _config.getLogFileName()
	}
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	c.mFileLogger = log.New(file, "", 0)

	return nil
}

/**
 * do we still need a chann here or there is an easier way to write to log
 */
func (c *fileHandler) run() {
	for {
		calmsg, ok := <-c.mMsgReadChann
		if !ok {
			//
			// @TODO calclient closed channel, clean up memory and exit
			//
			return
		}

		switch calmsg {
		case handlerCtrlMsgNewRoot:
		default:
			c.mFileLogger.Println(calmsg[:len(calmsg)-6])
		}
	}
}
