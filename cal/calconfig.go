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
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/paypal/hera/config"
)

type calConfig struct {
	enabled bool
	//
	// calmessage labels
	//
	poolName    string
	environment string
	label       string
	//
	// suffix after release build number
	//
	labelAffix      string
	releaseBuildNum string
	//
	// address to caldaemon
	//
	host string
	port string
	//
	// calclient connection timeout to caldaemon. connection is INPROGRESS before timeout
	//
	connTimeSec int
	//
	// we can keep pushing calmessages to calsockethander even before it has a connection
	// with caldaemon. these early-arrived calmessages will be kept on a ringbuffer.
	// if ringbuffer overflows, it stops to take additional calmessages.
	//
	ringBufferSize int
	//
	// ring buffer hold all the calmessages inside a pending root transaction.
	// at the start of root transaction, a pending flag is set to true to
	// hold each nested calmessages in this buffer instead of flushing it to socket.
	// at the end of root transaction, pending flag is set to false to allow
	// all calmessages in the buffer to be flushed to socket to caldaemon
	//
	msgBufferSize int
	//
	// socket or file (@TODO implement localfile)
	//
	handlerType string
	//
	// if handler type is file, this gives the name of localfile
	//
	logFileName string
	//
	// if ture, calmsgs with different threadids will be put in different swimminglanes.
	// otherwise, all calmsgs from a same process will be put in the same swimminglane
	// even these calmsgs carry different threadids.
	//
	enableTG bool
	//
	//
	//
	poolstackEnabled bool
	poolStackSize    int
}

const (
	loopbackIP = "127.0.0.1"
	unknown    = "unknown"
)

/**
 * @TODO get values from cal_client.txt.
 */
func (c *calConfig) initialize(cfg config.Config, vcfg config.Config, _labelAffix string) error {
	c.labelAffix = _labelAffix // still needs it?
	//
	// failure to initialize config results in calclient initialization failure
	// which leaves the global calclient instance null. without a valid calclient
	// instance, any calmessage operations become no-op
	//
	c.enabled = cfg.GetOrDefaultBool("enable_cal", true)
	c.poolName = cfg.GetOrDefaultString("cal_pool_name", "play_abc")
	c.environment = cfg.GetOrDefaultString("cal_environment", "")
	c.host = cfg.GetOrDefaultString("cal_socket_machine_name", loopbackIP)
	c.port = cfg.GetOrDefaultString("cal_socket_machine_port", "1118")
	c.connTimeSec = cfg.GetOrDefaultInt("cal_socket_connect_time_secs", 1)
	c.ringBufferSize = cfg.GetOrDefaultInt("cal_socket_ring_buffer_size", 32000)
	c.msgBufferSize = cfg.GetOrDefaultInt("cal_message_buffer_size", 300)
	c.handlerType = cfg.GetOrDefaultString("cal_handler", "socket")
	c.logFileName = cfg.GetOrDefaultString("cal_log_file", "logCalClient.txt")
	c.enableTG = (cfg.GetOrDefaultString("cal_enable_threadgroup", "false") == "true")
	c.poolstackEnabled = (cfg.GetOrDefaultString("cal_pool_stack_enable", "true") == "true")
	c.poolStackSize = cfg.GetOrDefaultInt("cal_max_pool_stack_size", 2048)
	if c.poolStackSize > 2048 {
		c.poolStackSize = 2048
	}

	var relProdNumber = unknown
	if vcfg != nil {
		relProdNumber = vcfg.GetOrDefaultString("release_product_number", unknown)
		c.releaseBuildNum = vcfg.GetOrDefaultString("release_build_number", unknown)
	}

	//fmt.Printf("%s %s %s %s %d %d %d %s %s %s %s\n", cfg.poolName, cfg.environment,
	//	cfg.host, cfg.port, cfg.connTimeSec, cfg.ringBufferSize, cfg.msgBufferSize,
	//	cfg.handlerType, cfg.logFileName, relProdNumber, cfg.releaseBuildNum)

	c.createLabel(relProdNumber, c.releaseBuildNum)
	return nil
}

func (c *calConfig) createLabel(_version string, _build string) {
	var buf bytes.Buffer

	localhost, err := os.Hostname()
	if err != nil {
		localhost = loopbackIP
	}
	buf.WriteString(fmt.Sprintf("SQLLog for %s:%s\r\n", c.poolName, localhost))
	buf.WriteString(fmt.Sprintf("Environment: %s\r\n", c.environment))
	buf.WriteString(fmt.Sprintf("Label: %s-%s-%s%s\r\n", c.poolName, _version, _build, c.labelAffix))
	buf.WriteString(fmt.Sprintf("Start: %s\r\n", time.Now().Format("2006-01-02 15:04:05")))
	buf.WriteString("\r\n")

	c.label = buf.String()
}

func (c *calConfig) getPoolName() string {
	return c.poolName
}

func (c *calConfig) getCalDaemonHost() string {
	return c.host
}

func (c *calConfig) getCalDaemonPort() string {
	return c.port
}

func (c *calConfig) getRingBufferSize() int {
	return c.ringBufferSize
}

func (c *calConfig) getMsgBufferSize() int {
	return c.msgBufferSize
}

func (c *calConfig) getLabel() string {
	return c.label
}

func (c *calConfig) getHandlerType() string {
	return c.handlerType
}

func (c *calConfig) getLogFileName() string {
	return c.logFileName
}

func (c *calConfig) getPoolstackEnabled() bool {
	return c.poolstackEnabled
}

func (c *calConfig) getReleaseBuildNum() string {
	return c.releaseBuildNum
}
