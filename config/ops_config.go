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

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	opscfgEntryFmt = "opscfg.default.server.%s"
)

// OpsConfig interface is used for config variables that can be changed at run time
// it is not thread safe so the users need to take care of the synchronization
type OpsConfig interface {
	Config
	// reload the configuration
	Load() error
	// checks if the config needs to be reloaded, by checking if the file was changed after the previous config load
	Changed() bool
}

type opsConfig struct {
	cfg         Config
	cfgName     string
	lastModTime time.Time
	err         error
	keyPrefix   string
}

var gConfig *opsConfig

// GetOpsConfig gets the instance
func GetOpsConfig() OpsConfig {
	return gConfig
}

// InitOpsConfig initializes the ops config. the name of the configuration file is determined from the config
func InitOpsConfig() error {
	name, err := configName()
	if err != nil {
		return err
	}
	return InitOpsConfigWithName(name)
}

// InitOpsConfigWithName initializes the ops config from the given file
func InitOpsConfigWithName(name string) error {
	// TODO: make it private
	gConfig = &opsConfig{cfgName: name, err: ErrInvalidConfig}
	file := filepath.Base(name)
	gConfig.keyPrefix = fmt.Sprintf("opscfg.%s.server.", file[:len(file)-4])
	return gConfig.Load()
}

func (cfg *opsConfig) Load() error {
	cfg.cfg, cfg.err = NewTxtConfig(cfg.cfgName)
	return cfg.err
}

func (cfg *opsConfig) Changed() bool {
	if len(cfg.cfgName) == 0 {
		return false
	}
	stat, err := os.Stat(cfg.cfgName)
	if err != nil {
		cfg.err = err
		return false
	}

	if stat.ModTime() != cfg.lastModTime {
		cfg.lastModTime = stat.ModTime()
		return true
	}
	return false
}

func (cfg *opsConfig) buildKey(key string) string {
	return strings.Join([]string{cfg.keyPrefix, key}, "")
}

// implements Config interface
func (cfg *opsConfig) GetInt(key string) (int, error) {
	if cfg.err != nil {
		return 0, cfg.err
	}
	val, err := cfg.cfg.GetInt(cfg.buildKey(key))
	if err == ErrNotFound {
		val, err = cfg.cfg.GetInt(fmt.Sprintf(opscfgEntryFmt, key))
	}
	return val, err
}

// implements Config interface
func (cfg *opsConfig) GetOrDefaultInt(key string, defaultVal int) int {
	if cfg.err != nil {
		return defaultVal
	}
	val, err := cfg.cfg.GetInt(cfg.buildKey(key))
	if err == ErrNotFound {
		val = cfg.cfg.GetOrDefaultInt(fmt.Sprintf(opscfgEntryFmt, key), defaultVal)
	}
	return val
}

// implements Config interface
func (cfg *opsConfig) GetString(key string) (string, error) {
	if cfg.err != nil {
		return "", cfg.err
	}
	val, err := cfg.cfg.GetString(cfg.buildKey(key))
	if err == ErrNotFound {
		val, err = cfg.cfg.GetString(fmt.Sprintf(opscfgEntryFmt, key))
	}
	return val, err
}

// implements Config interface
func (cfg *opsConfig) GetOrDefaultString(key string, def string) string {
	if cfg.err != nil {
		return def
	}
	val, err := cfg.cfg.GetString(cfg.buildKey(key))
	if err == ErrNotFound {
		val = cfg.cfg.GetOrDefaultString(fmt.Sprintf(opscfgEntryFmt, key), def)
	}
	return val
}

// implements Config interface
func (cfg *opsConfig) GetBool(key string) (bool, error) {
	if cfg.err != nil {
		return false, cfg.err
	}
	val, err := cfg.cfg.GetBool(cfg.buildKey(key))
	if err == ErrNotFound {
		val, err = cfg.cfg.GetBool(fmt.Sprintf(opscfgEntryFmt, key))
	}
	return val, err
}

// implements Config interface
func (cfg *opsConfig) GetOrDefaultBool(key string, def bool) bool {
	if cfg.err != nil {
		return def
	}
	val, err := cfg.cfg.GetBool(cfg.buildKey(key))
	if err == ErrNotFound {
		val = cfg.cfg.GetOrDefaultBool(fmt.Sprintf(opscfgEntryFmt, key), def)
	}
	return val
}

// implements Config interface
func (cfg *opsConfig) IsSwitchEnabled(key string) bool {
	if cfg.err != nil {
		return false
	}
	return cfg.cfg.IsSwitchEnabled(cfg.buildKey(key))
}

// implements Config interface
func (cfg *opsConfig) Dump() string {
	if cfg.err != nil {
		return cfg.err.Error()
	}
	return cfg.cfg.Dump()
}

// returns the file name where the ops config is
func getCalPoolName() (string, error) {
	// TODO: use CalClient::get_poolName()
	calCdb, err := NewTxtConfig("cal_client.txt")
	if err != nil {
		return "", err
	}
	poolName, err := calCdb.GetString("cal_pool_name")
	if err != nil {
		return "", err
	}
	return poolName, nil
}

func configName() (string, error) {
	poolName, err := getCalPoolName()
	if err != nil {
		return "", err
	}

	//
	// get app name from cal pool name
	//
	appname := poolName
	idx := strings.Index(strings.ToUpper(appname), "STAGE")
	if idx < 0 {
		idx = strings.Index(strings.ToUpper(appname), ".PG")
	}
	//
	// take out prefix from poolname.
	//
	if idx >= 0 {
		//
		// (comment from c++) Assuming the string following the first "_" is the poolname
		//
		idx := strings.Index(poolName, "_")
		if idx < 0 {
			//return "", errors.New("Cal pool name invalid, must be <stage>_<appname>")
			appname = "default"
		} else {
			appname = poolName[idx+1:]
		}
	}

	// determine the folder
	var dir string
	dir, err = os.Getwd()
	if err != nil {
		return "", err
	}

	// regex expression per C++ implementation
	re := regexp.MustCompile("^/x/web[0-9]*/?[^-/]*(-WACK)?")
	dir = re.FindString(dir)
	if len(dir) == 0 {
		//return "", errors.New("Current dir doesn not match a stage directory")
		return fmt.Sprintf("./%s.txt", appname), nil
	}
	return fmt.Sprintf("%s/opscfg/%s.txt", dir, appname), nil
}
