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
	"bufio"
	"os"
	"strings"
)

type txtConfig struct {
	data map[string][]byte
}

// Returns a basic implementation of rawConfig.
// The config file contains lines of <key>=<value> pairs, value must not contain "=" (i.e. escaping not supported).
// Lines starting with # are considered comments. The value has whitespaces trimmed at the beginning and at the end
func newTxtConfig(filename string) (RawConfig, error) {
	f, err := os.Open(filename)
	defer f.Close()
	if err != nil {
		return nil, err
	}
	cfg := &txtConfig{}
	cfg.data = make(map[string][]byte)
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		if !(strings.HasPrefix(line, "#")) && strings.Contains(line, "=") {
			pair := strings.Split(line, "=")
			pair[0] = strings.Trim(pair[0], " \t")
			pair[1] = strings.Trim(pair[1], " \t")
			cfg.data[pair[0]] = []byte(pair[1])
		}
	}
	return cfg, nil
}

// implements rawConfig
func (cfg *txtConfig) GetAllValues() map[string][]byte {
	return cfg.data
}

// implements rawConfig
func (cfg *txtConfig) GetValue(key string) ([]byte, error) {
	a, b := cfg.data[string(key)]
	if b {
		return a, nil
	}
	return nil, ErrNotFound
}
