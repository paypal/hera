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

// Package config defines configuration readers
package config

import (
	"errors"
	"strconv"
	"strings"
)

// Errors
var (
	ErrNotFound           = errors.New("Key not found")
	ErrInvalidConfig      = errors.New("Config object is invalid")
	ErrInvalidConfigValue = errors.New("Config entry invalid format")
	ErrInvalidCdb         = errors.New("Error parsing CDB")
	ErrInvalidProtected   = errors.New("Protected Config object is invalid")
)

// Config is a configuration reader
type Config interface {
	// Get the integer value corresponding to the given key, returning an error if not found or if the value is invalid
	GetInt(key string) (int, error)
	// Get the integer value corresponding to the given key, returning defaultVal if the config doesn't have a valid value
	GetOrDefaultInt(key string, defaultVal int) int
	// Get the string value corresponding to the given key, returning an error if not found
	GetString(key string) (string, error)
	// Get the string value corresponding to the given key, returning def if the config doesn't have a value for the key
	GetOrDefaultString(key string, def string) string
	// Get the bool value corresponding to the given key, returning an error if not found or if the value is invalid
	// "true" is one of "1", "on", "true", "yes", "enable", "enabled" (case insensitive), "false" is anything else
	GetBool(key string) (bool, error)
	// Get the integer value corresponding to the given key, returning def if the config doesn't have a valid value
	GetOrDefaultBool(key string, def bool) bool
	// return true if the value for key is one of "1", "on", "true", "yes", "enable", "enabled" (case insensitive)
	IsSwitchEnabled(key string) bool
	// for troubleshooting, dumps all the content
	Dump() string
}

// NewTxtConfig creates a Config for parsing a file where each line has the format <key>=<value>
func NewTxtConfig(filename string) (Config, error) {
	cfg, err := newTxtConfig(filename)
	if err == nil {
		return NewConfig(cfg), nil
	}
	return nil, err
}

// Implements Config interface
// It consists of a set of wrapper functions over a rawConfig interface
type wrapRawConfig struct {
	cfg RawConfig
}

// NewConfig wraps a simple RawConfig with easier to use Config functions
func NewConfig(cfg RawConfig) Config {
	return &wrapRawConfig{cfg: cfg}
}

// RawConfig interface that must be implemented to be used with the "config" struct
type RawConfig interface {
	GetValue(key string) ([]byte, error)
	GetAllValues() map[string][]byte
}

// implements Config interface
func (cfg *wrapRawConfig) GetInt(key string) (int, error) {
	strval, err := cfg.cfg.GetValue(key)
	if err != nil {
		return 0, err
	}
	var val int
	val, err = strconv.Atoi(string(strval))
	if err != nil {
		return 0, ErrInvalidConfigValue
	}
	return val, nil
}

// implements Config interface
func (cfg *wrapRawConfig) GetOrDefaultInt(key string, defaultVal int) int {
	val, err := cfg.GetInt(key)
	if err == nil {
		return val
	}
	return defaultVal
}

// implements Config interface
func (cfg *wrapRawConfig) GetString(key string) (string, error) {
	val, err := cfg.cfg.GetValue(key)
	if err != nil {
		return "", err
	}
	return string(val), nil
}

// implements Config interface
func (cfg *wrapRawConfig) GetOrDefaultString(key string, def string) string {
	val, err := cfg.cfg.GetValue(key)
	if err == nil {
		return string(val)
	}
	return def
}

// implements Config interface
func (cfg *wrapRawConfig) GetBool(key string) (bool, error) {
	val, err := cfg.cfg.GetValue(key)
	if err != nil {
		return false, err
	}
	sval := strings.ToLower(string(val))
	if (strings.Compare(sval, "1") == 0) || (strings.Compare(sval, "on") == 0) || (strings.Compare(sval, "true") == 0) ||
		(strings.Compare(sval, "yes") == 0) || (strings.Compare(sval, "enable") == 0) || (strings.Compare(sval, "enabled") == 0) {
		return true, nil
	}
	return false, nil
}

// implements Config interface
func (cfg *wrapRawConfig) GetOrDefaultBool(key string, def bool) bool {
	val, err := cfg.GetBool(key)
	if err == nil {
		return val
	}
	return def
}

// implements Config interface
func (cfg *wrapRawConfig) IsSwitchEnabled(key string) bool {
	return cfg.GetOrDefaultBool(key, false)
}

// implements Config interface
func (cfg *wrapRawConfig) Dump() string {
	var out string
	values := cfg.cfg.GetAllValues()
	for key, val := range values {
		out += key
		out += ": "
		out += string(val)
		out += "\n"
	}
	return out
}
