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

// +build linux

package utility

/*
#include <stdlib.h>
#include <sys/wait.h>
#include <stdio.h>
#include <signal.h>

void reapDefunctPids(int* _pids, int _size)
{
	if((_pids == 0) || (_size == 0))	{
		return;
	}
	int cnt = 0;
	int pid = 0;
	//
	// reap all defuncted without delay. may get some garbage that wont match the pidmap.
	//
	while((pid = waitpid((pid_t)(-1), 0, WNOHANG)) > 0) {
		*(_pids++) = pid;
		if(++cnt >= _size)	{
			break;
		}
	}
}

void killParam(int pid, int signal, int val) {
	union sigval value;
	value.sival_int = val;
	sigqueue(pid, signal, value);
}
*/
import "C"

import (
	"unsafe"
)

// ReapDefunctPids is a wrapper over wait() sistem call, returning the list of pids in case multiple child procesesses exited
func ReapDefunctPids(pids []int32) {
	arraySize := len(pids)
	if arraySize > 0 {
		C.reapDefunctPids((*C.int)(unsafe.Pointer(&pids[0])), C.int(arraySize))
	}
}

// KillParam is a wrapper over sigqueue system call, to send a signal with a parameter
func KillParam(pid int, sig int, value int) {
	C.killParam(C.int(pid), C.int(sig), C.int(value))
}
