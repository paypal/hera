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
	"sync/atomic"
)

// LockTimeout is a non-blocking best-effort mutex
type LockTimeout struct {
	frontLock int32 // sync.Mutex
}

// TryLock is attempting to acquire the lock
// Returns
// 1 if lock acquired,
// 0 if lock no available
func (lt *LockTimeout) TryLock() int {
	brv := atomic.CompareAndSwapInt32(&lt.frontLock, 0, 1)
	if brv {
		return 1
	}
	return 0
}

// Unlock releases the lock
func (lt *LockTimeout) Unlock() {
	atomic.StoreInt32(&lt.frontLock, 0)
}
