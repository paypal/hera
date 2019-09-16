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
	"time"
)

const(
	minDuration = 0
	maxDuration = 999999
)

/*
* More enhancement is needed
* Need to add more attributes to this struct,
* for now this is implemented to get over of calculating time duration from calActivity.mTimeStamp (string)
*/
type CalTimer struct {
  tBegin time.Time
}

// Reset the time to current time
func (ct *CalTimer) Reset(){
  ct.tBegin = time.Now()
}

// return time elapsed since last Reset call in 10th of micro seconds
func (ct *CalTimer) Duration() float32{
  var timediff time.Duration
  timediff = time.Since(ct.tBegin)
  return float32(timediff.Nanoseconds()) / float32(time.Millisecond * 10)
}
