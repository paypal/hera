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
	"testing"
)

func TestClient_IsEnabled(t *testing.T) {
	type fields struct {
		mCalConfig        *calConfig
		mCalHandler       handler
		mMsgChann         chan string
		mPendingMsgBuffer map[string]*[]string
		mPending          map[string]bool
		mCurrentCalTxn    map[string]*calTransaction
		mRootCalTxn       map[string]*calTransaction
		mCorrelationID    map[string]string
		mParentStack      map[string]string
		mCurrentOpName    map[string]string
		mAlreadyInit      bool
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "occ-calclient",
			fields: fields{
				mCalConfig: &calConfig{
					enabled: false,
				},
				mCalHandler:       &fileHandler{},
				mMsgChann:         make(chan string),
				mPendingMsgBuffer: make(map[string]*[]string),
				mPending:          make(map[string]bool),
				mCurrentCalTxn:    make(map[string]*calTransaction),
				mRootCalTxn:       make(map[string]*calTransaction),
				mCorrelationID:    make(map[string]string),
				mParentStack:      make(map[string]string),
				mCurrentOpName:    make(map[string]string),
				mAlreadyInit:      true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				mCalConfig:        tt.fields.mCalConfig,
				mCalHandler:       tt.fields.mCalHandler,
				mMsgChann:         tt.fields.mMsgChann,
				mPendingMsgBuffer: tt.fields.mPendingMsgBuffer,
				mPending:          tt.fields.mPending,
				mCurrentCalTxn:    tt.fields.mCurrentCalTxn,
				mRootCalTxn:       tt.fields.mRootCalTxn,
				mCorrelationID:    tt.fields.mCorrelationID,
				mParentStack:      tt.fields.mParentStack,
				mCurrentOpName:    tt.fields.mCurrentOpName,
				mAlreadyInit:      tt.fields.mAlreadyInit,
			}
			if got := c.IsEnabled(); got != tt.want {
				t.Errorf("Client.IsEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_IsInitialized(t *testing.T) {
	type fields struct {
		mCalConfig        *calConfig
		mCalHandler       handler
		mMsgChann         chan string
		mPendingMsgBuffer map[string]*[]string
		mPending          map[string]bool
		mCurrentCalTxn    map[string]*calTransaction
		mRootCalTxn       map[string]*calTransaction
		mCorrelationID    map[string]string
		mParentStack      map[string]string
		mCurrentOpName    map[string]string
		mAlreadyInit      bool
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "occ-calclient",
			fields: fields{
				mCalConfig: &calConfig{
					enabled: false,
				},
				mCalHandler:       &fileHandler{},
				mMsgChann:         make(chan string),
				mPendingMsgBuffer: make(map[string]*[]string),
				mPending:          make(map[string]bool),
				mCurrentCalTxn:    make(map[string]*calTransaction),
				mRootCalTxn:       make(map[string]*calTransaction),
				mCorrelationID:    make(map[string]string),
				mParentStack:      make(map[string]string),
				mCurrentOpName:    make(map[string]string),
				mAlreadyInit:      true,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				mCalConfig:        tt.fields.mCalConfig,
				mCalHandler:       tt.fields.mCalHandler,
				mMsgChann:         tt.fields.mMsgChann,
				mPendingMsgBuffer: tt.fields.mPendingMsgBuffer,
				mPending:          tt.fields.mPending,
				mCurrentCalTxn:    tt.fields.mCurrentCalTxn,
				mRootCalTxn:       tt.fields.mRootCalTxn,
				mCorrelationID:    tt.fields.mCorrelationID,
				mParentStack:      tt.fields.mParentStack,
				mCurrentOpName:    tt.fields.mCurrentOpName,
				mAlreadyInit:      tt.fields.mAlreadyInit,
			}
			if got := c.IsInitialized(); got != tt.want {
				t.Errorf("Client.IsInitialized() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_IsPoolstackEnabled(t *testing.T) {
	type fields struct {
		mCalConfig        *calConfig
		mCalHandler       handler
		mMsgChann         chan string
		mPendingMsgBuffer map[string]*[]string
		mPending          map[string]bool
		mCurrentCalTxn    map[string]*calTransaction
		mRootCalTxn       map[string]*calTransaction
		mCorrelationID    map[string]string
		mParentStack      map[string]string
		mCurrentOpName    map[string]string
		mAlreadyInit      bool
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{{
		name: "occ-calclient",
		fields: fields{
			mCalConfig: &calConfig{
				poolstackEnabled: true,
			},
			mCalHandler:       &fileHandler{},
			mMsgChann:         make(chan string),
			mPendingMsgBuffer: make(map[string]*[]string),
			mPending:          make(map[string]bool),
			mCurrentCalTxn:    make(map[string]*calTransaction),
			mRootCalTxn:       make(map[string]*calTransaction),
			mCorrelationID:    make(map[string]string),
			mParentStack:      make(map[string]string),
			mCurrentOpName:    make(map[string]string),
			mAlreadyInit:      true,
		},
		want: true,
	},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				mCalConfig:        tt.fields.mCalConfig,
				mCalHandler:       tt.fields.mCalHandler,
				mMsgChann:         tt.fields.mMsgChann,
				mPendingMsgBuffer: tt.fields.mPendingMsgBuffer,
				mPending:          tt.fields.mPending,
				mCurrentCalTxn:    tt.fields.mCurrentCalTxn,
				mRootCalTxn:       tt.fields.mRootCalTxn,
				mCorrelationID:    tt.fields.mCorrelationID,
				mParentStack:      tt.fields.mParentStack,
				mCurrentOpName:    tt.fields.mCurrentOpName,
				mAlreadyInit:      tt.fields.mAlreadyInit,
			}
			if got := c.IsPoolstackEnabled(); got != tt.want {
				t.Errorf("Client.IsPoolstackEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_SetParentStack(t *testing.T) {
	type fields struct {
		mCalConfig        *calConfig
		mCalHandler       handler
		mMsgChann         chan string
		mPendingMsgBuffer map[string]*[]string
		mPending          map[string]bool
		mCurrentCalTxn    map[string]*calTransaction
		mRootCalTxn       map[string]*calTransaction
		mCorrelationID    map[string]string
		mParentStack      map[string]string
		mCurrentOpName    map[string]string
		mAlreadyInit      bool
	}
	type args struct {
		_clientpoolInfo string
		_operationName  string
		_tgname         string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		wantVal string
	}{
		{
			name: "occ-calclient1",
			fields: fields{
				mCalConfig: &calConfig{
					poolstackEnabled: true,
				},
				mCalHandler:       &fileHandler{},
				mMsgChann:         make(chan string),
				mPendingMsgBuffer: make(map[string]*[]string),
				mPending:          make(map[string]bool),
				mCurrentCalTxn:    make(map[string]*calTransaction),
				mRootCalTxn:       make(map[string]*calTransaction),
				mCorrelationID:    make(map[string]string),
				mParentStack:      make(map[string]string),
				mCurrentOpName:    make(map[string]string),
				mAlreadyInit:      true,
			},
			args: args{
				_clientpoolInfo: "dalexampleserv^dalexampleserv2",
				_operationName:  "CLIENT_INFO",
				_tgname:         "occ-sample-poolstack",
			},
			wantErr: false,
			wantVal: "^dalexampleserv2^",
		},
		{
			name: "occ-calclient2",
			fields: fields{
				mCalConfig: &calConfig{
					poolstackEnabled: true,
				},
				mCalHandler:       &fileHandler{},
				mMsgChann:         make(chan string),
				mPendingMsgBuffer: make(map[string]*[]string),
				mPending:          make(map[string]bool),
				mCurrentCalTxn:    make(map[string]*calTransaction),
				mRootCalTxn:       make(map[string]*calTransaction),
				mCorrelationID:    make(map[string]string),
				mParentStack:      make(map[string]string),
				mCurrentOpName:    make(map[string]string),
				mAlreadyInit:      true,
			},
			args: args{
				_clientpoolInfo: "dalexampleserv^dalexampleserv2^dalexampleserv3",
				_operationName:  "CLIENT_INFO",
				_tgname:         "occ-sample-poolstack",
			},
			wantErr: false,
			wantVal: "^dalexampleserv2^dalexampleserv3^",
		},
		{
			name: "occ-calclient3",
			fields: fields{
				mCalConfig: &calConfig{
					poolstackEnabled: true,
				},
				mCalHandler:       &fileHandler{},
				mMsgChann:         make(chan string),
				mPendingMsgBuffer: make(map[string]*[]string),
				mPending:          make(map[string]bool),
				mCurrentCalTxn:    make(map[string]*calTransaction),
				mRootCalTxn:       make(map[string]*calTransaction),
				mCorrelationID:    make(map[string]string),
				mParentStack:      make(map[string]string),
				mCurrentOpName:    make(map[string]string),
				mAlreadyInit:      true,
			},
			args: args{
				_clientpoolInfo: "occ-sample",
				_operationName:  "CLIENT_INFO",
				_tgname:         "occ-sample-poolstack",
			},
			wantErr: true,
			wantVal: "occ-sample^",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				mCalConfig:        tt.fields.mCalConfig,
				mCalHandler:       tt.fields.mCalHandler,
				mMsgChann:         tt.fields.mMsgChann,
				mPendingMsgBuffer: tt.fields.mPendingMsgBuffer,
				mPending:          tt.fields.mPending,
				mCurrentCalTxn:    tt.fields.mCurrentCalTxn,
				mRootCalTxn:       tt.fields.mRootCalTxn,
				mCorrelationID:    tt.fields.mCorrelationID,
				mParentStack:      tt.fields.mParentStack,
				mCurrentOpName:    tt.fields.mCurrentOpName,
				mAlreadyInit:      tt.fields.mAlreadyInit,
			}
			if err := c.SetParentStack(tt.args._clientpoolInfo, tt.args._operationName, tt.args._tgname); (err != nil) != tt.wantErr {
				t.Errorf("Client.SetParentStack() error = %v, wantErr %v", err, tt.wantErr)
			}
			poolStack := c.GetPoolStack(tt.args._tgname)

			if poolStack != tt.wantVal {
				t.Errorf("Client.SetParentStack() = %v, want %v", poolStack, tt.wantVal)
			}
		})
	}
}

func TestClient_getCurrentPoolInfo(t *testing.T) {
	type fields struct {
		mCalConfig        *calConfig
		mCalHandler       handler
		mMsgChann         chan string
		mPendingMsgBuffer map[string]*[]string
		mPending          map[string]bool
		mCurrentCalTxn    map[string]*calTransaction
		mRootCalTxn       map[string]*calTransaction
		mCorrelationID    map[string]string
		mParentStack      map[string]string
		mCurrentOpName    map[string]string
		mAlreadyInit      bool
	}
	type args struct {
		_tgname []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				mCalConfig:        tt.fields.mCalConfig,
				mCalHandler:       tt.fields.mCalHandler,
				mMsgChann:         tt.fields.mMsgChann,
				mPendingMsgBuffer: tt.fields.mPendingMsgBuffer,
				mPending:          tt.fields.mPending,
				mCurrentCalTxn:    tt.fields.mCurrentCalTxn,
				mRootCalTxn:       tt.fields.mRootCalTxn,
				mCorrelationID:    tt.fields.mCorrelationID,
				mParentStack:      tt.fields.mParentStack,
				mCurrentOpName:    tt.fields.mCurrentOpName,
				mAlreadyInit:      tt.fields.mAlreadyInit,
			}
			if got := c.getCurrentPoolInfo(tt.args._tgname...); got != tt.want {
				t.Errorf("Client.getCurrentPoolInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}
