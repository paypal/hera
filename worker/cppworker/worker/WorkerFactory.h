// Copyright 2020 PayPal Inc.
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
#ifndef _WORKER_FACTORY_H
#define _WORKER_FACTORY_H

#include <memory>

class Worker;
class InitParams;

class WorkerFactory
{
public:
    virtual ~WorkerFactory() { }
	virtual std::unique_ptr<Worker> create(const InitParams& _params) const = 0;
	virtual const char* get_config_name() const = 0;
	virtual const char* get_server_name() const = 0;
};

#endif
