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
#ifndef _WORKERAPP_H_
#define _WORKERAPP_H_

#include <memory>

class Worker;
class InitParams;
class Config;
class LogWriterBase;
class WorkerFactory;

extern const char *g_log_path;
extern const char *g_cfg_path;

//-----------------------------------------------------------------------------

class WorkerApp {
public:
	static int execute(const WorkerFactory&);

private:
	enum {
		DEFAULT_FAILURE_DELAY = 20, // the sleep time for the child before exiting in case of failure.
	};

	WorkerApp(const WorkerFactory&);
	~WorkerApp();

	static void AtExit();

	int main();
	void initialize(const WorkerFactory&);
	void init_logs();

private:
	void* pin;
	std::unique_ptr<Config> config;
	std::unique_ptr<Worker> worker;
	LogWriterBase* logger;
	unsigned int failure_delay;

	static WorkerApp* instance;
};

#endif
