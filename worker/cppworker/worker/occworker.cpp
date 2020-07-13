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
#include <cstdlib>
#include <string.h>
#include <unistd.h>
#include "WorkerApp.h"
#include "OCCChildFactory.h"

#define ENV_CACHE "CACHE"

int main(int argc, const char** argv)
{
	if (true) {
		char *path = strdup(argv[0]);
		char *lastSlash = path;
		for (char *cur = path; *cur != '\0'; cur++) {
			if (*cur == '/') {
				lastSlash = cur;
			}
		}
		*lastSlash = '\0';
		g_log_path = path;
		g_cfg_path = path;
	} else {
		g_cfg_path = ".";
		g_log_path = ".";
	}
	if (getenv("DEBUG_WORKER_WAIT")) {
		volatile bool loop = true;
		while (loop) {
			sleep(1);
		}
	}

	OCCChildFactory factory;
	return WorkerApp::execute(factory);
}
