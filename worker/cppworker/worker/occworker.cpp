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
