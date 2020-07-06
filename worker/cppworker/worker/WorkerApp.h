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
