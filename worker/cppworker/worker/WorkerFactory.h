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
