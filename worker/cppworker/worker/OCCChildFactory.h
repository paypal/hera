#ifndef _OCCCHILDFACTORY_H_
#define _OCCCHILDFACTORY_H_

#include "WorkerFactory.h"
#include <memory>

class InitParams;

class OCCChildFactory: public WorkerFactory
{
public:
	virtual std::unique_ptr<Worker> create(const InitParams& _params) const;
	virtual const char* get_config_name() const;
	virtual const char* get_server_name() const;
};

#endif
