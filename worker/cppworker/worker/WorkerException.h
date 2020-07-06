#ifndef WORKER_EXCEPTION_H
#define WORKER_EXCEPTION_H

#include "utility/PPException.h"

class WorkerCreationException : public PPException {
	public:
		WorkerCreationException(const std::string &_message) PPEX_NOTHROW : PPException(_message) {};
		virtual ~WorkerCreationException() PPEX_NOTHROW {};
		virtual std::string get_name(void) const { return std::string("WorkerCreationException"); };
};

#endif
