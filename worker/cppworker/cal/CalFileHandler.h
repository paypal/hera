#ifndef __CALFILEHANDLER_H
#define __CALFILEHANDLER_H

#include "CalHandler.h"
#include <string>

class CalConfig;
class CalLog;
class CalFileHandler : public CalHandler
{
 public:
	CalFileHandler (CalConfig* _config, CalLog* _logger);
	~CalFileHandler () {}
	void write_data (const std::string& data);
};

#endif
