
#include "CalFileHandler.h"
#include "CalLog.h"
#include "CalConfig.h"

////////////////////////////////////
//
// CalFileHandler
//
////////////////////////////////////


CalFileHandler::CalFileHandler (CalConfig* _config, CalLog* _logger)
	:CalHandler(_logger) 
{
}

// true - data sent
// false - error while sending data
void CalFileHandler::write_data(const std::string& _data)
{
	if (m_logger)
	{
		// CAL_LOG_ALERT has side effect
		m_logger->write_cal_message (_data);
	}
}
