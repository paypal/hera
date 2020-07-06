#include <arpa/inet.h>
#include <sstream>
#include <string.h>

#include "worker/EORMessage.h"
#include "log/LogWriter.h"

EORMessage::EORMessage() : status(FREE),req_id(0)
{
}

EORMessage::EORMessage(Status _status, const uint32_t _reqid, const std::string& _payload) : status(_status), req_id(_reqid), payload(_payload)
{
}

void EORMessage::compose(std::string& _buffer)
{
	int len = 5 + payload.length(); // 3 ( 1 byte for Status, 4 bytes for req_id)
	_buffer.resize(len);
	char * buf = (char*)_buffer.c_str();
	buf[0] = '0' + status;
	buf[1] = (req_id >> 24) & 0xFF;
	buf[2] = (req_id >> 16) & 0xFF;
	buf[3] = (req_id >> 8) & 0xFF;
	buf[4] = req_id & 0xFF;
	memcpy(buf + 5, payload.c_str(), payload.length());
}

bool EORMessage::parse(const std::string& _buffer)
{
	if (_buffer.length() < 1)
	{
		return false;
	}
	status = (Status)(_buffer[0] - '0');
	req_id = (_buffer[1] << 24) + (_buffer[1] << 16) + (_buffer[1] << 8) + _buffer[4];
	payload.assign(_buffer.c_str() + 5, _buffer.length() - 1);
	return true;
}

void EORMessage::dump(LogWriterBase& out)
{
	WRITE_LOG_ENTRY(&out, LOG_VERBOSE,
		"EORMessage: status = %d, payload = (%s)",
		status, payload.c_str());
}

void EORMessage::dump(std::string& out)
{
	std::stringstream ss;
	ss << "EORMessage: status=" << status << ", payload=("
		<<  payload.c_str() << ")";

	std::string ss_str = ss.str();
	out.append(ss_str.c_str(), ss_str.length());
}
