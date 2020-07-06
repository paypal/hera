#ifndef OCCPROXY_CONTROLMESSAGE_HELPER_H
#define OCCPROXY_CONTROLMESSAGE_HELPER_H

#include <stdint.h>
#include <iostream>
#include <string>

class LogWriterBase;

class ControlMessage
{
public:
	// ! keep the values less then 255
	enum CtrlCmd{
		DEFAULT = 0,
		RESTART,
		PAUSE,
		RESUME,
		STRANDED_CLIENT_CLOSE,
		STRANDED_SATURATION_RECOVER,
		STRANDED_SWITCH,
		STRANDED_TIMEOUT,
		STRANDED_ERR,
    };

	ControlMessage();

	ControlMessage(uint32_t _req_byte_cnt, uint32_t _resp_byte_cnt,
		CtrlCmd _cmd=DEFAULT, bool _in_transaction = false);

	void compose(std::string& _buffer) const;
	bool parse(std::string& _buffer);
	static size_t get_serialized_size() { return sizeof(ControlMessage);}

	void dump(std::ostream& out) const;

	bool is_counter_cmd() const { return m_command == DEFAULT; }
	bool is_restart_cmd() const { return m_command == RESTART; }
	bool is_pause_cmd() const { return m_command == PAUSE; }
	bool is_resume_cmd() const { return m_command == RESUME; }

public:
	uint32_t m_command; 
	uint32_t m_req_byte_cnt;
	uint32_t m_resp_byte_cnt;
	uint32_t m_in_transaction;
	uint32_t m_magic; // must be MAGIC
};

#endif //OCCPROXY_CONTROLMESSAGE_HELPER_H
