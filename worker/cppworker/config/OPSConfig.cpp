#include "OPSConfig.h"
#include <sstream>

OPSConfig *OPSConfig::m_instance = 0;

OPSConfig::OPSConfig(const std::string& filename): m_cfg(filename)
{
	const char* start = filename.c_str();
	const char* end = start + filename.size() - 4;
	if ((end <= start) || (*end != '.')) 
	{
		std::ostringstream os;
		os << "Invalid file " << filename;
		throw ConfigException(os.str());
	}
	const char* p = end;
	while ((p >= start) && (*p != '/')) p--;
	p++;
	std::ostringstream os;
	os << "opscfg." << std::string(p, end - p) << ".server.";
	m_keyPrefix = os.str();
}

bool OPSConfig::get_value(const std::string& name, std::string& value) const
{
	std::string key = m_keyPrefix + name;
	if (m_cfg.get_value(key, value)) {
		return true;
	}
	return m_cfg.get_value(std::string("opscfg.default.server.") + name, value);
}

bool OPSConfig::load_if_changed()
{
	return true;
}

OPSConfig& OPSConfig::create_instance(const std::string& filename) {
	m_instance = new OPSConfig(filename);
	return *m_instance;
}