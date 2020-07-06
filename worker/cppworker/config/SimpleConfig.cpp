
#include "SimpleConfig.h"
#include <fstream>
#include <regex>

SimpleConfig::SimpleConfig(const std::string& filename)
{
	std::ifstream in(filename);
	if (!in.good()) {
		throw ConfigException("Can't read file");
	}
	std::string buf;
	std::regex e("(.*)=(.*)");
	while (std::getline(in, buf)) {
		std::smatch match;
		if (std::regex_search(buf, match, e) && match.size() > 1) {
    		values[match.str(1)] = match.str(2);
		}
	}
}

SimpleConfig::~SimpleConfig()
{
}

bool SimpleConfig::get_value(const std::string& name, std::string& value) const
{
	std::unordered_map<std::string,std::string>::const_iterator it = values.find(name);
	if (it != values.end()) {
		value = it->second;
		return true;
	}
	return false;
}
