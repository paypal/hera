#ifndef _SIMPLECONFIG_H_
#define _SIMPLECONFIG_H_

/*
  Fast hash-based config class

  Config format:
  [lwsp]name[lwsp]=[lwsp]value[lwsp][n]

  lwsp = linear white space
  n = newline

  Lines starting with # are comments

  19980517 - ech - began
*/

#include <string>
#include <unordered_map>
#include "Config.h"

//redefine this if you think your config file will be larger
#ifndef SIMPLE_CONFIG_INITIAL_SIZE
#define SIMPLE_CONFIG_INITIAL_SIZE 200
#endif

// SimpleConfigIterator class unused and removed for PPSCR00111530

class SimpleConfig : public Config
{
public:
	SimpleConfig(const std::string& filename);
	~SimpleConfig();

	virtual bool get_value(const std::string& name, std::string& value) const;
	bool get_all_values (std::unordered_map<std::string,std::string>& _values_out) const;

private:
	std::unordered_map<std::string,std::string> values;
};

#endif
