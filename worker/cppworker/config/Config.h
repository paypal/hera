#ifndef _CONFIG_H_
#define _CONFIG_H_

#include <string>
#include "utility/PPException.h"

/*
  This is the base abstraction of a simple configuration retrieval system.
  It does not support "setting" values.

  If you want a more elaborate system (with categorization of values) and
  setting the values, build on top of this, and use this as a thin client.
*/

//Exception
class ConfigException : public PPException
{
public:
	ConfigException(const std::string& _msg);
	virtual std::string get_name() const;
};


class Config
{
public:
	// taking this out because it is horrible and causes bugs
	//static Config * instance;

	Config();
	virtual ~Config();

	virtual bool get_value(const std::string& name, std::string & value) const = 0;

	// returns TRUE if enabled, FALSE if disabled, def_val if not present or not parseable
	virtual bool is_switch_enabled(const std::string& name, bool def_val) const;

	// file based config will check to see if the file has been modified
	// since the last check
	virtual bool check_if_changed() {return false;}

	// file based config will load data if the file has been modified
	// since the last check
	virtual bool load_if_changed() {return false;}
	

	//convenience getters, non-virtual function
	short get_short(const std::string& _key) const;
	short get_short(const std::string& _key, short _default) const;
	
	int get_int(const std::string& _key) const;
	int get_int(const std::string& _key, int _default) const;
	
	long get_long(const std::string& _key) const;
	long get_long(const std::string& _key, long _default) const;
	
	long long get_llong(const std::string& _key) const;
	long long get_llong(const std::string& _key, long long _default) const;
	
	ulong get_ulong(const std::string& _key) const;
	ulong get_ulong(const std::string& _key, ulong _default) const;
	
	unsigned long long get_ullong(const std::string& _key) const;
	unsigned long long get_ullong(const std::string& _key, unsigned long long _default) const;
	
	bool get_bool(const std::string& _key) const;
	bool get_bool(const std::string& _key, bool _default) const;
	
	std::string get_string(const std::string& _key) const;
	std::string get_string(const std::string& _key, const std::string& _default) const;
	
	// returns TRUE if switch_value represents 'enabled' in some form (e.g. "1", "yes", "enabled" etc.), 
	// FALSE if disabled, def_val if not present or not parseable
	static bool parse_switch_enabled_status(const std::string&  switch_value, bool def_val);

private:
	// these are PRIVATE. they are defined in the *.cpp file
	// please DO NOT move the definition of these functions to the header
	// it causes a dependency on <boost/lexical_cast.h> for EVERY FILE which
	// includes Config.h, which is A LOT OF FILES, and the boost header files
	// are extremely verbose.
	template<typename T> T get_val(const std::string& _key) const;
	template<typename T> T get_val(const std::string& _key, T& _default) const;
};

#endif
