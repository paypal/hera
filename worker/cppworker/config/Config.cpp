#include <boost/lexical_cast.hpp>
#include <string.h>

#include "Config.h"

// ---------------------------------------------------------------------

ConfigException::ConfigException(const std::string &_msg) :	PPException(_msg)
{
}

std::string ConfigException::get_name() const
{
	return "ConfigException";
}

// ---------------------------------------------------------------------

Config::Config()
{
}

Config::~Config()
{
}

bool Config::parse_switch_enabled_status(const std::string& value, bool def_val)
{
	static const char * SWITCH_ONE      = "1";
	static const char * SWITCH_TRUE     = "true";
	static const char * SWITCH_ON       = "on";
	static const char * SWITCH_ENABLED  = "enabled";
	static const char * SWITCH_ENABLE   = "enable";
	static const char * SWITCH_YES      = "yes";

	static const char * SWITCH_ZERO     = "0";
	static const char * SWITCH_FALSE    = "false";
	static const char * SWITCH_OFF      = "off";
	static const char * SWITCH_DISABLED = "disabled";
	static const char * SWITCH_DISABLE  = "disable";
	static const char * SWITCH_NO       = "no";

	// put these first because they are the preferred settings
	if (strcmp(value.c_str(), SWITCH_ONE) == 0)
		return true;
	if (strcmp(value.c_str(), SWITCH_ZERO) == 0)
		return false;

	// is it on?
	if (!strcasecmp(value.c_str(), SWITCH_TRUE) ||
	  !strcasecmp(value.c_str(), SWITCH_ON) ||
	  !strcasecmp(value.c_str(), SWITCH_ENABLED) ||
	  !strcasecmp(value.c_str(), SWITCH_ENABLE) ||
	  !strcasecmp(value.c_str(), SWITCH_YES))
	{
		return true;
	}

	// is it off?
	if (!strcasecmp(value.c_str(), SWITCH_FALSE) ||
	  !strcasecmp(value.c_str(), SWITCH_OFF) ||
	  !strcasecmp(value.c_str(), SWITCH_DISABLED) ||
	  !strcasecmp(value.c_str(), SWITCH_DISABLE) ||
	  !strcasecmp(value.c_str(), SWITCH_NO))
	{
		return false;
	}

	// hmm... what to do?
	return def_val;
}

bool Config::is_switch_enabled(const std::string& name, bool def_val) const
{
	std::string ret_val;

	// return default value if not present
	if (!get_value(name, ret_val))
		return def_val;

	return parse_switch_enabled_status( &ret_val[0], def_val );
}

//------------------------------
//getters
//------------------------------

template <typename T>
T Config::get_val(const std::string& _key) const
{
	T ret;
	std::string value;
	if (get_value(_key, value))
	{
		try
		{
			ret = boost::lexical_cast<T, const char *>(value.c_str());
		}
		catch (boost::bad_lexical_cast& e)
		{
			std::ostringstream msg;
			msg << __PRETTY_FUNCTION__ << " cannot parse value '" << value << "' from key'" << _key << "'";
			throw ConfigException(msg.str());
		}
	}
	else
	{
		std::ostringstream msg;
		msg << "Config name " << _key << " not found";
		throw ConfigException(msg.str());
	}

	return ret;
}

template <typename T>
T Config::get_val(const std::string& _key, T& _default) const
{
	T ret;
	std::string value;
	if (get_value(_key, value))
	{
		try
		{
			ret = boost::lexical_cast<T, const char *>(value.c_str());
		}
		catch (boost::bad_lexical_cast& e)
		{
			std::ostringstream msg;
			msg << __PRETTY_FUNCTION__ << " cannot parse value '" << value << "' from key'" << _key << "'";
			throw ConfigException(msg.str());
		}
	} 
	else 
	{
		ret = _default;
	}

	return ret;
}

short Config::get_short(const std::string& _key) const
{
	return get_val<short>(_key);
}

short Config::get_short(const std::string& _key, short _default) const
{
	return get_val(_key, _default);
}

int Config::get_int(const std::string& _key) const
{
	return get_val<int>(_key);
}

int Config::get_int(const std::string& _key, int _default) const
{
	return get_val(_key, _default);
}

long Config::get_long(const std::string& _key) const
{
	return get_val<long>(_key);
}

long Config::get_long(const std::string& _key, long _default) const
{
	return get_val(_key, _default);
}

long long Config::get_llong(const std::string& _key) const
{
	return get_val<long long>(_key);
}

long long Config::get_llong(const std::string& _key, long long _default) const
{
	return get_val(_key, _default);
}

ulong Config::get_ulong(const std::string& _key) const
{  
	return get_val<ulong>(_key);
}

ulong Config::get_ulong(const std::string& _key, ulong _default) const
{
	return get_val(_key, _default);
}

unsigned long long  Config::get_ullong(const std::string& _key) const
{
	return get_val<unsigned long long>(_key);
}

unsigned long long  Config::get_ullong(const std::string& _key, unsigned long long _default) const
{
	return get_val(_key, _default);
}

bool Config::get_bool(const std::string& _key) const
{
	std::string value;
	bool ret = get_value(_key, value);
	if(!ret)
	{
		std::ostringstream msg;
		msg << "get_bool: Config name " << _key << " not found";
		throw ConfigException(msg.str());
	}
	if(!strcasecmp(value.c_str(), "1") ||
	   !strcasecmp(value.c_str(), "on") ||
	   !strcasecmp(value.c_str(), "enable") ||
	   !strcasecmp(value.c_str(), "true") ||
	   !strcasecmp(value.c_str(), "yes") ||
	   !strcasecmp(value.c_str(), "enabled")){
		return true;
	}else{
		return false;
	}
}

bool Config::get_bool(const std::string& _key, bool _default) const
{
	bool ret = _default;
	try{
		ret = get_bool(_key);
	}catch(PPException& e)
	{
		ret =  _default;
	}

	return ret;
}

std::string Config::get_string(const std::string& _key) const
{
	std::string value;
	bool ret = get_value(_key, value);
	if(ret){
		return value;
	}else{
		std::ostringstream msg;
		msg << "get_bool: Config name " << _key << " not found";
		throw ConfigException(msg.str());
	}
}

std::string Config::get_string(const std::string& _key, const std::string& _default) const
{
	std::string value;
	bool ret = get_value(_key, value);
	if(ret){
		return value;
	}else{
		return _default;
	}
}
