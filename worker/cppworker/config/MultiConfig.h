#ifndef MULTI_CONFIG_H
#define MULTI_CONFIG_H

#include "Config.h"
#include <deque>

class MultiConfig : public Config
{
public:
	MultiConfig(bool _delete_on_destroy = true);
	virtual ~MultiConfig();

	// appends config to end of list (i.e. last file searched), returns 0 on success, -1 on failure
	//  MultiConfig will delete the configs passed in if delete_on_destroy is non-zero
	int add_config(Config *cfg);

	// prepends config to the beginning of list (i.e. first file searched), returns 0 on success, -1 on failure.
	int prepend_config(Config *cfg);

	virtual bool get_value(const std::string& name, std::string& value) const;

	// Check to see if the file has been modified since the last check
	virtual bool check_if_changed();
	
	// reloads file based configurations if changed
	virtual bool load_if_changed();
private:
	std::deque<Config *> m_configs;
	bool             m_delete_on_destroy;

	// copies not supported
	MultiConfig(const MultiConfig& rhs);
	MultiConfig& operator=(const MultiConfig& rhs);
};

#endif
