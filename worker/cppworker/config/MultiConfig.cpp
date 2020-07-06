#include "MultiConfig.h"

MultiConfig::MultiConfig(bool _delete_on_destroy)
{
	m_delete_on_destroy = _delete_on_destroy;
}

MultiConfig::~MultiConfig()
{
	if (m_delete_on_destroy) {
		for (std::deque<Config *>::iterator it = m_configs.begin(); it != m_configs.end(); it++)
		{
			delete *it;
		}
	}
}

int MultiConfig::add_config(Config *_config)
{
	// not NULL please
	if (!_config)
		return -1;

	// add it to the end of our list
	m_configs.push_back(_config);

	return 0;
}

int MultiConfig::prepend_config(Config *_config)
{
	// not NULL please
	if (!_config)
		return -1;

	// add it to the beginning of our list
	m_configs.push_front(_config);

	return 0;
}

bool MultiConfig::get_value(const std::string& name, std::string& value) const
{
	// try each config in order
	for (std::deque<Config *>::const_iterator it = m_configs.begin(); it != m_configs.end(); it++)
	{
		if ((*it)->get_value(name, value))
			return true;
	}

	// not found
	return false;
}

bool MultiConfig::load_if_changed()
{
	bool loaded = false;
	for (std::deque<Config *>::iterator it = m_configs.begin(); it != m_configs.end(); it++)
	{
		loaded = ((*it)->load_if_changed() || loaded);
	}
	return loaded;
}

bool MultiConfig::check_if_changed()
{
	bool changed = false;
	for (std::deque<Config *>::iterator it = m_configs.begin(); it != m_configs.end(); it++)
	{
		changed = ((*it)->check_if_changed() || changed);
	}
	return changed;
}
