#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <string.h>

#include <string>
#include <sstream>

#include "CDBConfig.h"
#include <config/CDBRead.h>

CDBConfig::CDBConfig(const std::string& name)
  : m_mtime(0)
{
	cdb_read = NULL;
	if (!set_filename(name.c_str())) {
		std::ostringstream os;
		os << "Can't read file " << name;
		throw ConfigException(os.str());
	}
}

CDBConfig::~CDBConfig()
{
	delete cdb_read;
}

bool CDBConfig::set_filename(const char * filename)
{
	// clean up
	delete cdb_read;
	cdb_read = NULL;

	// please not null
	if (!filename)
		return false;

	// initialize last modification time
	struct stat64 stat_buf;
	int stat_ret = stat64(filename, &stat_buf);
	if (stat_ret != 0 )
		return false;
        //check file size
        if(stat_buf.st_size == 0)
                return false;

	m_mtime = stat_buf.st_mtime;

	// open the file
	std::ifstream in(filename);
	if (!in.good())
		return false;

	// cdb_read reads in the whole thing at once, so
	// we don't need to keep "in"
	m_filename = filename;
	cdb_read = new CDBRead(in);

	return true;
}

bool CDBConfig::check_if_changed()
{
	struct stat64 stat_buf;
	int stat_ret = stat64(m_filename.c_str(), &stat_buf);

	if (stat_ret != 0)
	{
		std::ostringstream os;
		os << "unable to stat " << m_filename << ", errno=" << errno << ", errstr=" << strerror(errno);
		throw ConfigException(os.str());
	}

	// For safety, wait 5 seconds past the modification time before
	// re-reading the file.   Note, only doing the time() system call
	// during that 5 second window for improved performance.
	if (m_mtime != stat_buf.st_mtime &&
		time(NULL) >= (stat_buf.st_mtime + 5))
	{
		m_mtime = stat_buf.st_mtime;

		return true;
	}

	return false;
}

bool CDBConfig::load_if_changed()
{
	if (check_if_changed())
	{
		if (cdb_read)
		{
			delete cdb_read;
		}

		// open the file
		std::ifstream in(m_filename);
		if (!in.good())
		{
			std::ostringstream os;
			os << "unable to read " << m_filename << ", errno=" << errno << ", errnostr=" << strerror(errno);
			throw ConfigException(os.str());
		}

		cdb_read = new CDBRead(in);

		return true;
	}

	return false;
}

bool CDBConfig::get_value(const std::string& name, std::string& value) const
{
	// make sure we have a file
	if (!cdb_read)
	{
		value.erase();
		return false;
	}

	return cdb_read->get(name, value);
}

bool CDBConfig::get_all_values (std::unordered_map<std::string,std::string>& _values_out) const 
{
	if (!cdb_read)
	{
		return false;
	}
	return cdb_read->get_all_values(_values_out);
}
