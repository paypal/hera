#ifndef _MARKDOWN_H_
#define _MARKDOWN_H_

#include <log/LogWriter.h>
#include <string>
#include <vector>

enum  MarkdownEnum {
	MARKDOWN_NONE = 0,
	MARKDOWN_HOST = 1,
	MARKDOWN_TABLE = 2,
	MARKDOWN_SQL = 3,
	MARKDOWN_URL = 4,
	MARKDOWN_TRANS = 5,
	MARKDOWN_COMMIT = 6
};

struct MarkdownFilter
{
    MarkdownFilter()
    : freq(100) {}

    bool match(const std::string &src,
               const std::string &raw,
               const std::string &host_name,
	       const std::string &host_name2,
               bool isTable,
               LogWriterBase *log = NULL);

    // keyword list
    std::vector<std::string> keyword;
    // db host
    std::string host;
    // markdown frequency.
    int freq;

    // Last sql statement 
    std::string old_src;
};

class MarkdownList
{
public:
    MarkdownList() 
      : m_path(NULL)
    {
      m_last_mod_time[0] = 
        m_last_mod_time[1] = 0;
    }

    void load_control_files(const char *path,
                            LogWriterBase *log = NULL);

    bool isEmpty() const    {  return (m_filter[0].empty() && 
				       m_filter[2].empty());  }

    bool doMarkdown(std::string host,
		    std::string host2,
                    const std::string &raw,   // sql statement
                    LogWriterBase *log = NULL);


private:
    // filter[0]: for table markdown.
    // filter[1]: for sql markdown.
    std::vector<MarkdownFilter> m_filter[2];

    // last modification time of rule files.
    time_t m_last_mod_time[2];

    // markdown directory path.
    const char *m_path;
};

#endif
