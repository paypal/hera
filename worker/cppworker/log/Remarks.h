#ifndef REMARK_H
#define REMARK_H

#include <infra/utility/environment/log/LogWriter.h>
#include <unistd.h>


class Remarks
{
public:

	Remarks () : m_indent_level (0) { };
	void log (LogWriterBase &_log, LogLevelEnum _level, const String &_indent)
		{
			for (int i = 0; i < m_remarks.length(); i++)
			{
				String logmsg = m_remarks[i].to_string (_indent);
				_log.write_entry (_level, logmsg);
			}
		}

protected:
	friend class RemarkScope;
	void increment_level () { m_indent_level++; }
	void decrement_level () { m_indent_level--; }
	void append (const String &_remark) { m_remarks.append (RemarkEntry (m_indent_level, _remark)); }
		
private:
	struct RemarkEntry
	{
		RemarkEntry () : 						// For TArray.
			m_time (time(NULL)),
			m_indent_level (0),
			m_remark ()
			{}

		RemarkEntry (int _indent_level, const String &_remark) : 
			m_time (time(NULL)),
			m_indent_level (_indent_level),
			m_remark (_remark)
			{}

		String to_string (const String &_indent)
			{
				String ret;
				ret.fmt_ulong (m_time);
				ret.append ("  ");
				for (int i = 0; i < m_indent_level; i++)
					ret.append (_indent);
				ret.append (m_remark);
				return ret;
			}

		ulong m_time;
		int m_indent_level;
		String m_remark;
	};

	TArray<RemarkEntry> m_remarks;
	int m_indent_level;
};

class RemarkScope 
{
public:
	RemarkScope (Remarks *_current_remark, const String &_function) :
		m_current_remark (_current_remark),
		m_function (_function)
		{
			if (m_current_remark) m_current_remark->increment_level();
			append ("entered");
		}
	~RemarkScope () 
		{
			append ("exiting");
			if (m_current_remark) m_current_remark->decrement_level ();
		}
	void append (const char *_format, ...) 
		{
			if (m_current_remark)
			{
				String remark = m_function;
				remark.append (": ");
				va_list ap; 
				va_start (ap, _format);
				remark.vappend_formatted (_format, ap);
				va_end (ap);
				m_current_remark->append (remark);
			}
		}
private:
	Remarks *m_current_remark;
	String m_function;
};

#endif /*REMARK_H*/
