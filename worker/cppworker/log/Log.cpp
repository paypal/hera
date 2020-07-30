// Copyright 2020 PayPal Inc.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include <time.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#ifdef SOLARIS_PORT
#include <strings.h>
#endif
#include <string.h>
#include <sstream>

#include "Log.h"
#include "cal/CalMessages.h"
#include "utility/Timer.h"



Log * Log::instance = NULL;

static const char * level_text[] = {
	"alert",
	"warning",
	"info",
	"debug",
	"verbose",
};

const std::string g_log_level_names[] = {
	std::string("off"),
	std::string("alert"),
	std::string("warning"),
	std::string("info"),
	std::string("debug"),
	std::string("verbose")
};

Log::Log(std::ostream * out,LogLevelEnum l)
{
	//create a reasonable default for the format
	set_format("%u %t: %s\n");
	output = out;
	level = l;
	if (!instance)
		instance = this;
	// default all CAL logging to "off"
	mEnable_cal = false;
}

Log::~Log()
{
	delete output;
	//nothing to do here...should the output stream be closed?
	if (instance == this)
		instance = NULL;
}

/**
 * @brief Return the file descriptor to the output stream
 *
 * @return File descriptor
 */
int Log::get_fd()
{
	return 0;//output->get_fd();
} // get_fd()

/**
 * @brief Point stderr to write to the log file.
 *
 * Call this method so that all subsequent fprintf(stderr, ...) 
 * will write to the same log file.
 * NOTE: This method will silently fail to the caller. In which case,
 * all the fprintf(stderr, ...) will do whatever they did before.
 */
void Log::dup2_stderr()
{
	int log_file_fd = get_fd();

	if (log_file_fd != -1)
	{
		int stat = ::fcntl(log_file_fd, F_GETFL, 0);

		if (stat == -1)
		{
			write_entry(LOG_ALERT, "dup2_stderr() failed because fcntl() failed.");
			return;
		}

		// We want to turn off non-blocking IO to this stream.
		if (stat & O_NONBLOCK)
		{
			if (::fcntl(log_file_fd, F_SETFL, stat & ~(O_NONBLOCK)) < 0)
			{
				write_entry(LOG_ALERT, "dup2_stderr failed because we failed to turn off NONBLOCKing IO.");
				return;
			}
		}

		if (::dup2(log_file_fd, STDERR_FILENO) != STDERR_FILENO)
		{
			write_entry(LOG_ALERT, "dup2_stderr failed because dup2() failed.");
		}

#ifdef SOLARIS_PORT
		(void) ::fdopen(STDERR_FILENO, "ab");	// set up a stream from the new file descriptor.
		// fdopen uses _iob[STDERR_FILENO] ( == *stderr )
#else
		stderr = ::fdopen(STDERR_FILENO, "ab");	// open a stream from the new file descriptor.
#endif
		setbuf(stderr, NULL);	// turn off stream buffering.
	}
} // dup2_stderr()

//write an entry to the log file
void Log::write_entry(LogLevelEnum l, const char * str, ...)
{
	va_list ap;

	va_start(ap,str);
	vwrite_entry(l,str,ap);
	va_end(ap);
}

void Log::vwrite_entry(LogLevelEnum l, const char * str, va_list ap)
{
	unsigned int i;

	if(l <= level)
	{
		// clear the buffer
		buffer.clear();

		//iterate through the objects in the format
		for (i = 0; i < format.size(); i++)
		{
			switch (format[i].type)
			{
			case CustomText:
				buffer += format[i].fmt;
				break;
			case LogEntry:
				StringUtil::vappend_formatted(buffer, str, ap);
				break;
			case UnixTime:
				{
					std::ostringstream os;
					os << time(NULL);
					buffer.append(os.str());
					break;
				}
			case UnixTimeMicro:
				{
					struct timeval tv;
					gettimeofday(&tv, NULL);
					char tmbuff[256];
					sprintf(tmbuff, "%ld.%06ld", tv.tv_sec, tv.tv_usec);
					buffer.append(tmbuff);
				}
				break;
			case HumanTime:
				{
					char buf[32];
					time_t t = time(NULL);
					strftime(buf, sizeof(buf), "%m/%d/%Y %H:%M:%S", localtime(&t));
					buffer += buf;
				}
				break;
			case HumanTimeMicro:
				{
					char buf[32];
					struct timeval tv;
					gettimeofday(&tv, NULL);
					strftime(buf, sizeof(buf), "%m/%d/%Y %H:%M:%S", localtime(&tv.tv_sec));
					buffer.append(buf);
					sprintf(buf, ".%06ld", tv.tv_usec);
					buffer.append(buf);
				}
				break;
			case Level:
				{
					std::ostringstream os;
					os << l;
					buffer.append(os.str());
					break;
				}
			case LevelText:
				buffer += level_text[l];
				break;
			} //end switch
		} //end for

		if (!buffer.empty())
		{
			// Log CAL message but be careful not to fall into recursive loop
			if (mEnable_cal && !strstr(buffer.c_str(), " [cal"))
			{
				CalTransaction d("URL"); d.SetName("LOG"); d.SetStatus(CAL::TRANS_OK);
				CalEvent c("Msg", "Log", "0", buffer.c_str());
			}
			// actually write it
			*output << buffer;
		}
	} //end if special level
}

void Log::set_format(const char * fmt)
{
	unsigned int i;
	unsigned int start;
	std::string temp_string;

	if(fmt==NULL) {
		return;
	}

	format.clear();
	//go through and tokenize
	for (i = 0, start = 0; fmt[i]; i++)
	{
		if ((fmt[i] == '%') && (fmt[i + 1] != 0))
		{
			//a token break
			//see if we need to add special stuff
			if (i > start)
			{
				format.push_back(LogFormat(CustomText, std::string(fmt + start, i - start)));
			}
			//add the special item
			i++;
			switch(fmt[i]) {
			case 's':
				format.push_back(LogFormat(LogEntry));
				start = i+1;
				break;
			case 'u':
				format.push_back(LogFormat(UnixTime));
				start = i+1;
				break;
			case 'U':
				format.push_back(LogFormat(UnixTimeMicro));
				start = i+1;
				break;
			case 'h':
				format.push_back(LogFormat(HumanTime));
				start = i+1;
				break;
			case 'H':
				format.push_back(LogFormat(HumanTimeMicro));
				start = i+1;
				break;
			case 'l':
				format.push_back(LogFormat(Level));
				start = i+1;
				break;
			case 't':
				format.push_back(LogFormat(LevelText));
				start = i+1;
				break;
			default:
				break;
			}
		} //end if special
	} //end for
	//check if there was some left
	if (fmt[start] != 0)
	{
		format.push_back(LogFormat(CustomText, std::string(fmt + start, i - start)));
	}
}

void Log::log_time(LogLevelEnum l, Timer &t, const char *str, ...)
{
	std::ostringstream os;
	va_list ap;

	t.mark();
	os << "<" << t.get_string() << "> " << str;

	va_start(ap,str);
	vwrite_entry(l, os.str().c_str(),ap);
	va_end(ap);	
}

LogFormat::LogFormat(LogFormatType t)
{
	type = t;
}

LogFormat::LogFormat(LogFormatType t, const std::string& special)
	: fmt(special)
{
	type = t;
}
