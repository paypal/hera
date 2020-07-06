#include "StringUtil.h"
#include <string.h>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <vector>

int StringUtil::skip_newline(const std::string& str, int offset) {
    const char* start = str.c_str() + offset;
    const char* p = start;
    while (*p) {
        p++;
        if (*p == 0x0A) {
            p++;
            if (*p == 0x0D) {
                p++;
            }
            return p - start;
        }
        if (*p == 0x0D) {
            p++;
            if (*p == 0x0A) {
                p++;
            }
            return p - start;
        }
    }
    return str.size();
}

bool StringUtil::ends_with(const std::string& str, const std::string& end) {
    if (end.size() > str.size()) {
        return false;
    }
    return (0 == strcmp(str.c_str() + str.size() - end.size(), end.c_str()));
}

std::string& StringUtil::fmt_int(std::string& str, const int& val) {
	std::ostringstream os;
    os << val;
    str = os.str();
    return str;
}

std::string& StringUtil::fmt_ulong(std::string& str, const unsigned long& val) {
	std::ostringstream os;
    os << val;
    str = os.str();
    return str;
}

int StringUtil::to_int(const std::string& str) {
    int result = 0;
    const char* p = str.c_str();
    unsigned int c;
    int fSigned = 0;

    if(*p == '-') {
        fSigned=1;
        p++;
    } else if(*p == '+') {
        p++;
    }

	for(; (c = (unsigned int) (unsigned char) (*p - '0')) < 10; p++) {
		result = result * 10 + c;
	}

    if(fSigned) {
	    result = -result;
    }

	return result;
}

unsigned int StringUtil::to_uint(const std::string& str) {
    unsigned int result = 0;
    const char* p = str.c_str();
    unsigned int c;

	for(; (c = (unsigned int) (unsigned char) (*p - '0')) < 10; p++) {
		result = result * 10 + c;
	}

	return result;
}

long long StringUtil::to_llong(const std::string& str) {
    long long result = 0;
    const char* p = str.c_str();
    unsigned int c;
    int fSigned = 0;

    if(*p == '-') {
        fSigned=1;
        p++;
    } else if(*p == '+') {
        p++;
    }

	for(; (c = (unsigned int) (unsigned char) (*p - '0')) < 10; p++) {
		result = result * 10 + c;
	}

    if(fSigned) {
	    result = -result;
    }

	return result;
}

unsigned long long StringUtil::to_ullong(const std::string& str) {
    unsigned long long result = 0;
    const char* p = str.c_str();
    unsigned int c;

	for(; (c = (unsigned int) (unsigned char) (*p - '0')) < 10; p++) {
		result = result * 10 + c;
	}

	return result;
}

void StringUtil::to_lower_case(std::string& str) {
    std::transform(str.begin(), str.end(), str.begin(), 
        [](unsigned char c){ return std::tolower(c);});
}

void StringUtil::to_upper_case(std::string& str) {
    std::transform(str.begin(), str.end(), str.begin(), 
        [](unsigned char c){ return std::toupper(c);});
}

void StringUtil::trim(std::string& str) {
    int start = 0, end = str.length() - 1;
    while ((start < str.length()) && (str[start] == ' ')) start++;
    while ((end >= 0) && (str[end] == ' ')) end--;
    if (start == 0) {
        str.resize(end + 1);
    } else {
        str = str.substr(start, end + 1 - start);
    }
}

void StringUtil::vappend_formatted(std::string& str, const char *format, va_list ap) {
    va_list ap2;
    va_copy(ap2, ap);
    char stack_buff[1024];
    int sz = vsnprintf(stack_buff, sizeof(stack_buff), format, ap2);
    va_end(ap2);
    if ((sz > 0) && (sz < sizeof(stack_buff))) {
        str += stack_buff;
        return;
    }
    if (sz < 0) 
        return;
    sz++;
    std::vector<char> buff(sz);
    va_copy(ap2, ap);
    vsnprintf(&buff[0], sz, format, ap2);
    va_end(ap2);
    str += stack_buff;
}

bool StringUtil::tokenize(std::string& str, std::string& token, char ch) {
    std::size_t pos = str.find_first_of(ch);
    if (pos == std::string::npos)
        return false;
    token = str.substr(0, pos);
    str = str.substr(pos + 1, str.length() - pos);
    return true;
}

unsigned int StringUtil::fmt_uint(char * s,unsigned int u)
{
    // Converts an int to a string
    // returns the length of the string (which is NUL terminated)
    unsigned int len;
    unsigned int q;

    len = 1;
    q = u;
    while (q > 9) {
            ++len;
            q /= 10;
    }
    if (s) {
            s += len;
            *s = 0;                                         //NUL terminate
            do {
                    *--s = '0' + (u % 10);
                    u /= 10;
            } while(u); /* handles u == 0 */
    }
    return len;
}

void StringUtil::normalizeSQL(std::string& str) {
    const char* src = str.c_str();
    char* dst = (char*)src;
    char c;
    bool w = false;
    while (*src) {
        c = *src;
        if (c == '\r') {
            if (*(src + 1) == '\n') {
                src++;
            }
        }
        if (c == '\n') {
            c = ' ';
        }
        if (c == ' ') {
            if (w) {
                src++;
                continue;
            }
            w = true;
        } else {
            w = false;
        }
        *dst = c;
        dst++;
        src++;
    }
    str.resize(dst - str.c_str());
}

std::string& StringUtil::hex_escape(const std::string& str)
{
	// Allocate enough space to allow for every byte in Buffer to be
	// represented in the dump as \xAB (four bytes) plus one for the
	// trailing NUL.
	int dump_buffer_len = str.length() * 4 + 1;
	static std::string dump_buffer;
    static const char lower_hex_nums[] = "0123456789abcdef";


	dump_buffer.resize(0);
	dump_buffer.resize(dump_buffer_len);
	dump_buffer[0] = '\0';

	char *cur_pos = &dump_buffer[0];

	int /*amount_written,*/ dump_buffer_remaining;

	int hi, lo;

	unsigned char uc;

	dump_buffer_remaining = dump_buffer_len;
 
	int len = str.length();
	for (int i = 0; i < len && dump_buffer_remaining > 0; i++)
	{
		if (str[i] == '\\')
		{
			*cur_pos++ = '\\';
			*cur_pos++ = '\\';
			dump_buffer_remaining -= 2;
		}
		else if (str[i] <= 0 /* high ascii or NUL */)
		{
			*cur_pos++ = '\\';
			*cur_pos++ = 'x';
			uc = (unsigned char) str[i];
			hi = (uc >> 4) & 0xf;
			*cur_pos++ = lower_hex_nums[hi];
			lo = uc & 0xf;
			*cur_pos++ = lower_hex_nums[lo];
			dump_buffer_remaining -= 4;
		}
		else
		{
			*cur_pos++ = str[i];
			--dump_buffer_remaining;
		}
	}
	// trailing NUL
	*cur_pos = 0;
	return dump_buffer;
}

int StringUtil::compare_ignore_case(const std::string& stra, const std::string& strb) {
    return strcasecmp(stra.c_str(), strb.c_str());
}

size_t StringUtil::index_of_ignore_case(const std::string& stra, const std::string& strb) {
    const char* p = strcasestr(stra.c_str(), strb.c_str());
    if (p == NULL) {
        return std::string::npos;
    }
    return p - stra.c_str();
}

bool StringUtil::starts_with_ignore_case(const std::string& stra, const char* strb) {
    return strncasecmp(stra.c_str(), strb, strlen(strb)) == 0;
}

void StringUtil::replace_str(std::string& str, const std::string& old, const std::string& new_val) {
    std::string out;
    size_t pos = 0;
    while (true) {
        size_t new_pos = str.find(old, pos);
        out.append(str.substr(pos, new_pos - pos));
        if (new_pos == std::string::npos) {
            break;
        }
        out.append(new_val);
        pos = new_pos + old.length();
    }
    str = out;
}
