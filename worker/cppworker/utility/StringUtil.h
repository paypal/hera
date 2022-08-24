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
#ifndef PP_STRINGUTIL_H
#define PP_STRINGUTIL_H

#include <string>
#include <stdarg.h>

class StringUtil {
    public:
        static int skip_newline(const std::string& str, int offset);
        static bool ends_with(const std::string& str, const std::string& end);
        static std::string& fmt_int(std::string& str, const int& val);
        static std::string& fmt_ulong(std::string& str, const unsigned long& val);
        static std::string& fmt_ullong(std::string& str, const unsigned long long& val);
        static unsigned int fmt_uint(char * s,unsigned int u);
        static int to_int(const std::string& str);
        static unsigned int to_uint(const std::string& str);
        static long long to_llong(const std::string& str);
        static unsigned long long to_ullong(const std::string& str);
        static void to_lower_case(std::string& str);
        static void to_upper_case(std::string& str);
        static void trim(std::string& str);
        static void vappend_formatted(std::string& str, const char *format, va_list ap);
        static bool tokenize(std::string& str, std::string& token, char ch);
        // replace '\r\n' -> ' ', '\n' -> ' ', remove spaces following a space
        static void normalizeSQL(std::string& str);
        static std::string& hex_escape(const std::string& str);
        static int compare_ignore_case(const std::string& stra, const std::string& strb);
        static size_t index_of_ignore_case(const std::string& stra, const std::string& strb);
        static bool starts_with_ignore_case(const std::string& stra, const char* strb);
        static void replace_str(std::string& str, const std::string& old, const std::string& new_val);
};

#endif
