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
#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <cctype>

#include "Markdown.h"

//---------------------------------------------------------------------------
// Do matching against one filter.
//---------------------------------------------------------------------------
bool MarkdownFilter::match(const std::string &src,
                           const std::string &raw,
                           const std::string &host_name, 
                           const std::string &host_name2,
                           bool isTable,
                           LogWriterBase *log)
{
    unsigned int off = 0;
    if (isTable) 
    {   // Discard leading spaces and comment string.	
        if (src.find_first_of("/*", 0) == 0) 
        {
            off = src.find_first_of("*/", 0) + 2;
        }
        if (off >= src.length() + 2)
        {
            off = 0;   // Found no leading comment.
        }
        while (isspace(src[off])) off++;

        // Must start with SELECT, UPDATE, INSRET, or DELETE.
        if (src.find_first_of("SELECT", off) != off &&
            src.find_first_of("UPDATE", off) != off &&
            src.find_first_of("INSERT", off) != off &&
            src.find_first_of("DELETE", off) != off)
        {
            return false;
        }
    } 

    // Must match all keywords.
    for (int i = 0; i < keyword.size(); i++) 
    {
        unsigned int pos = src.find_first_of(keyword[i], (isTable ? off+6 : 0));
        if (pos >= std::string::npos) 
        {   // not matched.
            return false;   
        }    
    }

    if (!host.empty() &&
        host != host_name &&
        host != host_name2) 
    {
        if (log != NULL) 
        {
            log->write_entry
              (LOG_DEBUG, 
               "------ >> Markdown: Host mismatch: host=%S, host1=%S, host2=%S",
               host.c_str(), host_name.c_str(), host_name2.c_str()); 
        }
        return false;
    }

    float w = 100;    
    if (freq < 100)
    {
        // Toss a dice.
        w = (float(random()) / RAND_MAX) * 100;

        if (isTable &&
            src.find_first_of("SELECT", off) == off) 
        {
            w *= 2;
        }
        if (w > freq) 
            return false;

        if (old_src == src)
        { // Same sql as last time.  Skip this time.
            old_src = "";
            return false;
        }
    }

    // Found a match.
    std::string kwlist;
    for (int i = 0; i < keyword.size() - 1; i++) 
    {
        kwlist += keyword[i];
        kwlist += '~';
    }
    kwlist += keyword[keyword.size()-1];

    if (log != NULL) 
        log->write_entry(LOG_DEBUG, 
                         "------ >> Markdown: n=%.1f, type=%s, kw=%S, src=%S", 
                         w, (isTable ? "TABLE" : "SQL"), 
                         kwlist.c_str(), src.c_str());

    old_src = src;
    return true;
}

time_t get_mod_time(const char* name)
{
    struct stat64 w;
    if (!stat64(name, &w))
        return w.st_mtime;

    return -1;
}

//---------------------------------------------------------------------------
// Load rule_table and rule_sql control files.
// 
// Extract keywords, frequency, host names and save in markdown filter list.
//---------------------------------------------------------------------------
void MarkdownList::load_control_files(const char *path,
                                      LogWriterBase *log)
{
    size_t LEN = 8000;
    char buf[LEN];
    char *pbuf = buf;

    std::ostringstream fname[2];

    if (path == NULL) 
    {
        return;
    }

    m_path = path;
    fname[0] << path << "/rule_table" << std::ends;
    fname[1] << path << "/rule_sql" << std::ends;

    for (int i = 0; i < 2; i++) 
    {
        const char *ptr = fname[i].str().c_str();
        time_t t = get_mod_time(ptr);

        if (t == -1 || t == m_last_mod_time[i]) 
            continue;

        // Need to reload the control file.
        srandom(getpid());
        if (log != NULL) 
            log->write_entry(LOG_DEBUG, "-- Markdown: Load Control File: %s", 
			     fname[i].str().c_str());

        m_last_mod_time[i] = t;
        m_filter[i].clear();
        
        FILE *fp = fopen(ptr, "r");

        // Parse a line consisting of keyword list, frequency and host name.
        //-------------------------------------------------------------------
        while (getline(&pbuf, &LEN, fp) > 0)
        {
            MarkdownFilter entry;
            entry.freq = 100;

            std::string tokens = buf;
            std::string kwlist, sqllist, freq, host;

            // Extract keyword list.
            //--------------------------------------------
            if (!StringUtil::tokenize(tokens, kwlist, '|'))
                continue;
          
            StringUtil::trim(kwlist);
            StringUtil::to_upper_case(kwlist);
            sqllist = kwlist;

            if (kwlist.empty()) 
                continue;
            
            if (i == 0) 
            {   // kwlist has a table name only.
              entry.keyword.push_back(kwlist);
            }
            else
            {   // Split SQL phrases in kwlist.     
              std::string kw;
              while (StringUtil::tokenize(kwlist, kw, '~')) 
              {
                StringUtil::trim(kw);
                if (!kw.empty()) 
                {
                  entry.keyword.push_back(kw);
                }
                        
              }
            }

            // Extract frequency.
            //--------------------------------------------
            if (!StringUtil::tokenize(tokens, freq, '|')) 
            {
                if (log != NULL) 
                {
                    log->write_entry(LOG_DEBUG, "---- kwlist=%S, freq=%d, host=%S", 
                                     sqllist.c_str(), entry.freq, entry.host.c_str()); 
                }
                m_filter[i].push_back(entry); 
                continue;
            }
           
            StringUtil::trim(freq);
            if (!freq.empty()) 
            {
                entry.freq = StringUtil::to_int(freq);
            }
            if (entry.freq > 100 || entry.freq < 0)
            {
                entry.freq = 0;
            }

            // Extract host name.
            //--------------------------------------------
            if (StringUtil::tokenize(tokens, host, '|')) 
            {
                StringUtil::trim(host);
                StringUtil::to_upper_case(host);
                entry.host = host;
            }

            if (log != NULL) 
            {
                log->write_entry(LOG_DEBUG, "---- kwlist=%S, freq=%d, host=%S", 
                                 sqllist.c_str(), entry.freq, entry.host.c_str()); 
            }
            m_filter[i].push_back(entry);
        } // while

        fclose(fp);
    }  // for
}

//---------------------------------------------------------------------------
// Loop through markdown filter list and find matching.
//
// Return true if the current sql operation needs to be markdowned. 
// and false otherwise.
//---------------------------------------------------------------------------
bool MarkdownList::doMarkdown(std::string host,
                              std::string host2,
                              const std::string &raw,
                              LogWriterBase *log)
{
    std::string src;

    if (isEmpty())
      return false;

    // Reload control files if modified.
    load_control_files(m_path, log);

    src = raw;
    StringUtil::trim(src);
    StringUtil::to_upper_case(src);

    StringUtil::to_upper_case(host);
    StringUtil::to_upper_case(host2);

    //--------------------------------------------------------
    // Loop through markdown filter list and find matching.
    //--------------------------------------------------------
    for (int k = 0; k < 2; k++)
    {
        for (int i = 0; i < m_filter[k].size(); i++) 
        {
            if (m_filter[k][i].match(src,
                                     raw,
                                     host, 
                                     host2,
                                     (k==0), 
                                     log))
            {
                return true;
            }
        }
    }

    return false;
}
