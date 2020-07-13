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
#include "utility/FileUtil.h"

#include <iostream>
#include <ext/stdio_filebuf.h>

std::istream* FileUtil::istream_from_fd(int fd)
{
    __gnu_cxx::stdio_filebuf<char> *fb = new __gnu_cxx::stdio_filebuf<char>(fd, std::ios::in | std::ios::binary);
    return new std::istream(fb);
}

std::ostream* FileUtil::ostream_from_fd(int fd)
{
    __gnu_cxx::stdio_filebuf<char> *fb = new __gnu_cxx::stdio_filebuf<char>(fd, std::ios::out | std::ios::binary);
    return new std::ostream(fb);
}

bool FileUtil::read_full(std::istream* is, std::string* buff, int n)
{
    is->read((char*)(buff->c_str()), n);
    return is->good();
}
