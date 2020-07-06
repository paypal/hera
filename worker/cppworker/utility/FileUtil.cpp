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
