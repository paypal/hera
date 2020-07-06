#ifndef PP_FILEUTIL_H
#define PP_FILEUTIL_H

#include <iostream>

class FileUtil {
    public:
        static std::istream* istream_from_fd(int fd);
        static std::ostream* ostream_from_fd(int fd);
        static bool read_full(std::istream* is, std::string* buff, int n);
};

#endif
