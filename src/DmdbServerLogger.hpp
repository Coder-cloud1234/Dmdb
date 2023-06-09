#pragma once

#include <stdarg.h>
#include <sys/time.h>
#include <unistd.h>

#include <string>
#include <fstream>

#include "DmdbUtil.hpp"

namespace Dmdb {

class DmdbServerLogger
{
public:
    const size_t LOG_CONTENT_MAX_LEN = 1024;
    enum class Verbosity {
        DEBUG,
        VERBOSE,
        NOTICE,
        WARNING
    };
    static DmdbServerLogger* GetUniqueServerLogger(const std::string &logFile, Verbosity verbosity);
    void WriteToServerLog(Verbosity logLevel, const char *fmt, ...);
    static bool String2Verboity(const std::string &strVerbosity, Verbosity &verbosity); 
    ~DmdbServerLogger();

private:
    std::string _server_log_file;
    Verbosity _server_log_verbosity;
    std::fstream _server_log_stream;
    static DmdbServerLogger* _server_logger_instance;
    static std::string _log_level_msg[4];
    DmdbServerLogger(const std::string &logFile, Verbosity verbosity);
};


}