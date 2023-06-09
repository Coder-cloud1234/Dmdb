#include "DmdbServerLogger.hpp"

namespace Dmdb {

DmdbServerLogger* DmdbServerLogger::_server_logger_instance = nullptr;
std::string DmdbServerLogger::_log_level_msg[4] = {"DEBUG", "VERBOSE", "NOTICE", "WARNING"};

DmdbServerLogger::DmdbServerLogger(const std::string &logFile, Verbosity verbosity)
{
    _server_log_verbosity = verbosity;
    _server_log_file = logFile;
    _server_log_stream.open(_server_log_file.c_str(), std::ios::out | std::ios::app);
    if(!_server_log_stream.is_open()) {
        DmdbUtil::ServerHandleOpenFileFailure(_server_log_file);
    }
}

DmdbServerLogger* DmdbServerLogger::GetUniqueServerLogger(const std::string &logFile,
                                                          Verbosity verbosity) {
    if(_server_logger_instance == nullptr) {
        _server_logger_instance = new DmdbServerLogger(logFile, verbosity);
    } 
    return _server_logger_instance;
}

bool DmdbServerLogger::String2Verboity(const std::string &strVerbosity, Verbosity &verbosity) {  
    std::string tmpStrVerbosity = strVerbosity; 
    std::transform(tmpStrVerbosity.begin(), tmpStrVerbosity.end(), tmpStrVerbosity.begin(), toupper);
    for(int i = static_cast<int>(Verbosity::DEBUG); static_cast<Verbosity>(i) <= Verbosity::WARNING; i++) {
        if(_log_level_msg[i] == tmpStrVerbosity) {
            verbosity = static_cast<Verbosity>(i);
            return true;
        }
    }
    return false;
}

void DmdbServerLogger::WriteToServerLog(Verbosity logLevel, const char *fmt, ...) {
    char logContentArray[LOG_CONTENT_MAX_LEN];
    va_list ap;
    if (logLevel < _server_log_verbosity || logLevel > Verbosity::WARNING)
        return;
    va_start(ap, fmt);
    vsnprintf(logContentArray, sizeof(logContentArray), fmt, ap);
    va_end(ap);

    pid_t pid = getpid();
    char buf[100] = {0};
    int off;
    struct timeval tv;

    gettimeofday(&tv,NULL);

    struct tm tmToGetDST;
    time_t ut = static_cast<long long>(tv.tv_sec);
    localtime_r(&ut,&tmToGetDST);
    //struct tm tm;
    DmdbUtil::LocalTime(&tmToGetDST,tv.tv_sec, timezone, tmToGetDST.tm_isdst);
    off = strftime(buf,sizeof(buf),"%d %b %Y %H:%M:%S.",&tmToGetDST);
    snprintf(buf+off,sizeof(buf)-off,"%03d",(int)tv.tv_usec/1000);

    std::string logContent = std::to_string(pid) + ":\t";
    logContent += std::string(buf) + "\t" + _log_level_msg[static_cast<uint8_t>(logLevel)] +
                    "\t" + logContentArray;
    _server_log_stream << logContent << std::endl;
    _server_log_stream.flush();
}

DmdbServerLogger::~DmdbServerLogger()
{
    _server_log_stream.close();
    _server_log_stream.clear();
}

}