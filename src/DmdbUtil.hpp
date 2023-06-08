#pragma once

#include <unistd.h>
#include <string.h>

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>


namespace Dmdb {
class DmdbUtil {
public:
    static void ServerExitWithErrMsg(const std::string &errMsg);
    static void TrimString(std::string &str);
    static bool GetBoolFromString(std::string &str, bool &boolVal);
    static void ServerHandleOpenFileFailure(const std::string &fileName);
    static void ServerHandleStartListenFailure(int fd);
    static bool IsValidIPV4Address(const std::string &strIPV4);
    static void LocalTime(struct tm *tmp, time_t t, time_t tz, int dst);
    static uint64_t GetCurrentMs();
    static uint64_t Crc64(uint64_t crc, const unsigned char *s, uint64_t l);
private:
    static bool IsValidIPV4Num(const std::string &strNum);
    static int IsLeapYear(time_t year);
};


}