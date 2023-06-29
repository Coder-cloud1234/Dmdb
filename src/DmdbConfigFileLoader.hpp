#pragma once

#include <string.h>

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>



namespace Dmdb {
enum class LineRet {
    LINE_OK,
    LINE_NO_PARA_VAL,
    LINE_NO_PARA_NAME,
    LINE_EMPTY_PARA_NAME,
    LINE_EMPTY_PARA_VAL
};

class DmdbConfigFileLoader {
public:
    enum class LineRet {
        LINE_OK,
        LINE_NO_PARA_VAL,
        LINE_NO_PARA_NAME,
        LINE_EMPTY_PARA_NAME,
        LINE_EMPTY_PARA_VAL
    };
    void LoadConfigFile(std::unordered_map<std::string, std::vector<std::string>> &parasMap);
    DmdbConfigFileLoader(const std::string &configFile);
    ~DmdbConfigFileLoader();
private: 
    std::string _config_file;
    LineRet GetParaNameFromLine(const std::string &fileLine, std::string &paraName);
    LineRet GetParaValueFromLine(const std::string &fileLine, std::vector<std::string> &paraValue);
};
}