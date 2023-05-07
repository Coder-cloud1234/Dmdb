
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>

#pragma once

namespace Dmdb {

enum class LineRet {
    LINE_OK,
    LINE_NO_PARA_VAL,
    LINE_NO_PARA_NAME,
    LINE_EMPTY_PARA_NAME,
    LINE_EMPTY_PARA_VAL
};

void LoadBaseConfigFile(const std::string &configFile, 
                        std::unordered_map<std::string, std::vector<std::string>> &parasMap);

LineRet GetParaNameFromLine(const std::string &fileLine, std::string &paraName);
LineRet GetParaValueFromLine(const std::string &fileLine, std::vector<std::string> &paraValue);

}