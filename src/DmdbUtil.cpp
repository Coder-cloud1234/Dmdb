
#include "DmdbUtil.hpp"

namespace Dmdb {

void TrimString(std::string &str) {
    while(str.length() > 0 && (str[0] == '\t' || str[0] == '\b'
            || str[0] == '\a' || str[0] == ' ')) {
        str.erase(0);
    }
    while(str.length() > 0 && (str[str.length()-1] == '\t'||
            str[str.length()-1] == '\b' || str[str.length()-1] == '\a'
            || str[str.length()-1] == ' ')) {
        str.erase(str.length()-1);
    }
}

LineRet GetParaNameFromLine(const std::string &fileLine, std::string &paraName) {
    int splitPos = fileLine.find("=");
    if(splitPos == fileLine.npos) {
        return LineRet::LINE_NO_PARA_VAL;
    }
    if(splitPos == 0) {
        return LineRet::LINE_NO_PARA_NAME;
    }
    std::string paraName = fileLine.substr(0, splitPos);
    TrimString(paraName);
    if(paraName.length() <= 0) {
        return LineRet::LINE_EMPTY_PARA_NAME;
    } 
    return LineRet::LINE_OK;
}

LineRet GetParaValueFromLine(const std::string &fileLine, std::vector<std::string> &paraValue) {
    int startPos = fileLine.find("=");
    if(startPos == fileLine.npos || startPos == fileLine.length() - 1)
        return LineRet::LINE_NO_PARA_VAL;
    startPos++;
    int splitPos = fileLine.find(",", startPos);
    std::string arg;
    while(splitPos != fileLine.npos) {
        arg = fileLine.substr(startPos, splitPos - startPos);
        TrimString(arg);
        paraValue.emplace_back(arg);
        startPos = splitPos+1;
        if(startPos >= fileLine.length())
            break;
        splitPos = fileLine.find(",", startPos);
    }
    if(paraValue.size() == 0)
        return LineRet::LINE_EMPTY_PARA_VAL;
    return LineRet::LINE_OK;
}

void LoadBaseConfigFile(const std::string &configFile, 
                                    std::unordered_map<std::string, std::vector<std::string>> &parasMap) {
    std::ifstream configFileStream;
    std::string fileLine;
    std::string paraName;
    std::vector<std::string> paraValue;

    configFileStream.open(configFile);
    if(!configFileStream) {
        /*TODO: print a log*/
        return;
    }

    while(std::getline(configFileStream, fileLine)) { 
        if(GetParaNameFromLine(fileLine, paraName) != LineRet::LINE_OK) {
            /*TODO: print a log*/
        }
        if(GetParaValueFromLine(fileLine, paraValue) != LineRet::LINE_OK) {
            /*TODO: print a log*/
        }
        parasMap[paraName] = paraValue;
        //paraName.clear();
        paraValue.clear();
    }
    configFileStream.close();
}

}