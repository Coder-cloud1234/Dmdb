#include "DmdbConfigFileLoader.hpp"
#include "DmdbUtil.hpp"

namespace Dmdb {

DmdbConfigFileLoader::DmdbConfigFileLoader(const std::string &configFile) {
    _config_file = configFile;
}

DmdbConfigFileLoader::LineRet DmdbConfigFileLoader::GetParaNameFromLine(const std::string &fileLine, std::string &paraName) {
    size_t splitPos = fileLine.find("=");
    if(splitPos == fileLine.npos) {
        return LineRet::LINE_NO_PARA_VAL;
    }
    if(splitPos == 0) {
        return LineRet::LINE_NO_PARA_NAME;
    }
    paraName = fileLine.substr(0, splitPos);
    DmdbUtil::TrimString(paraName);
    if(paraName.length() <= 0) {
        return LineRet::LINE_EMPTY_PARA_NAME;
    } 
    return LineRet::LINE_OK;
}

DmdbConfigFileLoader::LineRet DmdbConfigFileLoader::GetParaValueFromLine(const std::string &fileLine, std::vector<std::string> &paraValue) {
    size_t startPos = fileLine.find("=");
    if(startPos == fileLine.npos || startPos == fileLine.length() - 1)
        return LineRet::LINE_NO_PARA_VAL;
    startPos++;
    size_t splitPos = 0;
    std::string arg;
    while(startPos < fileLine.length()) {
        splitPos = fileLine.find(",", startPos);
        splitPos = splitPos==fileLine.npos?fileLine.length():splitPos;
        arg = fileLine.substr(startPos, splitPos - startPos);
        DmdbUtil::TrimString(arg);
        if(arg.length() > 0)
            paraValue.emplace_back(arg);
        startPos = splitPos+1;
    }
    if(paraValue.size() == 0)
        return LineRet::LINE_EMPTY_PARA_VAL;
    return LineRet::LINE_OK;
}


void DmdbConfigFileLoader::LoadConfigFile(std::unordered_map<std::string, std::vector<std::string>> &parasMap) {
    std::ifstream configFileStream;
    std::string fileLine;
    std::string paraName;
    std::vector<std::string> paraValue;
    if(_config_file.length() == 0) {
        return;
    }
    configFileStream.open(_config_file.c_str(), std::ios::in);
    if(!configFileStream.is_open()) {
        DmdbUtil::ServerHandleOpenFileFailure(_config_file);
    }

    while(std::getline(configFileStream, fileLine)) {
        DmdbUtil::TrimString(fileLine);
        if(fileLine.length() == 0)
            continue; 
        if(GetParaNameFromLine(fileLine, paraName) != LineRet::LINE_OK ||
           GetParaValueFromLine(fileLine, paraValue) != LineRet::LINE_OK)
            continue;
        parasMap[paraName] = paraValue;
        //paraName.clear();
        paraValue.clear();
    }
    configFileStream.close();
    configFileStream.clear();
}

};