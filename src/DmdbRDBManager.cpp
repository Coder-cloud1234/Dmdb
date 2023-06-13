#include <sys/types.h>
#include <signal.h>

#include <fstream>

#include "DmdbRDBManager.hpp"
#include "DmdbServerFriends.hpp"
#include "DmdbServerLogger.hpp"
#include "DmdbUtil.hpp"
#include "DmdbDatabaseManager.hpp"

namespace Dmdb {

const std::string DMDB_MARK = "Dmdb";
const uint8_t RDB_VERSION = 1;
const uint8_t DMDB_EOF = 255;

const uint8_t TIME_STAMP_LENGTH = 8;
const uint8_t PREAMBLE_LEN = 1;
const uint32_t BUF_SIZE = 1024*1024;

DmdbRDBManager* DmdbRDBManager::_instance = nullptr;

#ifdef MAKE_TEST
void PrintData(uint8_t* buf, size_t bufLen) {
    for(size_t i = 0; i < bufLen; ++i) {
        std::cout << std::hex << static_cast<uint16_t>(buf[i]);
    }
    std::cout << std::endl;
}
#endif

bool DmdbRDBManager::IsRDBChildAlive() {
    return _rdb_child_pid > 0;
}

bool DmdbRDBManager::KillChildProcessIfAlive() {
    if(_rdb_child_pid > 0) {
        kill(_rdb_child_pid, SIGUSR1);
        /* TODO: remove RDB tmp file? */

        DmdbRDBRequiredComponents components;
        GetDmdbRDBRequiredComponents(components);
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE, 
                                                    "Killed the running RDB child process:%u",
                                                    _rdb_child_pid);
        _rdb_child_pid = -1;
        return true;
    }
    return false;
}

void DmdbRDBManager::GenerateRDBHeader(std::fstream &fs, DmdbRDBRequiredComponents &components, uint64_t &crcCode) {
    /* This will be optimized in the future */
    fs.write(DMDB_MARK.c_str(), DMDB_MARK.length());
    crcCode = DmdbUtil::Crc64(crcCode, (unsigned char*)DMDB_MARK.c_str(), DMDB_MARK.length());
    fs.write((char*)&_rdb_version, sizeof(_rdb_version));
    crcCode = DmdbUtil::Crc64(crcCode, (unsigned char*)&_rdb_version, sizeof(_rdb_version));
    fs.write((char*)&components._server_version, sizeof(components._server_version));
    crcCode = DmdbUtil::Crc64(crcCode, (unsigned char*)&components._server_version, sizeof(components._server_version));
    uint64_t currentMs = DmdbUtil::GetCurrentMs();
    fs.write((char*)&currentMs, sizeof(currentMs));
    crcCode = DmdbUtil::Crc64(crcCode, (unsigned char*)&currentMs, sizeof(currentMs));
    uint8_t isPreamble = *(components._is_preamble);
    fs.write((char*)&isPreamble, sizeof(isPreamble));
    crcCode = DmdbUtil::Crc64(crcCode, (unsigned char*)&isPreamble, sizeof(isPreamble));
    uint32_t dbSize = components._database_manager->GetDatabaseSize();
    fs.write((char*)&dbSize, sizeof(dbSize));
    crcCode = DmdbUtil::Crc64(crcCode, (unsigned char*)&dbSize, sizeof(dbSize));
}


/* The format of RDB file is:
 * "Dmdb": 4 bytes string
 * RDB_VERSION: 1 byte
 * Dmdb version: 1 byte
 * Time stamp: 8 bytes
 * AOF_PREAMBLE: 1 byte
 * DB_SIZE: 4 bytes
 * Key-values: unknown size, format raw data
 * EOF: 1 byte unsigned char
 * Checksum: 8 bytes unsigned long long int*/
bool DmdbRDBManager::SaveDatabaseToDisk() {
    /* TODO: Use RDB tmp file then rename it? */

    DmdbRDBRequiredComponents components;
    GetDmdbRDBRequiredComponents(components);
    std::fstream rdbStream;
    rdbStream.open(_rdb_file.c_str(), std::ios::out | std::ios::binary);
    /* Save rdb header */
    if(!rdbStream.is_open()) {
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                    "Failed to open rdb file: %s! Error info: %s",
                                                    _rdb_file.c_str(),
                                                    strerror(errno));
        return false;
    }
    _is_rdb_loading = true;

    uint64_t crcCode = 0;
    GenerateRDBHeader(rdbStream, components, crcCode);
    /*
    rdbStream.flush();
    rdbStream.close();

    rdbStream.open(_rdb_file.c_str(), std::ios::out | std::ios::binary);
    if(!rdbStream.is_open()) {
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                    "Failed to open rdb file: %s! Error info: %s",
                                                    _rdb_file.c_str(),
                                                    strerror(errno));
        return false;
    }
    */    
    size_t pairAmountOfThisCopy = 0, savedPairAmount = 0;
    size_t copiedSize = 0;
    uint8_t* pairsRawData = new uint8_t[BUF_SIZE]{0};
    
    while(savedPairAmount < components._database_manager->GetDatabaseSize()) {
        components._database_manager->GetNPairsFormatRawSequential(pairsRawData, BUF_SIZE, copiedSize, 
                                                                   100, pairAmountOfThisCopy);
        rdbStream.write((char*)pairsRawData, copiedSize);
        rdbStream.flush();
        crcCode = DmdbUtil::Crc64(crcCode, pairsRawData, copiedSize);
        memset(pairsRawData, 0, copiedSize);
        copiedSize = 0;
        savedPairAmount += pairAmountOfThisCopy;
        pairAmountOfThisCopy = 0;
    }
    /* Save eof */
    uint8_t eof = 255;
    rdbStream.write((char*)&eof, 1);
    rdbStream.flush();
    /* Compute crcCode and save it */
    crcCode = DmdbUtil::Crc64(crcCode, &eof, sizeof(eof));
    rdbStream.write((char*)&crcCode, sizeof(crcCode));
    rdbStream.close();
    delete pairsRawData;
    return true;
}


LoadRetCode DmdbRDBManager::GetOnePair(char* buf, size_t bufLen, 
                                       DmdbRDBRequiredComponents &components, size_t &pos,
                                       FieldOfSavedPair &field, bool isLast) {
    uint64_t expireTime = 0;
    uint32_t keyLen = 0;
    std::string keyName = "";
    uint8_t valType;
    uint32_t valLen = 0;
    size_t originalPos = pos;
    FieldOfSavedPair originalField = field;

    if(isLast) {
        if(field == FieldOfSavedPair::EXPIRE_TIME && static_cast<uint8_t>(buf[pos]) == DMDB_EOF)
            return LoadRetCode::END;
        else
            return LoadRetCode::CORRUPTION;       
    }

    if(field == FieldOfSavedPair::EXPIRE_TIME) {
        if(pos+sizeof(uint64_t) > bufLen)
            goto NOT_ENOUGH;
        expireTime = *((uint64_t*)(buf+pos));
        pos += sizeof(expireTime);
        field = FieldOfSavedPair::KEY_LEN;       
    }

    if(field == FieldOfSavedPair::KEY_LEN) {
        if(pos+sizeof(uint32_t) > bufLen)
            goto NOT_ENOUGH;
        keyLen = *((uint32_t*)(buf+pos));
        pos += sizeof(keyLen);
        field = FieldOfSavedPair::KEY_NAME;        
    }

    if(field == FieldOfSavedPair::KEY_NAME) {
        if(keyLen > BUF_SIZE)
            return LoadRetCode::INVALID_LENGTH;
        else if(pos+keyLen > bufLen)
            goto NOT_ENOUGH;
        for(uint32_t i = 0; i<keyLen; ++i) {
            keyName.push_back((buf+pos)[i]);
        }
        pos += keyLen;
        field = FieldOfSavedPair::VAL_TYPE;        
    }

    if(field == FieldOfSavedPair::VAL_TYPE) {
        if(pos+sizeof(uint8_t) > bufLen)
            goto NOT_ENOUGH;
        valType = *((uint8_t*)(buf+pos));
        pos += sizeof(valType);
        field = FieldOfSavedPair::VAL_LEN;        
    }

    if(field == FieldOfSavedPair::VAL_LEN) {
        if(pos+sizeof(uint32_t) > bufLen)
            goto NOT_ENOUGH;
        valLen = *((uint32_t*)(buf+pos));
        pos += sizeof(valLen);
        field = FieldOfSavedPair::VAL;        
    }

    if(field == FieldOfSavedPair::VAL) {
        if(valLen > BUF_SIZE)
            return LoadRetCode::INVALID_LENGTH;
        else if(pos+valLen>bufLen)
            goto NOT_ENOUGH;
        switch(static_cast<DmdbValueType>(valType)) {
            case DmdbValueType::STRING: {
                std::string valueStr = "";
                for(uint32_t i = 0; i<valLen; ++i) {
                    valueStr.push_back((buf+pos)[i]);
                }
                std::vector<std::string> vec;
                vec.emplace_back(valueStr);
                components._database_manager->SetKeyValuePair(keyName, vec, static_cast<DmdbValueType>(valType), expireTime);
                break;             
            }

        }
        pos += valLen;
        field = FieldOfSavedPair::EXPIRE_TIME;        
    }

    return LoadRetCode::OK;

NOT_ENOUGH:
    pos = originalPos;
    field = originalField;
    return LoadRetCode::NOT_ENOUGH;
}

bool DmdbRDBManager::LoadDatabaseFromDisk() {
    DmdbRDBRequiredComponents components;
    GetDmdbRDBRequiredComponents(components);    
    std::fstream rdbStream;
    char buf[BUF_SIZE] = {0};
    uint8_t rdbVersion = 0;
    uint8_t serverVersion = 0;
    uint32_t dbSize = 0;
    uint32_t getPairsCount = 0;
    uint64_t expectedCrcCode = 0;
    LoadRetCode everyLoadResult;
    std::streamsize expectReadCount = 0;
    std::streamsize remainingCountAfterOneProcess = 0;
    std::streamsize needToReadCountWhenEof = 0;
    FieldOfSavedPair fieldNow = FieldOfSavedPair::EXPIRE_TIME;
    size_t processedPos = 0;
    uint64_t savedCrcCode = 0;

    rdbStream.open(_rdb_file.c_str(), std::ios::in | std::ios::binary);
    if(!rdbStream.is_open()) {
        return false;
    }
    
    rdbStream.read(buf, DMDB_MARK.length());
    if(buf != DMDB_MARK) {
        goto corrupted_error;
    }
    expectedCrcCode = DmdbUtil::Crc64(expectedCrcCode, (uint8_t*)(buf), rdbStream.gcount());
    memset(buf, 0, sizeof(buf));

    rdbStream.read(buf, sizeof(RDB_VERSION));
    rdbVersion = *((uint8_t*)(buf));
    if(rdbVersion > _rdb_version) {
        goto corrupted_error;
    }
    expectedCrcCode = DmdbUtil::Crc64(expectedCrcCode, (uint8_t*)(buf), rdbStream.gcount());
    memset(buf, 0, sizeof(buf));

    rdbStream.read(buf, sizeof(components._server_version));
    serverVersion = *((uint8_t*)(buf));
    if(serverVersion > components._server_version) {
        goto corrupted_error;
    }
    expectedCrcCode = DmdbUtil::Crc64(expectedCrcCode, (uint8_t*)(buf), rdbStream.gcount());
    memset(buf, 0, sizeof(buf));

    rdbStream.read(buf, TIME_STAMP_LENGTH);
    expectedCrcCode = DmdbUtil::Crc64(expectedCrcCode, (uint8_t*)(buf), rdbStream.gcount());
    memset(buf, 0, sizeof(buf));

    rdbStream.read(buf, PREAMBLE_LEN);
    *components._is_preamble = buf[0]!=0 ? true:false;
    expectedCrcCode = DmdbUtil::Crc64(expectedCrcCode, (uint8_t*)(buf), rdbStream.gcount());
    memset(buf, 0, sizeof(buf));

    rdbStream.read(buf, sizeof(dbSize));
    dbSize = *(uint32_t*)(buf);
    expectedCrcCode = DmdbUtil::Crc64(expectedCrcCode, (uint8_t*)(buf), rdbStream.gcount());
    memset(buf, 0, sizeof(buf));

    expectReadCount = sizeof(buf);
    while(!rdbStream.eof()) {
        rdbStream.read(buf+remainingCountAfterOneProcess, expectReadCount);
        std::streamsize readCount = rdbStream.gcount();
        processedPos = 0;
        size_t dataLen = readCount+remainingCountAfterOneProcess;
        everyLoadResult = GetOnePair(buf, dataLen, components, processedPos, fieldNow, getPairsCount == dbSize);
        while(everyLoadResult == LoadRetCode::OK) {
            getPairsCount++;
            everyLoadResult = GetOnePair(buf, dataLen, components, processedPos, fieldNow, getPairsCount == dbSize);        
        }
        if(everyLoadResult == LoadRetCode::INVALID_LENGTH || everyLoadResult == LoadRetCode::CORRUPTION) {
            goto corrupted_error;
        } else if(everyLoadResult == LoadRetCode::NOT_ENOUGH) {
            expectedCrcCode = DmdbUtil::Crc64(expectedCrcCode, (uint8_t*)(buf), processedPos);
            memmove(buf, buf+processedPos, dataLen-processedPos);
            expectReadCount = processedPos;
            remainingCountAfterOneProcess = dataLen-processedPos;
        } else { /* everyLoadResult == LoadRetCode::END */
            expectedCrcCode = DmdbUtil::Crc64(expectedCrcCode, (uint8_t*)(buf), processedPos);
            memmove(buf, buf+processedPos, dataLen-processedPos);
            remainingCountAfterOneProcess = dataLen-processedPos;
            break;
        }
    }

    if(everyLoadResult != LoadRetCode::END) {
        goto corrupted_error;
    }
    /*
    if(fieldNow != FieldOfSavedPair::EXPIRE_TIME) {
        goto corrupted_error;
    }
    */

    /* If there is no corruption, only crccode and eof exist */
    if(static_cast<uint32_t>(remainingCountAfterOneProcess) > sizeof(expectedCrcCode)+sizeof(DMDB_EOF)) {
        goto corrupted_error;
    } else if(static_cast<uint32_t>(remainingCountAfterOneProcess) < sizeof(expectedCrcCode)+sizeof(DMDB_EOF)) {
        needToReadCountWhenEof = sizeof(expectedCrcCode)+sizeof(DMDB_EOF)-remainingCountAfterOneProcess;
        rdbStream.read(buf+remainingCountAfterOneProcess, needToReadCountWhenEof);
    }

    expectedCrcCode = DmdbUtil::Crc64(expectedCrcCode, (uint8_t*)(buf), sizeof(DMDB_EOF));
    savedCrcCode = *(uint64_t*)(buf+sizeof(DMDB_EOF));
    
    if(expectedCrcCode != savedCrcCode) {
        goto corrupted_error;
    }

    components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                                "RDB file:%s has been loaded successfully",
                                                _rdb_file);
    rdbStream.close();
    return true;

corrupted_error:
    components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                "RDB file:%s has been corrupted",
                                                _rdb_file.c_str());
    components._database_manager->Destroy();
    rdbStream.close();
    return false;
}

std::string DmdbRDBManager::GetRDBFile() {
    return _rdb_file;
}


DmdbRDBManager* DmdbRDBManager::GetUniqueRDBManagerInstance(const std::string &file) {
    if(_instance == nullptr) {
        _instance = new DmdbRDBManager(file);
    }
    return _instance;
}


DmdbRDBManager::DmdbRDBManager(const std::string &file) {
    _rdb_file = file;
    _rdb_version = RDB_VERSION;
    _rdb_child_pid = -1;
}

DmdbRDBManager::~DmdbRDBManager() {
    
}

}