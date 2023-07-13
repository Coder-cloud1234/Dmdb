#include <sys/types.h>
#include <signal.h>
#include <unistd.h>
#include <stdio.h>
#include <wait.h>

#ifdef MAKE_TEST
#include <fcntl.h>
#endif


#include <fstream>

#include "DmdbRDBManager.hpp"
#include "DmdbServerFriends.hpp"
#include "DmdbServerLogger.hpp"
#include "DmdbClientManager.hpp"
#include "DmdbUtil.hpp"
#include "DmdbDatabaseManager.hpp"
#include "DmdbReplicationManager.hpp"
#include "DmdbServer.hpp"
#include "DmdbClientContact.hpp"

namespace Dmdb {

const std::string DMDB_MARK = "Dmdb";
const uint8_t RDB_VERSION = 1;
const uint8_t DMDB_EOF = 255;

const uint8_t TIME_STAMP_LENGTH = 8;
const uint8_t PREAMBLE_LEN = 1;
const uint32_t BUF_SIZE = 1024*1024;

DmdbRDBManager* DmdbRDBManager::_instance = nullptr;

bool DmdbRDBManager::IsRDBChildAlive() {
    return _rdb_child_pid > 0;
}

bool DmdbRDBManager::IsMyselfRDBChild() {
    return _my_parent_pid > 0;
}

bool DmdbRDBManager::RemoveRdbChildTmpFile() {
    if(_rdb_child_pid > 0 && _rdb_child_for_replica_fd > 0) {
        std::string tmpFile = std::to_string(_rdb_child_pid) + "_" + std::to_string(_rdb_child_start_ms) + ".rdb";
        unlink(tmpFile.c_str());
        return true;
    }
    return false;
}

bool DmdbRDBManager::KillChildProcessIfAlive() {
    if(_rdb_child_pid > 0) {
        int statLoc;
        kill(_rdb_child_pid, SIGTERM);
        waitpid(_rdb_child_pid, &statLoc, WSTOPPED);
        if(_rdb_child_for_replica_fd > 0) {
            std::string tmpFile = std::to_string(_rdb_child_pid) + "_" + std::to_string(_rdb_child_start_ms) + ".rdb";
            unlink(tmpFile.c_str());            
        }
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

size_t DmdbRDBManager::GenerateRDBHeader(uint8_t* buf, size_t bufLen, DmdbRDBRequiredComponents &components, bool isForReplica = false) {
    size_t genSize = 0;
    uint64_t currentMs = 0;
    uint8_t isPreamble = 0;
    long long replOffset = 0;
    uint32_t dbSize = 0;
    uint64_t totalTransferBytes = 0;
    size_t planSize = DMDB_MARK.length() + sizeof(_rdb_version) + 
                      sizeof(components._server_version) + sizeof(currentMs) + 
                      sizeof(isPreamble) + sizeof(replOffset) + sizeof(dbSize);
    if(isForReplica)
        planSize += sizeof(totalTransferBytes);

    if(planSize > bufLen) {
        return genSize;
    }

    if(isForReplica) {
        /* The space size of saving all pairs of database */
        totalTransferBytes = components._database_manager->GetTotalBytesOfPairsWhenSave();
        /* Header size, EOF and crc checksum size */
        totalTransferBytes += planSize + sizeof(DMDB_EOF) + sizeof(uint64_t);
        memcpy(buf+genSize, (uint64_t*)&totalTransferBytes, sizeof(totalTransferBytes));
        genSize += sizeof(totalTransferBytes);          
    }
    memcpy(buf+genSize, DMDB_MARK.c_str(), DMDB_MARK.length());
    genSize += DMDB_MARK.length();

    memcpy(buf+genSize, (uint8_t*)&_rdb_version, sizeof(_rdb_version));
    genSize += sizeof(_rdb_version);

    memcpy(buf+genSize, (uint8_t*)&components._server_version, sizeof(components._server_version));
    genSize += sizeof(components._server_version);

    currentMs = DmdbUtil::GetCurrentMs();
    memcpy(buf+genSize, (uint8_t*)&currentMs, sizeof(currentMs));
    genSize += sizeof(currentMs);

    isPreamble = *(components._is_preamble);
    memcpy(buf+genSize, (uint8_t*)&isPreamble, sizeof(isPreamble));
    genSize += sizeof(isPreamble);

    replOffset = components._repl_manager->GetReplOffset();
    memcpy(buf+genSize, (uint8_t*)&replOffset, sizeof(replOffset));
    genSize += sizeof(replOffset);

    dbSize = components._database_manager->GetDatabaseSize();
    memcpy(buf+genSize, (uint8_t*)&dbSize, sizeof(dbSize));
    genSize += sizeof(dbSize);

    return genSize;

    /*
    fs.write(DMDB_MARK.c_str(), DMDB_MARK.length());
    crcCode = DmdbUtil::Crc64(crcCode, (unsigned char*)DMDB_MARK.c_str(), DMDB_MARK.length());
    fs.write((char*)&_rdb_version, sizeof(_rdb_version));
    crcCode = DmdbUtil::Crc64(crcCode, (unsigned char*)&_rdb_version, sizeof(_rdb_version));
    fs.write((char*)&components._server_version, sizeof(components._server_version));
    crcCode = DmdbUtil::Crc64(crcCode, (unsigned char*)&components._server_version, sizeof(components._server_version));
    currentMs = DmdbUtil::GetCurrentMs();
    fs.write((char*)&currentMs, sizeof(currentMs));
    crcCode = DmdbUtil::Crc64(crcCode, (unsigned char*)&currentMs, sizeof(currentMs));
    isPreamble = *(components._is_preamble);
    fs.write((char*)&isPreamble, sizeof(isPreamble));
    crcCode = DmdbUtil::Crc64(crcCode, (unsigned char*)&isPreamble, sizeof(isPreamble));
    dbSize = components._database_manager->GetDatabaseSize();
    fs.write((char*)&dbSize, sizeof(dbSize));
    crcCode = DmdbUtil::Crc64(crcCode, (unsigned char*)&dbSize, sizeof(dbSize));
    */
}

/*
bool DmdbRDBManager::SaveDatabaseToDisk() {
    DmdbRDBRequiredComponents components;
    GetDmdbRDBRequiredComponents(components);
    std::fstream rdbStream;
    rdbStream.open(_rdb_file.c_str(), std::ios::out | std::ios::binary);

    if(!rdbStream.is_open()) {
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                    "Failed to open rdb file: %s! Error info: %s",
                                                    _rdb_file.c_str(),
                                                    strerror(errno));
        return false;
    }
    

    uint64_t crcCode = 0;
    GenerateRDBHeader(rdbStream, components, crcCode);
  
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

    uint8_t eof = 255;
    rdbStream.write((char*)&eof, 1);
    rdbStream.flush();

    crcCode = DmdbUtil::Crc64(crcCode, &eof, sizeof(eof));
    rdbStream.write((char*)&crcCode, sizeof(crcCode));
    rdbStream.close();
    delete pairsRawData;
    return true;
}
*/

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
    std::string valueStr = "";

    if(isLast) {
        if(field == FieldOfSavedPair::EXPIRE_TIME && static_cast<uint8_t>(buf[pos]) == DMDB_EOF)
            return LoadRetCode::END;
        else
            return LoadRetCode::CORRUPTION;       
    }

    if(field == FieldOfSavedPair::EXPIRE_TIME) {
        if(pos+sizeof(expireTime) > bufLen)
            goto NOT_ENOUGH;
        expireTime = *((uint64_t*)(buf+pos));
        pos += sizeof(expireTime);
        field = FieldOfSavedPair::KEY_LEN;       
    }

    if(field == FieldOfSavedPair::KEY_LEN) {
        if(pos+sizeof(keyLen) > bufLen)
            goto NOT_ENOUGH;
        keyLen = *((uint32_t*)(buf+pos));
        pos += sizeof(keyLen);
        field = FieldOfSavedPair::KEY_NAME;        
    }

    if(field == FieldOfSavedPair::KEY_NAME) {
        if(keyLen > BUF_SIZE) {
            return LoadRetCode::INVALID_LENGTH;
        } else if(pos+keyLen > bufLen) {
            goto NOT_ENOUGH;
        }
        for(uint32_t i = 0; i<keyLen; ++i) {
            keyName.push_back((buf+pos)[i]);
        }
        pos += keyLen;
        field = FieldOfSavedPair::VAL_TYPE;        
    }

    if(field == FieldOfSavedPair::VAL_TYPE) {
        if(pos+sizeof(valType) > bufLen)
            goto NOT_ENOUGH;
        valType = *((uint8_t*)(buf+pos));
        pos += sizeof(valType);
        field = FieldOfSavedPair::VAL_LEN;        
    }

    if(field == FieldOfSavedPair::VAL_LEN) {
        if(pos+sizeof(valLen) > bufLen)
            goto NOT_ENOUGH;
        valLen = *((uint32_t*)(buf+pos));
        pos += sizeof(valLen);
        field = FieldOfSavedPair::VAL;        
    }

    if(field == FieldOfSavedPair::VAL) {
        if(valLen > BUF_SIZE) {
            return LoadRetCode::INVALID_LENGTH;
        } else if(pos+valLen>bufLen) {
            goto NOT_ENOUGH;
        }
        switch(static_cast<DmdbValueType>(valType)) {
            case DmdbValueType::STRING: {
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

/*
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
    _is_rdb_loading = true;
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
        } else {
            expectedCrcCode = DmdbUtil::Crc64(expectedCrcCode, (uint8_t*)(buf), processedPos);
            memmove(buf, buf+processedPos, dataLen-processedPos);
            remainingCountAfterOneProcess = dataLen-processedPos;
            break;
        }
    }

    if(everyLoadResult != LoadRetCode::END) {
        goto corrupted_error;
    }


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
    _is_rdb_loading = false;
    return true;

corrupted_error:
    components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                "RDB file:%s has been corrupted",
                                                _rdb_file.c_str());
    components._database_manager->Destroy();
    rdbStream.close();
    _is_rdb_loading = false;
    return false;
}
*/

bool DmdbRDBManager::IsErrorOccurs(const char* replBuf) {
    std::string msg = replBuf;
    if(msg.find("-ERR") != std::string::npos) {
        return true;
    }
    return false;
}

/* fd<0 means loading rdb data from rdb file, otherwise replicate rdb data from master */
bool DmdbRDBManager::LoadDatabase(int fd) {
    DmdbRDBRequiredComponents components;
    GetDmdbRDBRequiredComponents(components);    
    std::fstream rdbStream;
    char buf[BUF_SIZE] = {0};
    char dmdbMark[5] = {0};
    uint8_t rdbVersion = 0;
    uint8_t serverVersion = 0;
    long long replOffset = 0;
    uint32_t dbSize = 0;
    uint32_t getPairsCount = 0;
    uint64_t expectedCrcCode = 0;
    LoadRetCode everyLoadResult;
    uint32_t expectReadCount = 0;
    uint32_t remainingCountAfterOneProcess = 0;
    uint32_t needToReadCountWhenEof = 0;
    FieldOfSavedPair fieldNow = FieldOfSavedPair::EXPIRE_TIME;
    size_t processedPos = 0;
    uint64_t savedCrcCode = 0;
    int readRet = 0;
    uint64_t shouldReadBytesFromMaster = 0;
    uint64_t readBytesFromMaster = 0;
    bool isReadOver = false;
    uint8_t headerSize = DMDB_MARK.length() + sizeof(RDB_VERSION) + sizeof(components._server_version) +
                         TIME_STAMP_LENGTH + PREAMBLE_LEN + sizeof(replOffset) + sizeof(dbSize);
    uint8_t headerPos = 0;

    if(fd < 0) {
        rdbStream.open(_rdb_file.c_str(), std::ios::in | std::ios::binary);
        if(!rdbStream.is_open()) {
            return false;
        }
        _is_rdb_loading = true;
        rdbStream.read(buf, headerSize);
        if(rdbStream.bad()) {
            goto read_err;
        }          
    } else {
        /* If master failed to replicate, the data will start with "-ERR ", so we need to have a check */
        readRet = read(fd, buf, 5);
        if(readRet < 0)
            goto read_err;
        bool isStartError = IsErrorOccurs(buf);
        readBytesFromMaster += 5;
        if(isStartError) {
            /* Calling this function will remove "\r\n" and cover the 5 bytes of "-ERR " in buf */
            DmdbUtil::RecvLineFromSocket(fd, buf, sizeof(buf));
            components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                        buf);
            return false; 
        }
        headerSize += sizeof(shouldReadBytesFromMaster);
        readRet = read(fd, buf+5, headerSize-5);
        if(readRet < 0) {
            goto read_err;
        }

        readBytesFromMaster += readRet;
        shouldReadBytesFromMaster = *((uint64_t*)buf);
        headerPos += sizeof(shouldReadBytesFromMaster);
    }

    memcpy(dmdbMark, buf+headerPos, DMDB_MARK.length());
    if (std::string(dmdbMark) != DMDB_MARK) {
        goto corrupted_error;
    }
    headerPos += DMDB_MARK.length();
    rdbVersion = *((uint8_t*)(buf+headerPos));
    if(rdbVersion > _rdb_version) {
        goto corrupted_error;
    }
    headerPos += sizeof(RDB_VERSION);
    serverVersion = *((uint8_t*)(buf+headerPos));
    if(serverVersion > components._server_version) {
        goto corrupted_error;
    }
    headerPos += sizeof(components._server_version);
    headerPos += TIME_STAMP_LENGTH; /* We dismiss time stamp currently */
    *components._is_preamble = buf[headerPos]!=0 ? true:false;
    headerPos += PREAMBLE_LEN;
    replOffset = *(long long*)(buf+headerPos);
    headerPos += sizeof(replOffset);
    dbSize = *(uint32_t*)(buf+headerPos);
    headerPos += sizeof(dbSize);
    expectedCrcCode = DmdbUtil::Crc64(expectedCrcCode, (uint8_t*)(buf), headerSize);

    memset(buf, 0, headerSize);

    isReadOver = fd<0 ? rdbStream.eof() : readBytesFromMaster>=shouldReadBytesFromMaster;
    expectReadCount = sizeof(buf);
    while(!isReadOver) {
        size_t readCountThisRound = 0;
        if(fd < 0) {
            while(!rdbStream.eof() && readCountThisRound<expectReadCount) {
                rdbStream.read(buf+remainingCountAfterOneProcess+readCountThisRound, expectReadCount-readCountThisRound);
                if(rdbStream.bad()) {
                    goto read_err;
                }
                readCountThisRound += rdbStream.gcount();               
            }
        } else {
            while(readBytesFromMaster<shouldReadBytesFromMaster && readCountThisRound<expectReadCount) {
                readRet = read(fd, buf+remainingCountAfterOneProcess+readCountThisRound, expectReadCount-readCountThisRound);
                if(readRet < 0) {
                    goto read_err;
                }
                readCountThisRound += readRet;
                readBytesFromMaster += readRet;               
            }

        }
        
        processedPos = 0;
        size_t dataLen = readCountThisRound+remainingCountAfterOneProcess;
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
        } else {
            /* everyLoadResult == LoadRetCode::END */
            expectedCrcCode = DmdbUtil::Crc64(expectedCrcCode, (uint8_t*)(buf), processedPos);
            memmove(buf, buf+processedPos, dataLen-processedPos);
            remainingCountAfterOneProcess = dataLen-processedPos;
            break;
        }
        isReadOver = fd<0 ? rdbStream.eof() : readBytesFromMaster>=shouldReadBytesFromMaster;

    }

    if(everyLoadResult != LoadRetCode::END) {
        goto corrupted_error;
    }

    if(static_cast<uint32_t>(remainingCountAfterOneProcess) > sizeof(expectedCrcCode)+sizeof(DMDB_EOF)) {
        goto corrupted_error;
    } else if(static_cast<uint32_t>(remainingCountAfterOneProcess) < sizeof(expectedCrcCode)+sizeof(DMDB_EOF)) {
        needToReadCountWhenEof = sizeof(expectedCrcCode)+sizeof(DMDB_EOF)-remainingCountAfterOneProcess;
        if(fd < 0) {
            rdbStream.read(buf+remainingCountAfterOneProcess, needToReadCountWhenEof);
            if(rdbStream.bad()) {
                goto read_err;
            }
        } else {
            readRet = read(fd, buf+remainingCountAfterOneProcess, needToReadCountWhenEof);
            if(readRet < 0) {
                goto read_err;
            }
            readBytesFromMaster += readRet;
        }
    }

    expectedCrcCode = DmdbUtil::Crc64(expectedCrcCode, (uint8_t*)(buf), sizeof(DMDB_EOF));
    savedCrcCode = *(uint64_t*)(buf+sizeof(DMDB_EOF));
    
    if(expectedCrcCode != savedCrcCode) {
        goto corrupted_error;
    }
    if(fd < 0) {
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                                    "RDB file:%s has been loaded successfully",
                                                    _rdb_file.c_str());
        rdbStream.close();
        _is_rdb_loading = false;        
    } else {
        /* If it is replication from master, fd should be managed by replication manager,
         * we won't close fd in this function */
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                                    "RDB data has been received successfully");        
    }
    /* This function makes sense for both mater and replica */
    components._repl_manager->SetReplOffset(replOffset);
    return true;

corrupted_error:
    if(fd < 0) {
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                    "RDB file:%s has been corrupted",
                                                    _rdb_file.c_str());
        rdbStream.close();
        _is_rdb_loading = false;                
    } else {
        /* If it is replication from master, fd should be managed by replication manager,
         * we won't close fd in this function */
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                    "RDB data has been corrupted");        
    }
    components._database_manager->Destroy();
    return false;

read_err:
    if(fd < 0) {
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                    "Failed to read RDB file:%s",
                                                    _rdb_file.c_str());
        rdbStream.close();
        _is_rdb_loading = false;        
    } else {
        /* If it is replication from master, fd should be managed by replication manager,
         * we won't close fd in this function */
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                    "Failed to receive RDB data");        
    }
    components._database_manager->Destroy();
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

void DmdbRDBManager::WriteDataToPipeIfNeed(const std::string& data, bool isBgSave) {
    if(isBgSave) {
        write(_pipe_with_child[1], data.c_str(), data.length());
    }
}

bool DmdbRDBManager::ReceiveRetCodeFromPipe(SaveRetCode &retCode, bool isBgSave) {
    if(isBgSave) {
        char buf[10] = {0};
        int ret = read(_pipe_with_child[0], buf, sizeof(buf));
        if (ret <= 0) {
            return false;
        }
        /* We assume data we read can always be transfered into SaveRetCode value */
        retCode = static_cast<SaveRetCode>(atoi(buf));
        return true;
    }
    return false;
}

/* fd == -1 means we need to save data into disk, otherwise into replica,
 * then we will add a uint64_t value at first to tell how many bytes we need to replicate
 * The format of RDB file is:
 * "Dmdb": 4 bytes string
 * RDB_VERSION: 1 byte
 * Dmdb version: 1 byte
 * Time stamp: 8 bytes
 * AOF_PREAMBLE: 1 byte
 * Replication offset: 8 bytes
 * DB_SIZE: 4 bytes
 * Key-values: unknown size, format raw data
 * EOF: 1 byte unsigned char
 * Checksum: 8 bytes unsigned long long int */
SaveRetCode DmdbRDBManager::SaveData(int fd, bool isBgSave) {
    DmdbRDBRequiredComponents components;
    GetDmdbRDBRequiredComponents(components);
    pid_t myPid = getpid();
    std::fstream rdbStream;
    std::string fileToSave = "";
    if(isBgSave)
        fileToSave = std::to_string(myPid) + "_" + std::to_string(_rdb_child_start_ms) + ".rdb";
    else
        fileToSave = _rdb_file;
    if(fd < 0) {
        rdbStream.open(fileToSave.c_str(), std::ios::out | std::ios::binary);
        if(!rdbStream.is_open()) {
            components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                        "Failed to open rdb file: %s! Error info: %s",
                                                        fileToSave.c_str(),
                                                        strerror(errno));
            WriteDataToPipeIfNeed(std::to_string(static_cast<int>(SaveRetCode::OPEN_ERR)), isBgSave);
            return SaveRetCode::OPEN_ERR;
        }        
    }

    uint8_t* pairsRawData = new uint8_t[BUF_SIZE]{0};
    uint64_t crcCode = 0;
    size_t headerSize = GenerateRDBHeader(pairsRawData, BUF_SIZE, components, fd>0);
    if(fd > 0) {
        if(write(fd, (char*)pairsRawData, headerSize) < 0) {
            WriteDataToPipeIfNeed(std::to_string(static_cast<int>(SaveRetCode::SEND_ERR)), isBgSave);
            delete[] pairsRawData;
            return SaveRetCode::SEND_ERR;
        }
    } else {
        rdbStream.write((char*)pairsRawData, headerSize);
        if(rdbStream.bad()) {
            WriteDataToPipeIfNeed(std::to_string(static_cast<int>(SaveRetCode::WRITE_ERR)), isBgSave);
            delete[] pairsRawData;
            return SaveRetCode::WRITE_ERR;
        }
            
    }
    crcCode = DmdbUtil::Crc64(crcCode, pairsRawData, headerSize);
    memset(pairsRawData, 0, headerSize);
    size_t pairAmountOfThisCopy = 0, savedPairAmount = 0;
    size_t copiedSize = 0;
    
    while(savedPairAmount < components._database_manager->GetDatabaseSize()) {
        components._database_manager->GetNPairsFormatRawSequential(pairsRawData, BUF_SIZE, copiedSize, 
                                                                   100, pairAmountOfThisCopy);
        size_t writeCountOfThisRound = 0;
        if(fd < 0) {
            while(writeCountOfThisRound < copiedSize) {
                size_t before = rdbStream.tellp();
                rdbStream.write((char*)pairsRawData+writeCountOfThisRound, copiedSize-writeCountOfThisRound);
                if(rdbStream.bad()) {
                    WriteDataToPipeIfNeed(std::to_string(static_cast<int>(SaveRetCode::WRITE_ERR)), isBgSave);
                    delete[] pairsRawData;
                    return SaveRetCode::WRITE_ERR;
                }
                writeCountOfThisRound += rdbStream.tellp() - before;
            }              
        } else {
            while(writeCountOfThisRound < copiedSize) {
                int ret = write(fd, (char*)pairsRawData+writeCountOfThisRound, copiedSize-writeCountOfThisRound);
                if(ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                    WriteDataToPipeIfNeed(std::to_string(static_cast<int>(SaveRetCode::SEND_ERR)), isBgSave);
                    delete[] pairsRawData;
                    return SaveRetCode::SEND_ERR;
                }
                if(ret > 0)
                    writeCountOfThisRound += ret;
            }
        }
        crcCode = DmdbUtil::Crc64(crcCode, pairsRawData, copiedSize);
        memset(pairsRawData, 0, copiedSize);
        copiedSize = 0;
        savedPairAmount += pairAmountOfThisCopy;
        pairAmountOfThisCopy = 0;
    }
    /* Save eof */
    uint8_t eof = 255;
    if(fd == -1) {
        rdbStream.write((char*)&eof, sizeof(eof));
        if (rdbStream.bad()) {
            WriteDataToPipeIfNeed(std::to_string(static_cast<int>(SaveRetCode::WRITE_ERR)), isBgSave);
            delete[] pairsRawData;
            return SaveRetCode::WRITE_ERR;
        }
                    
    } else {
        if(write(fd, (char*)&eof, sizeof(eof)) < 0) {
            WriteDataToPipeIfNeed(std::to_string(static_cast<int>(SaveRetCode::SEND_ERR)), isBgSave);
            delete[] pairsRawData;
            return SaveRetCode::SEND_ERR;
        }
    }
    /* Compute crcCode*/
    crcCode = DmdbUtil::Crc64(crcCode, &eof, sizeof(eof));

    /* Save crcCode */
    if(fd == -1) {
        rdbStream.write((char*)&crcCode, sizeof(crcCode));
        if (rdbStream.bad()) {
            WriteDataToPipeIfNeed(std::to_string(static_cast<int>(SaveRetCode::WRITE_ERR)), isBgSave);
            delete[] pairsRawData;
            return SaveRetCode::WRITE_ERR;
        }    
        rdbStream.flush();
        rdbStream.close();
        if(isBgSave)
            rename(fileToSave.c_str(), _rdb_file.c_str());      
    } else {
        /* If it is replica's fd, we can't close it */
        if(write(fd, (char*)&crcCode, sizeof(crcCode)) < 0) {
            WriteDataToPipeIfNeed(std::to_string(static_cast<int>(SaveRetCode::SEND_ERR)), isBgSave);
            delete[] pairsRawData;
            return SaveRetCode::SEND_ERR;
        }
    }
    delete[] pairsRawData;
    WriteDataToPipeIfNeed(std::to_string(static_cast<int>(SaveRetCode::SAVE_OK)), isBgSave);
    return SaveRetCode::SAVE_OK;
}


/* fd < 0 means we need to save data into disk, otherwise into replica */
bool DmdbRDBManager::BackgroundSave() {
    DmdbRDBRequiredComponents components;
    GetDmdbRDBRequiredComponents(components);
    if(pipe(_pipe_with_child) == -1) {
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                    "Failed to create pipes! Error info:%s", 
                                                    strerror(errno));
        /* The reason why we set _rdb_child_pid=-1 here is that sometimes child is created successfully before but fails to complete its task,
         * if we don't set, CheckRdbChildFinished() may execute wrongly */
        _rdb_child_pid = -1;
        ClearBackgroundSavePlan();
        return false;
    }
    pid_t parentPid = getpid();
    _rdb_child_start_ms = DmdbUtil::GetCurrentMs();
    pid_t pid = fork();
    if(pid < 0) {
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING, 
                                                    "Failed to fork RDB child process! Error info:%s", 
                                                    strerror(errno));
        /* The reason why we set _rdb_child_pid=-1 here is that sometimes child is created successfully before but fails to complete its task,
         * if we don't set, CheckRdbChildFinished() may execute wrongly */
        _rdb_child_pid = -1;
        ClearBackgroundSavePlan();
        return false;
    } else if(pid == 0) {
        DmdbServer::ClearSignalHandler();
        _my_parent_pid = parentPid;
        close(_pipe_with_child[0]); /* Close reading fd */
        bool isSaveOk = SaveData(_rdb_child_for_replica_fd, true) == SaveRetCode::SAVE_OK;
        close(_pipe_with_child[1]);
        exit(isSaveOk?0:1);
        return isSaveOk;
    } else {
        /* Close writting fd */
        close(_pipe_with_child[1]);
        //_rdb_child_for_replica_fd = fd;
        _rdb_child_pid = pid;
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE, "RDB child process:%d has been created", _rdb_child_pid);
        return true;
    }
}

void DmdbRDBManager::CheckRdbChildFinished() {
    DmdbRDBRequiredComponents components;
    GetDmdbRDBRequiredComponents(components);    
    int statloc;
    if(_rdb_child_pid < 0)
        return;
    /* WNOHANG means non-blocking waiting */
    pid_t checkPid = waitpid(_rdb_child_pid, &statloc, WNOHANG);

    /* Error occurs */
    if(checkPid < 0) {
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                    "Error happens when waiting rdb child process exiting! Error info:%s",
                                                    strerror(errno));
        return;
    }

    /* "checkPid == 0" means no child process exits in this call when it is non-block waiting */
    if(checkPid > 0) {
        bool isKilledBySignal = WIFSIGNALED(statloc); 
        if(isKilledBySignal){
            components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                                        "RDB Child process:%d is killed by signal",
                                                        checkPid);            
        }
        bool isNormalExit = WIFEXITED(statloc);
        if(isNormalExit) {
            components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                                        "RDB Child process:%d exits successfully",
                                                        checkPid);
        }
        HandleAfterChildExit(isKilledBySignal);
    }
}

void DmdbRDBManager::HandleAfterChildExit(bool isKilledBySignal) {
    DmdbRDBRequiredComponents components;
    GetDmdbRDBRequiredComponents(components);
    std::string logContent = "";
    SaveRetCode retCode = SaveRetCode::NONE;
    std::string tmpRdbFile;
    
    if(isKilledBySignal) { /* If rdb child process is killed by signal, we can't read data from pipe */
        logContent = "RDB child process is killed by signal!";
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE, logContent.c_str());
        if(_rdb_child_for_replica_fd == -1) {
            tmpRdbFile = std::to_string(_rdb_child_pid) + "_" + std::to_string(_rdb_child_start_ms) + ".rdb";
            unlink(tmpRdbFile.c_str());
            logContent = "Failed to save RDB data";
        } else {
            logContent = "Failed to full sync RDB data to replica:" + components._client_manager->GetNameOfClient(_rdb_child_for_replica_fd);
        }
    } else {
        ReceiveRetCodeFromPipe(retCode, true);
        if(retCode == SaveRetCode::SAVE_OK) {
            if(_rdb_child_for_replica_fd < 0) {
                
                logContent = "RDB data have been saved successfully";                
            } else {
                logContent = "RDB data have been replicated to replica:" + components._client_manager->GetNameOfClient(_rdb_child_for_replica_fd) +
                             " successfully";
            }
        } else {
            if(_rdb_child_for_replica_fd < 0) {
                tmpRdbFile = std::to_string(_rdb_child_pid) + "_" + std::to_string(_rdb_child_start_ms) + ".rdb";
                unlink(tmpRdbFile.c_str());
                logContent = "Failed to save RDB data";
            } else { /* If it failed for replication, the retCode must be SEND_ERR */
                logContent = "Disconnected with replica:" + components._client_manager->GetNameOfClient(_rdb_child_for_replica_fd);
                components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE, logContent.c_str());                    
                logContent = "Failed to full sync RDB data to replica:" + components._client_manager->GetNameOfClient(_rdb_child_for_replica_fd);
            }
        } 
    }
    components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE, logContent.c_str());
    if(_rdb_child_for_replica_fd > 0) {
        components._repl_manager->HandleFullSyncOver(_rdb_child_for_replica_fd, retCode==SaveRetCode::SAVE_OK, retCode==SaveRetCode::SEND_ERR);
    }
    close(_pipe_with_child[0]);
    ClearBackgroundSavePlan();
    _rdb_child_start_ms = 0;
    _rdb_child_pid = -1;
}

void DmdbRDBManager::SetBackgroundSavePlan(int clientFd, int replicaFd) {
    _rdb_child_for_client_fd = clientFd;
    _rdb_child_for_replica_fd = replicaFd;
    _is_plan_to_bgsave_rdb = true;
}

void DmdbRDBManager::ClearBackgroundSavePlan() {
    _is_plan_to_bgsave_rdb = false;
    _rdb_child_for_replica_fd = -1;
    _rdb_child_for_client_fd = -1;
}

void DmdbRDBManager::FeedbackToClientOfRdbChild(DmdbRDBRequiredComponents &components, const std::string& feedback) {
    DmdbClientContact* clientContact = components._client_manager->GetClientContactByFd(_rdb_child_for_client_fd);
    if(clientContact != nullptr) {
        clientContact->AddReplyData2Client(feedback);
    }    
}

void DmdbRDBManager::BackgroundSaveIfNeed() {
    DmdbRDBRequiredComponents components;
    GetDmdbRDBRequiredComponents(components);
    /* Rdb child process is running, we can't do it again */
    if(_rdb_child_pid > 0) {
        return;
    }
    if(!_is_plan_to_bgsave_rdb) {
        return;
    }
    
    bool isCreateChildOk = BackgroundSave();
    if(!isCreateChildOk) {
        if (_rdb_child_for_replica_fd > 0)
            FeedbackToClientOfRdbChild(components, "-ERR Replication can't be finished\r\n");
        else
            FeedbackToClientOfRdbChild(components, "-ERR RDB data can't be saved into disk in the background\r\n");
    }
}

void DmdbRDBManager::RdbCheckAndFinishJob() {
    /* The functions' call order must be as below, if not, it is possible that child have finished its job but we still execute
     * saving in the background */
    CheckRdbChildFinished();
    BackgroundSaveIfNeed();
}

DmdbRDBManager::DmdbRDBManager(const std::string &file) {
    _is_rdb_loading = false;
    _rdb_file = file;
    _rdb_version = RDB_VERSION;
    _rdb_child_pid = -1;
    _my_parent_pid = -1;
    _rdb_child_start_ms = 0;
    _is_plan_to_bgsave_rdb = false;
    _rdb_child_for_client_fd = -1;
    _rdb_child_for_replica_fd = -1;
}

DmdbRDBManager::~DmdbRDBManager() {
    KillChildProcessIfAlive();
}

}