#pragma once

#include <string>

namespace Dmdb {

class DmdbServerLogger;
class DmdbDatabaseManager;



struct DmdbRDBRequiredComponents {
    DmdbServerLogger* _server_logger;
    DmdbDatabaseManager* _database_manager;
    bool* _is_preamble;
    uint8_t _server_version;
};


enum class LoadRetCode {
    OK,
    END,
    NOT_ENOUGH,
    INVALID_LENGTH,
    CORRUPTION
};

enum class FieldOfSavedPair {
    EXPIRE_TIME,
    KEY_LEN,
    KEY_NAME,
    VAL_TYPE,
    VAL_LEN,
    VAL
};

class DmdbRDBManager {

public:
    bool SaveDatabaseToDisk();
    bool LoadDatabaseFromDisk();
    std::string GetRDBFile();
    static DmdbRDBManager* GetUniqueRDBManagerInstance(const std::string &file);
    ~DmdbRDBManager();
private:
    void GenerateRDBHeader(std::fstream &fs, DmdbRDBRequiredComponents &components, uint64_t &crcCode);
    LoadRetCode GetOnePair(char* buf, size_t bufLen, DmdbRDBRequiredComponents &components, size_t &pos, 
                           FieldOfSavedPair &field, bool isLast);
    DmdbRDBManager(const std::string &file);
    bool _is_rdb_loading;
    std::string _rdb_file;
    uint8_t _rdb_version;
    static DmdbRDBManager* _instance;
};


};