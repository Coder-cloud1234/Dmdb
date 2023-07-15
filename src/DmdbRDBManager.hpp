#pragma once

#include <string>

namespace Dmdb {

class DmdbServerLogger;
class DmdbDatabaseManager;
class DmdbClientManager;
class DmdbReplicationManager;


struct DmdbRDBRequiredComponents {
    DmdbServerLogger* _server_logger;
    DmdbDatabaseManager* _database_manager;
    DmdbClientManager* _client_manager;
    DmdbReplicationManager* _repl_manager;
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

enum class SaveRetCode {
    SAVE_OK,
    OPEN_ERR,
    WRITE_ERR,
    SEND_ERR,
    NONE
};

class DmdbRDBManager {

public:
    bool IsRDBChildAlive();
    bool IsMyselfRDBChild();
    bool RemoveRdbChildTmpFile();
    bool KillChildProcessIfAlive();
    bool SaveDatabaseToDisk();
    bool LoadDatabase(int fd);
    bool BackgroundSave();
    SaveRetCode SaveData(int fd, bool isBgSave);
    std::string GetRDBFile();
    void CheckRdbChildFinished();
    void HandleAfterChildExit(bool isKilledBySignal); 
    void WriteDataToPipeIfNeed(const std::string& data, bool isBgSave);
    bool ReceiveRetCodeFromPipe(SaveRetCode &retCode, bool isBgSave);
    void BackgroundSaveIfNeed();
    void SetBackgroundSavePlan(int clientFd, int replicaFd);
    void ClearBackgroundSavePlan(); 
    void FeedbackToClientOfRdbChild(DmdbRDBRequiredComponents &components, const std::string& feedback);
    void RdbCheckAndFinishJob();
    static DmdbRDBManager* GetUniqueRDBManagerInstance(const std::string &file);
    ~DmdbRDBManager();
private:
    size_t GenerateRDBHeader(uint8_t* buf, size_t bufLen, DmdbRDBRequiredComponents &components, bool isForReplica);
    LoadRetCode GetOnePair(char* buf, size_t bufLen, DmdbRDBRequiredComponents &components, size_t &pos, 
                           FieldOfSavedPair &field, bool isLast);
    bool IsErrorOccurs(const char* replBuf);
    DmdbRDBManager(const std::string &file);
    bool _is_rdb_loading;
    bool _is_plan_to_bgsave_rdb;
    std::string _rdb_file;
    uint8_t _rdb_version;
    uint64_t _rdb_child_start_ms;
    pid_t _rdb_child_pid;
    pid_t _my_parent_pid;
    int _rdb_child_for_replica_fd; /* >0 means rdb child created for replica, otherwise for bgsave */
    int _rdb_child_for_client_fd; /* Client fd that rdb child process is created for */
    int _pipe_with_child[2];
    static DmdbRDBManager* _instance;
};


};