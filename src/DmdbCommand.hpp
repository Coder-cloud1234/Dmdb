
#pragma once

#include <string>
#include <vector>


namespace Dmdb {

class DmdbDatabaseManager;
class DmdbClientContact;
class DmdbRDBManager;
class DmdbClientManager;

struct DmdbCommandRequiredComponent {
    DmdbDatabaseManager* _server_database_manager;
    DmdbRDBManager* _server_rdb_manager;
    DmdbClientManager* _server_client_manager; 
};

class DmdbCommand {
public:
    static DmdbCommand* GenerateCommandByName(const std::string &name);
    std::string GetName();
    void AppendCommandPara(const std::string &para);
    virtual bool Execute(DmdbClientContact &clientContact) = 0;
    virtual ~DmdbCommand();
protected:
    DmdbCommand(std::string name);
    std::string FormatHelpMsgFromArray(const std::vector<std::string> &vec);
    std::string _command_name;
    std::vector<std::string> _parameters;
    
};

class DmdbAuthCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbAuthCommand(std::string name);
    ~DmdbAuthCommand();
};

class DmdbMultiCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbMultiCommand(std::string name);
    ~DmdbMultiCommand();
};

class DmdbExecCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbExecCommand(std::string name);
    ~DmdbExecCommand();
};

class DmdbSetCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbSetCommand(std::string name);
    ~DmdbSetCommand();
};

class DmdbGetCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbGetCommand(std::string name);
    ~DmdbGetCommand();
};

class DmdbDelCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbDelCommand(std::string name);
    ~DmdbDelCommand();
};

class DmdbExistsCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbExistsCommand(std::string name);
    ~DmdbExistsCommand();
};

class DmdbMSetCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbMSetCommand(std::string name);
    ~DmdbMSetCommand();
};

class DmdbExpireCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbExpireCommand(std::string name);
    ~DmdbExpireCommand();
};

class DmdbKeysCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbKeysCommand(std::string name);
    ~DmdbKeysCommand();
};

class DmdbDbsizeCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbDbsizeCommand(std::string name);
    ~DmdbDbsizeCommand();
};

class DmdbPingCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbPingCommand(std::string name);
    ~DmdbPingCommand();
};

class DmdbEchoCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbEchoCommand(std::string name);
    ~DmdbEchoCommand();
};

class DmdbSaveCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbSaveCommand(std::string name);
    ~DmdbSaveCommand();
};

class DmdbTypeCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbTypeCommand(std::string name);
    ~DmdbTypeCommand();
};

class DmdbPTTLCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbPTTLCommand(std::string name);
    ~DmdbPTTLCommand();
};

class DmdbPersistCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbPersistCommand(std::string name);
    ~DmdbPersistCommand();
};

class DmdbClientCommand : public DmdbCommand {
public:
    virtual bool Execute(DmdbClientContact &clientContact);
    DmdbClientCommand(std::string name);
    ~DmdbClientCommand();
};

}