#include <algorithm>


#include "DmdbCommand.hpp"
#include "DmdbServerFriends.hpp"
#include "DmdbClientContact.hpp"
#include "DmdbDatabaseManager.hpp"
#include "DmdbUtil.hpp"
#include "DmdbRDBManager.hpp"
#include "DmdbClientManager.hpp"
#include "DmdbReplicationManager.hpp"


namespace Dmdb {

DmdbCommand* DmdbCommand::GenerateCommandByName(const std::string &name) {
    std::string lowerName = name;
    std::transform(lowerName.begin(), lowerName.end(), lowerName.begin(), tolower);
    if(lowerName == "auth") {
        return new DmdbAuthCommand(lowerName);
    } else if(lowerName == "multi") {
        return new DmdbMultiCommand(lowerName);
    } else if(lowerName == "exec") {
        return new DmdbExecCommand(lowerName);
    } else if(lowerName == "set") {
        return new DmdbSetCommand(lowerName);
    } else if(lowerName == "get") {
        return new DmdbGetCommand(lowerName);
    } else if(lowerName == "del") {
        return new DmdbDelCommand(lowerName);
    } else if(lowerName == "exists") {
        return new DmdbExistsCommand(lowerName);
    } else if(lowerName == "mset") {
        return new DmdbMSetCommand(lowerName);
    } else if(lowerName == "expire") {
        return new DmdbExpireCommand(lowerName);
    } else if(lowerName == "keys") {
        return new DmdbKeysCommand(lowerName);
    } else if(lowerName == "dbsize") {
        return new DmdbDbsizeCommand(lowerName);
    } else if(lowerName == "ping") {
        return new DmdbPingCommand(lowerName);
    } else if(lowerName == "echo") {
        return new DmdbEchoCommand(lowerName);
    } else if(lowerName == "save") {
        return new DmdbSaveCommand(lowerName);
    } else if(lowerName == "type") {
        return new DmdbTypeCommand(lowerName);
    } else if(lowerName == "pttl") {
        return new DmdbPTTLCommand(lowerName);
    } else if(lowerName == "persist") {
        return new DmdbPersistCommand(lowerName);
    } else if(lowerName == "client") {
        return new DmdbClientCommand(lowerName);
    } else if(lowerName == "sync") {
        return new DmdbSyncCommand(lowerName);
    } else if(lowerName == "replconf") {
        return new DmdbReplconfCommand(lowerName);
    } else if(lowerName == "bgsave") {
        return new DmdbBgSaveCommand(lowerName);
    } else if(lowerName == "shutdown") {
        return new DmdbShutdownCommand(lowerName);
    } else if(lowerName == "role") {
        return new DmdbRoleCommand(lowerName);
    } else if(lowerName == "wait") {
        return new DmdbWaitCommand(lowerName);
    }
    return nullptr;
}

bool DmdbCommand::IsWCommand(const std::string &commandName) {
    std::string lowerName = commandName;
    std::transform(lowerName.begin(), lowerName.end(), lowerName.begin(), tolower);
    if(lowerName == "set" || lowerName == "del" || lowerName == "mset" || lowerName == "expire" ||
       lowerName == "persist") {
        return true;
    }
    return false;
}

void DmdbCommand::AddExecuteRetToClientIfNeed(const std::string &msg, DmdbClientContact &clientContact, bool isForce = false) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    if(isForce || !components._repl_manager->IsMyMaster(clientContact.GetClientName())) {
        clientContact.AddReplyData2Client(msg);
    }
}

std::string DmdbCommand::GetName() {
    return _command_name;
}

void DmdbCommand::AppendCommandPara(const std::string &para) {
    _parameters.emplace_back(para);
}

std::string DmdbCommand::FormatHelpMsgFromArray(const std::vector<std::string> &vec) {
    std::string commandName = _command_name;
    std::transform(commandName.begin(), commandName.end(), commandName.begin(), toupper);
    std::string ret = ("+" + commandName + " <subcommand> arg arg ... arg. Subcommands are:\r\n");
    for(size_t i = 0; i < vec.size(); ++i) {
        ret += ("+"+vec[i]+"\r\n");
    }
    ret = "*" + std::to_string(vec.size()+1) + "\r\n" + ret;
    return ret;
}

DmdbCommand::DmdbCommand(std::string name) : _command_name(name) {

}


DmdbCommand::~DmdbCommand() {

}


DmdbAuthCommand::DmdbAuthCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbAuthCommand::~DmdbAuthCommand() {

}

bool DmdbAuthCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    if(components._repl_manager->IsMyMaster(clientContact.GetClientName())) {
        return true;
    }
    std::string msg;
    if(_parameters.size() < 1) {
        msg = "-ERR too few parameters\r\n";
        AddExecuteRetToClientIfNeed(msg, clientContact);
        return false;
    } else if(components._server_client_manager->GetServerPassword() != _parameters[0]) {
        msg = "-ERR invalid password\r\n";           
        AddExecuteRetToClientIfNeed(msg, clientContact);
        return false;
    } else {
        clientContact.SetChecked();
        msg = "+OK\r\n";
        AddExecuteRetToClientIfNeed(msg, clientContact);        
        return true;
    }   
}


DmdbMultiCommand::DmdbMultiCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbMultiCommand::~DmdbMultiCommand() {

}

bool DmdbMultiCommand::Execute(DmdbClientContact &clientContact) {
    if(clientContact.IsMultiState()) {
        std::string errMsg = "-ERR MULTI calls can not be nested\r\n";
        AddExecuteRetToClientIfNeed(errMsg, clientContact);
        return false;
    }
    clientContact.SetMultiState(true);
    std::string msg = "+OK\r\n";
    AddExecuteRetToClientIfNeed(msg, clientContact);
    return true;
}


DmdbExecCommand::DmdbExecCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbExecCommand::~DmdbExecCommand() {

}

bool DmdbExecCommand::Execute(DmdbClientContact &clientContact) {
    if(!clientContact.IsMultiState()) {
        std::string errMsg = "-ERR EXEC without MULTI\r\n";
        AddExecuteRetToClientIfNeed(errMsg, clientContact);
        return false;
    }
    std::string msg = "*" + std::to_string(clientContact.GetMultiQueueSize()) + "\r\n";
    AddExecuteRetToClientIfNeed(msg, clientContact);
    DmdbCommand* command = clientContact.PopCommandOfExec();
    while(command != nullptr) {
        command->Execute(clientContact);
        delete command;
        command = clientContact.PopCommandOfExec();
    }
    clientContact.SetMultiState(false);
    return true;
}


DmdbSetCommand::DmdbSetCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbSetCommand::~DmdbSetCommand() {

}

bool DmdbSetCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    if(_parameters.size() < 2) {
        std::string errMsg = "-ERR too few parameters\r\n";
        AddExecuteRetToClientIfNeed(errMsg, clientContact);
        return false;        
    }
    if(_parameters.size() > 5) {
        std::string errMsg = "-ERR too many parameters\r\n";
        AddExecuteRetToClientIfNeed(errMsg, clientContact);
        return false;
    }
    uint64_t expireTime = 0;
    bool isNx = false;
    bool isXx = false;

    for (size_t i = 2; i < _parameters.size(); ++i)
    {
        std::string upperPara = _parameters[i];
        std::transform(upperPara.begin(), upperPara.end(), upperPara.begin(), toupper);
        if (upperPara == "EX" || upperPara == "PX")
        {
            if (i + 1 == _parameters.size())
            {
                std::string errMsg = "-ERR too few parameters\r\n";
                AddExecuteRetToClientIfNeed(errMsg, clientContact);
                return false;
            }
            expireTime = strtoull(_parameters[i + 1].c_str(), nullptr, 10);
            if (errno == ERANGE)
            {
                std::string errMsg = "-ERR invalid parameter\r\n";
                AddExecuteRetToClientIfNeed(errMsg, clientContact);
                return false;
            }
            if (upperPara == "EX")
                expireTime *= 1000;
            i++;
            expireTime += DmdbUtil::GetCurrentMs();
        }
        else if (upperPara == "NX")
        {
            isNx = true;
        }
        else if (upperPara == "XX")
        {
            isXx = true;
        }
        else
        {
            std::string errMsg = "-ERR syntax error\r\n";
            AddExecuteRetToClientIfNeed(errMsg, clientContact);
            return false;
        }
    }

    DmdbValue* value = components._server_database_manager->GetValueByKey(_parameters[0]);
    std::vector<std::string> valArray;
    valArray.emplace_back(_parameters[1]);
    std::string msg;

    if((isNx&&value!=nullptr) || (isXx&&value==nullptr) ) {
        msg = "$-1\r\n";
        AddExecuteRetToClientIfNeed(msg, clientContact);
        return false;
    }

    components._server_database_manager->SetKeyValuePair(_parameters[0], valArray,
                                                         DmdbValueType::STRING, expireTime);

    msg = "+OK\r\n";
    AddExecuteRetToClientIfNeed(msg, clientContact);
    return true;    
}


DmdbGetCommand::DmdbGetCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbGetCommand::~DmdbGetCommand() {

}

bool DmdbGetCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    if(components._repl_manager->IsMyMaster(clientContact.GetClientName())) {
        return true;
    }
    if(_parameters.size() != 1) {
        std::string errMsg = "-ERR too few or many parameters\r\n";
        AddExecuteRetToClientIfNeed(errMsg, clientContact);
        return false;        
    }
    DmdbValue* value = components._server_database_manager->GetValueByKey(_parameters[0]);
    std::string msgResult;
    if(value != nullptr) {
        if(value->GetValueType() != DmdbValueType::STRING) {
            msgResult = "-ERR -WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
            AddExecuteRetToClientIfNeed(msgResult, clientContact);
            return false;
        }
        msgResult = "$" + std::to_string(value->GetValueString().length()) + "\r\n" + value->GetValueString() + "\r\n";
        AddExecuteRetToClientIfNeed(msgResult, clientContact);
        return true; 
    } else {
        msgResult = "$-1\r\n";
        AddExecuteRetToClientIfNeed(msgResult, clientContact);
        return false;
    }
    
}


DmdbDelCommand::DmdbDelCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbDelCommand::~DmdbDelCommand() {

}

bool DmdbDelCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    std::string msgResult;
    size_t delNum = 0;
    for(size_t i = 0; i < _parameters.size(); ++i) {
        bool deleted = components._server_database_manager->DelKey(_parameters[i]);
        if(deleted) {
            delNum++;
        }
    }
    msgResult = ":" + std::to_string(delNum) + "\r\n";
    AddExecuteRetToClientIfNeed(msgResult, clientContact);
    if(delNum > 0)
        return true;
    return false;
}

DmdbExistsCommand::DmdbExistsCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbExistsCommand::~DmdbExistsCommand() {

}

bool DmdbExistsCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    std::string msgResult;
    size_t existsNum = 0;
    for(size_t i = 0; i < _parameters.size(); ++i) {
        DmdbValue* value = components._server_database_manager->GetValueByKey(_parameters[i]);
        if(value != nullptr) {
            existsNum++;
        }
    }
    msgResult = ":" + std::to_string(existsNum) + "\r\n";
    AddExecuteRetToClientIfNeed(msgResult, clientContact);
    if(existsNum > 0)
        return true;
    return false;
}

DmdbMSetCommand::DmdbMSetCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbMSetCommand::~DmdbMSetCommand() {

}

bool DmdbMSetCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    std::string msgResult;
    if(_parameters.size()%2 != 0) {
        msgResult = "-ERR wrong number of arguments for MSET\r\n";
        AddExecuteRetToClientIfNeed(msgResult, clientContact);
        return false;
    }
    std::vector<std::string> valArr;
    for(size_t i = 0; i < _parameters.size(); i+=2) {
        valArr.emplace_back(_parameters[i+1]);
        components._server_database_manager->SetKeyValuePair(_parameters[i], valArr, DmdbValueType::STRING, 0);
        valArr.clear();
    }
    msgResult = "+OK\r\n";
    AddExecuteRetToClientIfNeed(msgResult, clientContact);
    return true;
}


DmdbExpireCommand::DmdbExpireCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbExpireCommand::~DmdbExpireCommand() {

}

bool DmdbExpireCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    std::string msgResult;
    if(_parameters.size() != 2) {
        msgResult = "-ERR wrong number of arguments for Expire\r\n";
        AddExecuteRetToClientIfNeed(msgResult, clientContact);        
        return false;
    }
    uint64_t expireTime = strtoull(_parameters[1].c_str(), nullptr, 10);
    if(errno == ERANGE) {
        msgResult = "-ERR invalid parameter\r\n";
        AddExecuteRetToClientIfNeed(msgResult, clientContact);
        return false;
    }
    expireTime *= 1000;
    expireTime += DmdbUtil::GetCurrentMs();
    bool isOk = components._server_database_manager->SetKeyExpireTime(_parameters[0], expireTime);
    if(isOk)
        msgResult = ":1\r\n";
    else
        msgResult = ":0\r\n";
    AddExecuteRetToClientIfNeed(msgResult, clientContact);
    return isOk;
}

DmdbKeysCommand::DmdbKeysCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbKeysCommand::~DmdbKeysCommand() {

}

bool DmdbKeysCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    if(components._repl_manager->IsMyMaster(clientContact.GetClientName())) {
        return true;
    }
    std::string msgResult;
    if(_parameters.size() > 1) {
        msgResult = "-ERR too many parameters\r\n";
        AddExecuteRetToClientIfNeed(msgResult, clientContact);
        return false;        
    }
    std::vector<DmdbKey> keys;
    components._server_database_manager->GetKeysByPattern(_parameters[0], keys);
    msgResult = "*" + std::to_string(keys.size()) + "\r\n";
    for(size_t i = 0; i < keys.size(); ++i) {
        msgResult += "$" + std::to_string(keys[i].GetName().length()) + "\r\n";
        msgResult += keys[i].GetName() + "\r\n";
    }
    AddExecuteRetToClientIfNeed(msgResult, clientContact);
    if(keys.size() > 0)
        return true;
    return false;
}

DmdbDbsizeCommand::DmdbDbsizeCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbDbsizeCommand::~DmdbDbsizeCommand() {

}

bool DmdbDbsizeCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    if(components._repl_manager->IsMyMaster(clientContact.GetClientName())) {
        return true;
    }
    std::string msgResult;
    msgResult = ":" + std::to_string(components._server_database_manager->GetDatabaseSize()) + "\r\n";
    AddExecuteRetToClientIfNeed(msgResult, clientContact);
    return true;
}


DmdbPingCommand::DmdbPingCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbPingCommand::~DmdbPingCommand() {

}

bool DmdbPingCommand::Execute(DmdbClientContact &clientContact) {
    /* We don't process ping command from master currently, but may process in the future */
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    if(components._repl_manager->IsMyMaster(clientContact.GetClientName())) {
        return true;
    }    
    std::string msgResult;
    msgResult = "+PONG\r\n";
    AddExecuteRetToClientIfNeed(msgResult, clientContact);
    return true;
}


DmdbEchoCommand::DmdbEchoCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbEchoCommand::~DmdbEchoCommand() {

}

bool DmdbEchoCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    if(components._repl_manager->IsMyMaster(clientContact.GetClientName())) {
        return true;
    }
    std::string msgResult = "$";
    if(_parameters.size() > 0) {
        msgResult += std::to_string(_parameters[0].length()) + "\r\n" + _parameters[0] + "\r\n";
    } else {
        msgResult += "0\r\n\r\n";
    }
    AddExecuteRetToClientIfNeed(msgResult, clientContact);
    return true;
}

DmdbSaveCommand::DmdbSaveCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbSaveCommand::~DmdbSaveCommand() {

}

bool DmdbSaveCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    std::string msg = "";
    bool isSaveOk = components._server_rdb_manager->SaveData(-1, false) == SaveRetCode::SAVE_OK;
    if(!isSaveOk) {
        msg = "-ERR Failed to save rdb data into disk\r\n";
        AddExecuteRetToClientIfNeed(msg, clientContact);
        return false;
    }
    msg = "+OK\r\n";
    AddExecuteRetToClientIfNeed(msg, clientContact);        
    return true;
}

DmdbTypeCommand::DmdbTypeCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbTypeCommand::~DmdbTypeCommand() {

}

bool DmdbTypeCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    if(components._repl_manager->IsMyMaster(clientContact.GetClientName())) {
        return true;
    }
    std::string msg = "";
    if(_parameters.size() != 1) {
        msg = "-ERR wrong number of arguments for Type\r\n";
        AddExecuteRetToClientIfNeed(msg, clientContact);
        return false;        
    }
    DmdbValue* value = components._server_database_manager->GetValueByKey(_parameters[0]);
    msg = "+";
    if(value != nullptr) {
        msg += value->GetValueTypeString();
    } else {
        msg += "none";
    }
    msg += "\r\n";
    AddExecuteRetToClientIfNeed(msg, clientContact);        
    return value != nullptr;
}

DmdbPTTLCommand::DmdbPTTLCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbPTTLCommand::~DmdbPTTLCommand() {

}

bool DmdbPTTLCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    if(components._repl_manager->IsMyMaster(clientContact.GetClientName())) {
        return true;
    }
    std::string msg = "";
    if(_parameters.size() != 1) {
        msg = "-ERR wrong number of arguments for PTTL\r\n";
        AddExecuteRetToClientIfNeed(msg, clientContact);
        return false;        
    }
    DmdbKey key("");
    bool isExist = components._server_database_manager->GetKeyByName(_parameters[0], key);
    msg = ":";
    if(!isExist) {
        msg = "-2";
    } else {
        uint64_t expireTime = key.GetExpireTime();
        if(expireTime == 0) {
            msg += "-1";
        } else {
            msg += std::to_string(expireTime);
        }
    }
    msg += "\r\n";
    AddExecuteRetToClientIfNeed(msg, clientContact);        
    return isExist;
}

DmdbPersistCommand::DmdbPersistCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbPersistCommand::~DmdbPersistCommand() {

}

bool DmdbPersistCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    std::string msg = "";
    if(_parameters.size() != 1) {
        msg = "-ERR Wrong number of arguments for Type\r\n";
        AddExecuteRetToClientIfNeed(msg, clientContact);
        return false;        
    }
    bool isExist = components._server_database_manager->SetKeyExpireTime(_parameters[0], 0);
    msg = ":";
    if(!isExist) {
        msg += "0";
    } else {
        msg += "1";
    }
    msg += "\r\n";
    AddExecuteRetToClientIfNeed(msg, clientContact);        
    return isExist;
}

DmdbClientCommand::DmdbClientCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbClientCommand::~DmdbClientCommand() {

}

bool DmdbClientCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    if(components._repl_manager->IsMyMaster(clientContact.GetClientName())) {
        return true;
    }
    std::string msg = "";
    std::vector<std::string> helpStrVec = {
"getname                -- Return the name of the current connection.",
"kill <option> <value> [option value ...] -- Kill connections. Options are:",
"     addr <ip:port>                      -- Kill connection made from <ip:port>",
"pause <timeout>        -- Suspend all Redis clients for <timout> milliseconds."};

    if(_parameters.size() == 1 && _parameters[0] == "help") {
        msg = FormatHelpMsgFromArray(helpStrVec);
        AddExecuteRetToClientIfNeed(msg, clientContact);
        return true;        
    }
    if(_parameters.size() == 1 && _parameters[0] == "getname") {
        msg = clientContact.GetClientName();
        msg = ("$" + std::to_string(msg.length()) + "\r\n" + msg + "\r\n");
    } else if(_parameters.size() == 2 && _parameters[0] == "pause") {
        uint64_t pauseMs = strtoull(_parameters[1].c_str(), nullptr, 10);
        if(errno == ERANGE) {
            msg = "-ERR invalid parameter\r\n";
            AddExecuteRetToClientIfNeed(msg, clientContact);
            return false;
        }
        components._server_client_manager->PauseClients(pauseMs);
        msg = "+OK\r\n";
    } else if(_parameters.size() >= 3 && _parameters[0] == "kill") {
        /* For this command, currently we only support option "addr" */
        if(_parameters[1] == "addr") {
            /* Because we use unordered_map ot traverse all the DmdbClientContacts, so we can't disconnect
             * DmdbClientContact directly for avoiding iterator failure */
            DmdbClientContact *targetClient = components._server_client_manager->GetClientContactByName(_parameters[2]);
            if(targetClient == nullptr) {
                msg = "-ERR No such client\r\n";
                AddExecuteRetToClientIfNeed(msg, clientContact);
                return false;
            } else {
                targetClient->SetStatus(static_cast<uint32_t>(ClientStatus::CLOSE_AFTER_REPLY));
                msg = "+OK\r\n";
            }
        }
    } else {
        msg = FormatHelpMsgFromArray(helpStrVec);
        AddExecuteRetToClientIfNeed(msg, clientContact);
        return false;        
    }
    AddExecuteRetToClientIfNeed(msg, clientContact);        
    return true;
}

DmdbSyncCommand::DmdbSyncCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbSyncCommand::~DmdbSyncCommand() {

}

/* This command is only processed for replica by master */
bool DmdbSyncCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    std::string replyToSlaveMsg;
    if(!components._is_myself_master) {
        replyToSlaveMsg = "-ERR You can't sync with a replica\r\n";
        AddExecuteRetToClientIfNeed(replyToSlaveMsg, clientContact);
        return false;
    }

    if(!clientContact.IsChecked()) {
        replyToSlaveMsg = "-ERR You must authenticate before sync\r\n";
        AddExecuteRetToClientIfNeed(replyToSlaveMsg, clientContact);
        return false;
    }

    if(components._server_rdb_manager->IsRDBChildAlive()) {
        replyToSlaveMsg = "-ERR RDB child process is running, please try later\r\n";
        AddExecuteRetToClientIfNeed(replyToSlaveMsg, clientContact);
        return false;        
    }
    long long myCurReplOffset = components._repl_manager->GetReplOffset();
    AddExecuteRetToClientIfNeed("+FULLRESYNC " + std::to_string(myCurReplOffset) + "\r\n", clientContact);
    components._repl_manager->FullSyncDataToReplica(&clientContact);
    return true;
}

DmdbReplconfCommand::DmdbReplconfCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbReplconfCommand::~DmdbReplconfCommand() {

}

/* This command is only processed for replica or master client */
bool DmdbReplconfCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    std::string msg = "";
    if(!components._is_myself_master) {
        if(_parameters.size() != 1) {
            msg = "-ERR Wrong number of arguments when I am a replica\r\n";
            AddExecuteRetToClientIfNeed(msg, clientContact);
            return false;
        }
        if(_parameters[0] != "getack") {
            msg = "-ERR Invalid parameter\r\n";
            AddExecuteRetToClientIfNeed(msg, clientContact);
            return false;            
        }
        if(!components._repl_manager->IsMyMaster(clientContact.GetClientName())) {
            msg = "-ERR Replconf can only be executed by my master when I am a replica\r\n";
            AddExecuteRetToClientIfNeed(msg, clientContact);
            return false;
        }
        components._repl_manager->ReportToMasterMyReplayOkSize();
    } else {
        if(_parameters.size() != 2) {
            msg = "-ERR Wrong number of arguments when I am a master\r\n";
            AddExecuteRetToClientIfNeed(msg, clientContact);
            return false;
        }
        if(_parameters[0] != "ack") {
            msg = "-ERR Invalid parameter\r\n";
            AddExecuteRetToClientIfNeed(msg, clientContact);
            return false;            
        }        
        if(!components._repl_manager->IsOneOfMySlaves(&clientContact)) {
            msg = "-ERR Replconf can only be executed by my replicas when I am a master\r\n";
            AddExecuteRetToClientIfNeed(msg, clientContact);
            return false;
        }
        long long replicaReplayOkSize = strtoll(_parameters[1].c_str(), nullptr, 10);
        if(errno == ERANGE) {
            msg = "-ERR Invalid parameter\r\n";
            AddExecuteRetToClientIfNeed(msg, clientContact);
            return false;
        }
        components._repl_manager->SetReplicaReplayOkSize(&clientContact, replicaReplayOkSize);
    }
    return true;
}


DmdbBgSaveCommand::DmdbBgSaveCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbBgSaveCommand::~DmdbBgSaveCommand() {

}

bool DmdbBgSaveCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    std::string msg = "";
    bool isRdbChildRunning = components._server_rdb_manager->IsRDBChildAlive();
    if(isRdbChildRunning) {
        msg = "-ERR RDB background saving is running, we can't save rdb data in the background now\r\n";
        AddExecuteRetToClientIfNeed(msg, clientContact);
        return false;
    }
    components._server_rdb_manager->SetBackgroundSavePlan(clientContact.GetClientSocket(), -1);
    msg = "+Background saving started\r\n";
    AddExecuteRetToClientIfNeed(msg, clientContact);        
    return true;
}

DmdbShutdownCommand::DmdbShutdownCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbShutdownCommand::~DmdbShutdownCommand() {

}

bool DmdbShutdownCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    *components._is_plan_to_shutdown = true;        
    return true;
}

DmdbRoleCommand::DmdbRoleCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbRoleCommand::~DmdbRoleCommand() {

}

bool DmdbRoleCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    std::string msg = "";
    if(components._is_myself_master){
        msg = "*3\r\n";
        msg += "$6\r\n";
        msg += "master\r\n";
        msg += ":" + std::to_string(components._repl_manager->GetReplOffset()) + "\r\n";
        msg += components._repl_manager->GetMultiBulkOfReplicasOrMaster();
    } else {
        msg += components._repl_manager->GetMultiBulkOfReplicasOrMaster();
    }
    AddExecuteRetToClientIfNeed(msg, clientContact);       
    return true;
}

DmdbWaitCommand::DmdbWaitCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbWaitCommand::~DmdbWaitCommand() {

}

bool DmdbWaitCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    std::string msg;
    if(!components._is_myself_master) {
        msg = "-ERR WAIT cannot be used with replica instances.\r\n";
        AddExecuteRetToClientIfNeed(msg, clientContact);
        return false;
    }
    if(_parameters.size() != 2) {
        msg = "-ERR Wrong number of arguments.\r\n";
        AddExecuteRetToClientIfNeed(msg, clientContact);
        return false;
    }
    uint64_t numOfReplicasToWait = strtoul(_parameters[0].c_str(), nullptr, 10);
    if(errno == ERANGE) {
        msg = "-ERR Invalid parameter.\r\n";
        AddExecuteRetToClientIfNeed(msg, clientContact);
        return false;        
    }
    if(numOfReplicasToWait > components._repl_manager->GetReplicaCount() ) {
        msg = "-ERR Number is too big.\r\n";
        AddExecuteRetToClientIfNeed(msg, clientContact);
        return false;        
    }
    uint64_t numOfMsToWait = strtoull(_parameters[1].c_str(), nullptr, 10);
    if(errno == ERANGE) {
        msg = "-ERR Invalid parameter.\r\n";
        AddExecuteRetToClientIfNeed(msg, clientContact);
        return false;        
    }
    if(numOfReplicasToWait <= components._repl_manager->CountNumOfReplicasByOffset() || clientContact.IsMultiState()) {
        msg = ":" + std::to_string(components._repl_manager->CountNumOfReplicasByOffset()) + "\r\n";
        AddExecuteRetToClientIfNeed(msg, clientContact);
        return true;
    }
    components._repl_manager->WaitForNReplicasAck(&clientContact, numOfReplicasToWait, numOfMsToWait);
    return true;
}


}