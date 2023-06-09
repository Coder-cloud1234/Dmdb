#include <algorithm>


#include "DmdbCommand.hpp"
#include "DmdbServerFriends.hpp"
#include "DmdbClientContact.hpp"
#include "DmdbDatabaseManager.hpp"
#include "DmdbUtil.hpp"
#include "DmdbRDBManager.hpp"
#include "DmdbClientManager.hpp"


namespace Dmdb {

DmdbCommand* DmdbCommand::GenerateCommandByName(const std::string &name) {
    std::string lowerName = name;
    std::transform(lowerName.begin(), lowerName.end(), lowerName.begin(), tolower);
    if(lowerName == "auth") {
        return new DmdbAuthCommand(name);
    } else if(lowerName == "multi") {
        return new DmdbMultiCommand(name);
    } else if(lowerName == "exec") {
        return new DmdbExecCommand(name);
    } else if(lowerName == "set") {
        return new DmdbSetCommand(name);
    } else if(lowerName == "get") {
        return new DmdbGetCommand(name);
    } else if(lowerName == "del") {
        return new DmdbDelCommand(name);
    } else if(lowerName == "exists") {
        return new DmdbExistsCommand(name);
    } else if(lowerName == "mset") {
        return new DmdbMSetCommand(name);
    } else if(lowerName == "expire") {
        return new DmdbExpireCommand(name);
    } else if(lowerName == "keys") {
        return new DmdbKeysCommand(name);
    } else if(lowerName == "dbsize") {
        return new DmdbDbsizeCommand(name);
    } else if(lowerName == "ping") {
        return new DmdbPingCommand(name);
    } else if(lowerName == "echo") {
        return new DmdbEchoCommand(name);
    } else if(lowerName == "save") {
        return new DmdbSaveCommand(name);
    } else if(lowerName == "type") {
        return new DmdbTypeCommand(name);
    } else if(lowerName == "pttl") {
        return new DmdbPTTLCommand(name);
    } else if(lowerName == "persist") {
        return new DmdbPersistCommand("persist");
    } else if(lowerName == "client") {
        return new DmdbClientCommand("client");
    }
    return nullptr;
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
    std::string msg;
    if(_parameters.size() < 1) {
        msg = "-ERR too few parameters\r\n";
        clientContact.AddReplyData2Client(msg);
        return false;
    } else if(components._server_client_manager->GetServerPassword() != _parameters[0]) {
        msg = "-ERR invalid password\r\n";
    } else {
        clientContact.SetChecked();
        msg = "+OK\r\n";
    }
    clientContact.AddReplyData2Client(msg);    
    return true;
}


DmdbMultiCommand::DmdbMultiCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbMultiCommand::~DmdbMultiCommand() {

}

bool DmdbMultiCommand::Execute(DmdbClientContact &clientContact) {
    if(clientContact.IsMultiState()) {
        std::string errMsg = "-ERR MULTI calls can not be nested\r\n";
        clientContact.AddReplyData2Client(errMsg);
        return false;
    }
    clientContact.SetMultiState(true);
    std::string msg = "+OK\r\n";
    clientContact.AddReplyData2Client(msg);
    return true;
}


DmdbExecCommand::DmdbExecCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbExecCommand::~DmdbExecCommand() {

}

bool DmdbExecCommand::Execute(DmdbClientContact &clientContact) {
    bool isExeOk = true;
    if(!clientContact.IsMultiState()) {
        std::string errMsg = "-ERR EXEC without MULTI\r\n";
        clientContact.AddReplyData2Client(errMsg);
        return false;
    }
    DmdbCommand* command = clientContact.PopCommandOfExec();
    while(command != nullptr) {
        if(!command->Execute(clientContact)) {
            isExeOk = false;
        }
        delete command;
        command = clientContact.PopCommandOfExec();
    }
    clientContact.SetMultiState(false);
    return isExeOk;
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
        clientContact.AddReplyData2Client(errMsg);
        return false;        
    }
    if(_parameters.size() > 5) {
        std::string errMsg = "-ERR too many parameters\r\n";
        clientContact.AddReplyData2Client(errMsg);
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
                clientContact.AddReplyData2Client(errMsg);
                return false;
            }
            expireTime = strtoull(_parameters[i + 1].c_str(), nullptr, 10);
            if (errno == ERANGE)
            {
                std::string errMsg = "-ERR invalid parameter\r\n";
                clientContact.AddReplyData2Client(errMsg);
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
            clientContact.AddReplyData2Client(errMsg);
            return false;
        }
    }

    DmdbValue* value = components._server_database_manager->GetValueByKey(_parameters[0]);
    std::vector<std::string> valArray;
    valArray.emplace_back(_parameters[1]);
    std::string msg;
    if((isNx&&value!=nullptr) || (isXx&&value==nullptr) ) {
        msg = "$-1\r\n";
        clientContact.AddReplyData2Client(msg);
        return true;
    }
    components._server_database_manager->SetKeyValuePair(_parameters[0], valArray,
                                                         DmdbValueType::STRING, expireTime);
    msg = "+OK\r\n";
    clientContact.AddReplyData2Client(msg);
    return true;    
}


DmdbGetCommand::DmdbGetCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbGetCommand::~DmdbGetCommand() {

}

bool DmdbGetCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    if(_parameters.size() != 1) {
        std::string errMsg = "-ERR too few or many parameters\r\n";
        clientContact.AddReplyData2Client(errMsg);
        return false;        
    }
    DmdbValue* value = components._server_database_manager->GetValueByKey(_parameters[0]);
    std::string msgResult;
    if(value != nullptr) {
        if(value->GetValueType() != DmdbValueType::STRING) {
            msgResult = "-ERR -WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
            clientContact.AddReplyData2Client(msgResult);
            return false;
        }
        msgResult = value->GetValueString() + "\r\n"; 
    } else {
        msgResult = "$-1\r\n";
    }
    clientContact.AddReplyData2Client(msgResult);
    return true;
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
    clientContact.AddReplyData2Client(msgResult);
    return true;
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
    clientContact.AddReplyData2Client(msgResult);
    return true;
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
        clientContact.AddReplyData2Client(msgResult);
        return false;
    }

    for(size_t i = 0; i < _parameters.size(); i+=2) {
        std::vector<std::string> valArr;
        valArr.emplace_back(_parameters[i+1]);
        components._server_database_manager->SetKeyValuePair(_parameters[i], valArr, DmdbValueType::STRING, 0);
    }
    msgResult = "+OK\r\n";
    clientContact.AddReplyData2Client(msgResult);
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
        msgResult = "-ERR wrong number of arguments for MSET";
        clientContact.AddReplyData2Client(msgResult);        
        return false;
    }
    uint64_t expireTime = strtoull(_parameters[1].c_str(), nullptr, 10);
    if(errno == ERANGE) {
        msgResult = "-ERR invalid parameter\r\n";
        clientContact.AddReplyData2Client(msgResult);
        return false;
    }
    components._server_database_manager->SetKeyExpireTime(_parameters[0], expireTime);
    msgResult = ":1\r\n";
    clientContact.AddReplyData2Client(msgResult);
    return true;
}

DmdbKeysCommand::DmdbKeysCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbKeysCommand::~DmdbKeysCommand() {

}

bool DmdbKeysCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    std::string msgResult;
    if(_parameters.size() > 1) {
        msgResult = "-ERR too many parameters\r\n";
        clientContact.AddReplyData2Client(msgResult);
        return false;        
    }
    std::vector<DmdbKey> keys;
    components._server_database_manager->GetKeysByPattern(_parameters[0], keys);
    msgResult = "*" + std::to_string(keys.size()) + "\r\n";
    for(size_t i = 0; i < keys.size(); ++i) {
        msgResult += "$" + std::to_string(keys[i].GetName().length()) + "\r\n";
        msgResult += keys[i].GetName() + "\r\n";
    }
    clientContact.AddReplyData2Client(msgResult);
    return true;
}

DmdbDbsizeCommand::DmdbDbsizeCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbDbsizeCommand::~DmdbDbsizeCommand() {

}

bool DmdbDbsizeCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    std::string msgResult;
    msgResult = ":" + std::to_string(components._server_database_manager->GetDatabaseSize()) + "\r\n";
    clientContact.AddReplyData2Client(msgResult);
    return true;
}


DmdbPingCommand::DmdbPingCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbPingCommand::~DmdbPingCommand() {

}

bool DmdbPingCommand::Execute(DmdbClientContact &clientContact) {
    std::string msgResult;
    msgResult = "+PONG\r\n";
    clientContact.AddReplyData2Client(msgResult);
    return true;
}


DmdbEchoCommand::DmdbEchoCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbEchoCommand::~DmdbEchoCommand() {

}

bool DmdbEchoCommand::Execute(DmdbClientContact &clientContact) {
    std::string msgResult = "";
    if(_parameters.size() > 0)
        msgResult = _parameters[0];
    msgResult += "\r\n";
    clientContact.AddReplyData2Client(msgResult);
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
    bool isSaveOk = components._server_rdb_manager->SaveDatabaseToDisk();
    if(!isSaveOk) {
        msg = "-ERR\r\n";
        clientContact.AddReplyData2Client(msg);
        return false;
    }
    msg = "+OK\r\n";
    clientContact.AddReplyData2Client(msg);        
    return true;
}

DmdbTypeCommand::DmdbTypeCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbTypeCommand::~DmdbTypeCommand() {

}

bool DmdbTypeCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    std::string msg = "";
    if(_parameters.size() != 1) {
        msg = "-ERR wrong number of arguments for Type\r\n";
        clientContact.AddReplyData2Client(msg);
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
    clientContact.AddReplyData2Client(msg);        
    return true;
}

DmdbPTTLCommand::DmdbPTTLCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbPTTLCommand::~DmdbPTTLCommand() {

}

bool DmdbPTTLCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    std::string msg = "";
    if(_parameters.size() != 1) {
        msg = "-ERR wrong number of arguments for Type";
        clientContact.AddReplyData2Client(msg);
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
    clientContact.AddReplyData2Client(msg);        
    return true;
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
        msg = "-ERR wrong number of arguments for Type";
        clientContact.AddReplyData2Client(msg);
        return false;        
    }
    bool isExist = components._server_database_manager->SetKeyExpireTime(_parameters[0], 0);
    msg = ":";
    if(!isExist) {
        msg = "0";
    } else {
        msg += "1";
    }
    msg += "\r\n";
    clientContact.AddReplyData2Client(msg);        
    return true;
}

DmdbClientCommand::DmdbClientCommand(std::string name) : DmdbCommand::DmdbCommand(name) {

}

DmdbClientCommand::~DmdbClientCommand() {

}

bool DmdbClientCommand::Execute(DmdbClientContact &clientContact) {
    DmdbCommandRequiredComponent components;
    GetDmdbCommandRequiredComponents(components);
    std::string msg = "";
    std::vector<std::string> helpStrVec = {
"getname                -- Return the name of the current connection.",
"kill <option> <value> [option value ...] -- Kill connections. Options are:",
"     addr <ip:port>                      -- Kill connection made from <ip:port>",
"     type (normal|master|replica|pubsub) -- Kill connections by type.",
"     skipme (yes|no)   -- Skip killing current connection (default: yes).",
"list [options ...]     -- Return information about client connections. Options:",
"     type (normal|master|replica|pubsub) -- Return clients of specified type.",
"pause <timeout>        -- Suspend all Redis clients for <timout> milliseconds.",
"reply (on|off|skip)    -- Control the replies sent to the current connection."};
    /*
    if(_parameters.size() < 1) {
        msg = FormatHelpMsgFromArray(helpStrVec);
        clientContact.AddReplyData2Client(msg);
        return true;
    }
    */
    if(_parameters.size() == 1 && _parameters[0] == "help") {
        msg = FormatHelpMsgFromArray(helpStrVec);
        clientContact.AddReplyData2Client(msg);
        return true;        
    }
    if(_parameters.size() == 1 && _parameters[0] == "getname") {
        msg = clientContact.GetClientName();
        msg = ("$" + std::to_string(msg.length()) + msg + "\r\n");
    } else if(_parameters.size() == 2 && _parameters[0] == "pause") {
        uint64_t pauseMs = strtoull(_parameters[1].c_str(), nullptr, 10);
        if(errno == ERANGE) {
            msg = "-ERR invalid parameter\r\n";
            clientContact.AddReplyData2Client(msg);
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
            } else {
                targetClient->SetStatus(static_cast<uint32_t>(ClientStatus::CLOSE_AFTER_REPLY));
                msg = "+OK\r\n";
            }
        }
    } else {
        msg = FormatHelpMsgFromArray(helpStrVec);
        clientContact.AddReplyData2Client(msg);
        return true;        
    }
    clientContact.AddReplyData2Client(msg);        
    return true;
}


}