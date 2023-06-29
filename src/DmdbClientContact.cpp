#include "DmdbClientContact.hpp"
#include "DmdbServerFriends.hpp"
#include "DmdbClientManager.hpp"
#include "DmdbServerLogger.hpp"
#include "DmdbReplicationManager.hpp"
#include "DmdbCommand.hpp"


namespace Dmdb {

const size_t PROTO_MULTI_MAX_SIZE = 64*1024;

DmdbClientContact::DmdbClientContact(int fd, const std::string &ip, int port) {
    _client_socket = fd;
    _client_ip = ip;
    _client_port = port;
    _client_name = ip+":"+std::to_string(port);
    _client_status = 0;
    _process_pos_of_input_buf = 0;
    _is_chekced = false;
    _is_multi_state = false;
}

DmdbClientContact::~DmdbClientContact() {
    // close(_client_socket);
    // delete _current_command;
}

std::string DmdbClientContact::GetClientName() {
    return _client_name;
}


int DmdbClientContact::GetClientSocket() {
    return _client_socket;
}


void DmdbClientContact::AddReplyData2Client(const std::string &replyData) {
    _client_output_buffer.append(replyData);
}

size_t DmdbClientContact::GetInputBufLength() {
    return _client_input_buffer.length();
}

size_t DmdbClientContact::GetOutputBufLength() {
    return _client_output_buffer.length();
} 

const char* DmdbClientContact::GetOutputBuf() {
    return _client_output_buffer.c_str();
} 

void DmdbClientContact::AppendDataToInputBuf(char* data) {
    _client_input_buffer += data;
}

void DmdbClientContact::ClearRepliedData(size_t repliedLen) {
    if(repliedLen == _client_output_buffer.length()) {
        _client_output_buffer.clear();
        return;
    }
    _client_output_buffer = _client_output_buffer.substr(repliedLen, _client_output_buffer.length()-repliedLen);
}

void DmdbClientContact::ClearProcessedData() {
    if(_process_pos_of_input_buf == _client_input_buffer.length()) {
        _client_input_buffer.clear();
    } else {
        _client_input_buffer = _client_input_buffer.substr(_process_pos_of_input_buf, _client_input_buffer.length()-_process_pos_of_input_buf);
    }
    _process_pos_of_input_buf = 0;
}

bool DmdbClientContact::ProcessOneMultiProtocolRequest(size_t &startPos) {
    DmdbClientContactRequiredComponent components;
    GetDmdbClientContactRequiredComponent(components);
    if(_client_input_buffer[startPos] != '*') {
        std::string errMsg = "-ERR Protocol error: invalid start character\r\n";
        AddReplyData2Client(errMsg);
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE, 
                                                    "Client %s sent a request with invalid protocol start character",
                                                    _client_name);
        _client_status |= static_cast<uint32_t>(ClientStatus::CLOSE_AFTER_REPLY);
        return false;
    }
    size_t lineEndPos = _client_input_buffer.find("\r\n", startPos+1);
    if(lineEndPos == _client_input_buffer.npos) {
        /* If <= PROTO_MULTI_MAX_SIZE, maybe the remaining data hasn't been received, 
         * so we can't judge it is a protocol err */
        if(_client_input_buffer.size() - startPos > PROTO_MULTI_MAX_SIZE) {
            std::string errMsg = "-ERR Protocol error: too big mbulk count string\r\n";
            AddReplyData2Client(errMsg);
            components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE, 
                                                        "Client %s sent a request with too big mbulk count string",
                                                        _client_name);
            _client_status |= static_cast<uint32_t>(ClientStatus::CLOSE_AFTER_REPLY);
            return false;            
        }
    }
    std::string strParaNum = _client_input_buffer.substr(startPos+1, lineEndPos-startPos-1);
    uint32_t paraNum = strtoul(strParaNum.c_str(), nullptr, 10);
    if(paraNum>1024*1024 || errno==ERANGE) {
        std::string errMsg = "-ERR Protocol error: invalid multibulk length\r\n";
        AddReplyData2Client(errMsg);
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                                    "Client %s sent a request with invalid multibulk length",
                                                    _client_name);
        _client_status |= static_cast<uint32_t>(ClientStatus::CLOSE_AFTER_REPLY);
        return false;
    }
    startPos = lineEndPos+2;
    uint32_t i = 0;
    for(; i < paraNum && startPos < _client_input_buffer.size(); ++i) {
        if(_client_input_buffer[startPos] != '$') {
            std::string errMsg = "-ERR Protocol error: expected '$', got ";
            errMsg.push_back(_client_input_buffer[startPos]);
            AddReplyData2Client(errMsg);
            components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                                        "Client %s sent a request with format error",
                                                        _client_name);
            _client_status |= static_cast<uint32_t>(ClientStatus::CLOSE_AFTER_REPLY);
            return false;            
        } 
        lineEndPos = _client_input_buffer.find("\r\n", startPos);
        if(lineEndPos == _client_input_buffer.npos) {
            if(_client_input_buffer.size() - startPos > PROTO_MULTI_MAX_SIZE) {
                std::string errMsg = "-ERR Protocol error: too big bulk count string\r\n";
                AddReplyData2Client(errMsg);
                components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE, 
                                                            "Client %s sent a request with too big bulk count string",
                                                            _client_name);
                _client_status |= static_cast<uint32_t>(ClientStatus::CLOSE_AFTER_REPLY);
                return false;            
            }
        }
        std::string strBulkLen = _client_input_buffer.substr(startPos+1, lineEndPos-startPos-1);
        uint32_t bulkLen = strtoul(strBulkLen.c_str(), nullptr, 10);
        if(errno == ERANGE || bulkLen > 512*1024*1024) {
            std::string errMsg = "-ERR Protocol error: invalid bulk length\r\n";
            AddReplyData2Client(errMsg);
            components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                                        "Client %s sent a request with invalid bulk length",
                                                        _client_name);
            _client_status |= static_cast<uint32_t>(ClientStatus::CLOSE_AFTER_REPLY);
            return false;
        }
        startPos = lineEndPos+2;
        lineEndPos = _client_input_buffer.find("\r\n", startPos);
        if((lineEndPos-startPos) != bulkLen) {
            std::string errMsg = "-ERR Protocol error: the parameter's length doesn't equal bulk length\r\n";
            AddReplyData2Client(errMsg);
            components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                                        "Client %s sent a request whose parameter's length doesn't equal its bulk length",
                                                        _client_name);
            _client_status |= static_cast<uint32_t>(ClientStatus::CLOSE_AFTER_REPLY);
            return false;
        }        
        std::string parameter = _client_input_buffer.substr(startPos, lineEndPos-startPos);
        if(i == 0) {
            /* Factory pattern */
            _current_command = DmdbCommand::GenerateCommandByName(parameter);
            if(_current_command == nullptr) {
                AddReplyData2Client("-ERR Unknown command:" + parameter + "\r\n");
            } 

        } else {
            if(_current_command != nullptr)
                _current_command->AppendCommandPara(parameter);
        }
        startPos = lineEndPos+2;
    }
    if(i != paraNum) {
        std::string errMsg = "-ERR Protocol error: the parameter's number doesn't equal mbulk length\r\n";
        AddReplyData2Client(errMsg);
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                                    "Client %s sent a request whose parameter's number doesn't equal mbulk length",
                                                    _client_name);
        _client_status |= static_cast<uint32_t>(ClientStatus::CLOSE_AFTER_REPLY);
        return false;
    }
    /*
    if(startPos == _client_input_buffer.size()) {
        startPos = 0;
    }
    */
    return true;
}

void DmdbClientContact::SetChecked() {
    _is_chekced = true;
}

bool DmdbClientContact::IsMultiState() {
    return _is_multi_state;
}

void DmdbClientContact::SetMultiState(bool state) {
    _is_multi_state = state;
}

void DmdbClientContact::SetStatus(uint32_t status) {
    _client_status |= status;
}

uint32_t DmdbClientContact::GetStatus() {
    return _client_status;
}

size_t DmdbClientContact::GetMultiQueueSize() {
    return _exec_command_queue.size();
}

DmdbCommand* DmdbClientContact::PopCommandOfExec() {
    DmdbCommand* command = nullptr;
    if(_exec_command_queue.size() > 0) {
        command = _exec_command_queue.front();
        _exec_command_queue.pop();
    }
    return command;
}

bool DmdbClientContact::ProcessClientRequest() {
    DmdbClientContactRequiredComponent components;
    GetDmdbClientContactRequiredComponent(components);
    size_t lastProcessedPos = _process_pos_of_input_buf;

    /* We shouldn't pause the replication from master */
    while(!components._repl_manager->IsWaitting(this) && 
          (!components._client_manager->AreClientsPaused() || components._repl_manager->IsMyMaster(this->GetClientName())) &&
          _client_input_buffer.length() > _process_pos_of_input_buf && 
          ProcessOneMultiProtocolRequest(_process_pos_of_input_buf) == true) {
        if(_current_command == nullptr) {
            lastProcessedPos = _process_pos_of_input_buf;
            continue;
        }
            
        std::string commandNameLower = _current_command->GetName();
        std::transform(commandNameLower.begin(), commandNameLower.end(), commandNameLower.begin(), tolower);        
        /* If this is a master client, _is_chekced will be set to true once the connection is created,
         * and master client won't replicate auth command to its replicas */
        if(!_is_chekced) {
            if(commandNameLower != "auth") {
                std::string errMsg = "-ERR unauthenticated\r\n";
                AddReplyData2Client(errMsg);
                components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                                            "Unauthenticated client %s sent a request",
                                                            _client_name);
                
                _client_status |= static_cast<uint32_t>(ClientStatus::CLOSE_AFTER_REPLY);
                return false;
                                
            } else {
                /* If Auth command executes successfully, it will set _is_checked to true */
                _current_command->Execute(*this);
                if(!_is_chekced) {
                    components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                                                "Client %s failed to authenticate",
                                                                _client_name);
                    
                    _client_status |= static_cast<uint32_t>(ClientStatus::CLOSE_AFTER_REPLY);
                    return false;
                                        
                }
            }
            lastProcessedPos = _process_pos_of_input_buf;
        } else {
            /*
            if(commandNameLower == "multi") {
                _is_multi_state = true;
                continue;
            }
            */ 
            bool isWCommand = DmdbCommand::IsWCommand(_current_command->GetName());
            
            if(!components._is_myself_master && isWCommand && components._repl_manager->GetMasterClientContact() != this) {
                AddReplyData2Client("-ERR Command:" + _current_command->GetName() + " is forbidden in slave\r\n");
                delete _current_command;
                _current_command = nullptr;
                lastProcessedPos = _process_pos_of_input_buf;
                continue;
            }
            /* If it is in multi state, we don't have to record lastProcessedPos because we will replicate the whole multi-exec
             * block if I am a master. First, replicate multi, then replicate the remaining. */
            if(_is_multi_state && commandNameLower != "exec") {
                _exec_command_queue.push(_current_command);
                AddReplyData2Client("+QUEUED\r\n");
                if(components._is_myself_master) {
                    components._repl_manager->ReplicateDataToSlaves(_client_input_buffer.substr(lastProcessedPos, _process_pos_of_input_buf - lastProcessedPos));
                }
                lastProcessedPos = _process_pos_of_input_buf;
                _current_command = nullptr;
                continue;
            }
            _current_command->Execute(*this);
            if (components._is_myself_master && (_current_command->GetName() == "multi" || _current_command->GetName() == "exec" || isWCommand)) {
                components._repl_manager->ReplicateDataToSlaves(_client_input_buffer.substr(lastProcessedPos, _process_pos_of_input_buf - lastProcessedPos));
            }

            if(components._repl_manager->IsMyMaster(this->GetClientName())) {
                components._repl_manager->SetReplayOkSize(_process_pos_of_input_buf - lastProcessedPos);
            }
            lastProcessedPos = _process_pos_of_input_buf;
        }
        delete _current_command;
        _current_command = nullptr;
    }
    ClearProcessedData();
    return true;
}

bool DmdbClientContact::IsChecked() {
    return _is_chekced;
}

std::string DmdbClientContact::GetIp() {
    return _client_ip;
}

int DmdbClientContact::GetPort() {
    return _client_port;
}

#ifdef MAKE_TEST
void DmdbClientContact::TestReplyToClient() {
    AddReplyData2Client("Hello world\n\t");
}
#endif

}
