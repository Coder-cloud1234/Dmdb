#include <time.h>

#include <regex>

#include "DmdbDatabaseManager.hpp"
#include "DmdbUtil.hpp"


namespace Dmdb {

DmdbKey::DmdbKey(std::string name) : _key_name(name), _expire_ms(0) {

}

DmdbKey::DmdbKey(std::string name, uint64_t ms) : _key_name(name), _expire_ms(ms) {

}

std::string DmdbKey::GetName() const {
    return _key_name;
}

void DmdbKey::SetExpireTime(uint64_t expireTime) {
    _expire_ms = expireTime;
}

uint64_t DmdbKey::GetExpireTime() const {
    return _expire_ms;
}

/* Here you must ensure that buf's size is bigger than value's */
size_t DmdbValue::GetValueRawData(uint8_t* buf) {
    size_t getSize = GetValueSize();
    switch(_val_type) {
        case DmdbValueType::STRING: {
            memcpy(buf, static_cast<std::string*>(_value_ptr)->c_str(), getSize);
            return getSize;
        }
    }
    return 0;
}

size_t DmdbValue::GetValueSize() {
    switch(_val_type) {
        case DmdbValueType::STRING: {
            return static_cast<std::string*>(_value_ptr)->length();
            break;
        }
    }
    return 0;
}

DmdbValueType DmdbValue::GetValueType() {
    return _val_type;
}

std::string DmdbValue::GetValueTypeString() {
    switch(_val_type) {
        case DmdbValueType::STRING: {
            return "string";
            break;
        }
    }
    return "unknown type";
}

std::string DmdbValue::GetValueString() {
    std::string msgResult;
    switch(_val_type) {
        case DmdbValueType::STRING:{
            msgResult = *static_cast<std::string*>(_value_ptr);
            break;
        }
    }
    return msgResult; 
}

DmdbValue::DmdbValue(void* value):_value_ptr(value) {

}

DmdbValue::DmdbValue(void* value, DmdbValueType valType):_value_ptr(value), _val_type(valType) {

}


DmdbValue::~DmdbValue() {
    switch(_val_type) {
        case DmdbValueType::STRING: {
           delete static_cast<std::string*>(_value_ptr);
           break; 
        }
    } 
}


DmdbDatabaseManager::DmdbDatabaseManager() {}

DmdbDatabaseManager::~DmdbDatabaseManager() {
    std::unordered_map<DmdbKey, DmdbValue*, HashFunction<DmdbKey>, EqualFunction<DmdbKey>>::iterator it;
    while((it = _database.begin()) != _database.end()) {
        DelKey(it->first.GetName());
    }
}

void DmdbDatabaseManager::Destroy() {
    std::unordered_map<DmdbKey, DmdbValue*, HashFunction<DmdbKey>, EqualFunction<DmdbKey>>::iterator it;
    while((it = _database.begin()) != _database.end()) {
        DelKey(it->first.GetName());
    }    
}

bool DmdbDatabaseManager::GetKeyByName(const std::string &name, DmdbKey &key) {
    DmdbKey keyTmp(name);
    std::unordered_map<DmdbKey, DmdbValue*, HashFunction<DmdbKey>, EqualFunction<DmdbKey>>::iterator it = _database.find(keyTmp);
    if(it == _database.end()) {
        return false;
    }
    key = it->first;
    return true;
}

DmdbValue* DmdbDatabaseManager::GetValueByKey(const std::string &keyStr) {
    DmdbKey key(keyStr);
    std::unordered_map<DmdbKey, DmdbValue*, HashFunction<DmdbKey>, EqualFunction<DmdbKey>>::iterator it = _database.find(key);  
    if (it != _database.end()) {
        return it->second;
    }
    return nullptr;      
}

bool DmdbDatabaseManager::DelKey(const std::string &keyStr) {
    DmdbKey key(keyStr);
    std::unordered_map<DmdbKey, DmdbValue*, HashFunction<DmdbKey>, EqualFunction<DmdbKey>>::iterator it = _database.find(key);
    if (it != _database.end()) {
        delete it->second;
        _database.erase(key);
        return true;
    }
    return false;
}

bool DmdbDatabaseManager::SetKeyValuePair(const std::string& keyStr, const std::vector<std::string> &valVec, DmdbValueType valType, uint64_t ms) {
    if(ms!=0 && ms<=DmdbUtil::GetCurrentMs()) {
        DelKey(keyStr);
        return false;
    }
    DmdbKey key(keyStr, ms);
    std::unordered_map<DmdbKey, DmdbValue*, HashFunction<DmdbKey>, EqualFunction<DmdbKey>>::iterator it = _database.find(key);
    DmdbValue *val = nullptr;
    switch (valType) {
        case DmdbValueType::STRING: {
            /* Here we assume that valVec.size() > 0 */
            std::string *strPointer = new std::string(valVec[0]);
            val = new DmdbValue(strPointer, valType);
            break;
        }
    }
    if(it != _database.end()) {
        delete it->second;
    }
    _database[key] = val;
    return true;
}

bool DmdbDatabaseManager::SetKeyExpireTime(const std::string& keyStr, uint64_t ms) {
    DmdbKey key(keyStr, ms);
    std::unordered_map<DmdbKey, DmdbValue*, HashFunction<DmdbKey>, EqualFunction<DmdbKey>>::iterator it = _database.find(key);
    if(it == _database.end()) {
        return false;
    }
    if((ms!=0 && ms<=DmdbUtil::GetCurrentMs()) || (it->first.GetExpireTime() <= DmdbUtil::GetCurrentMs())) {
        DelKey(keyStr);
        return false;
    }
    DmdbValue* value = it->second;
    _database.erase(it);
    _database[key] = value;
    return true;    
}

void DmdbDatabaseManager::GetKeysByPattern(const std::string &patternStr, std::vector<DmdbKey> &keys) {
    std::regex regexPattern(patternStr);
    std::unordered_map<DmdbKey, DmdbValue*, HashFunction<DmdbKey>, EqualFunction<DmdbKey>>::iterator it = _database.begin();
    while(it != _database.end()) {
        if(std::regex_search(it->first.GetName(), regexPattern)) {
            if(it->first.GetExpireTime() == 0 || it->first.GetExpireTime() > DmdbUtil::GetCurrentMs()) {
                keys.emplace_back(it->first);
            }
        }
        it++;
    }
}

size_t DmdbDatabaseManager::GetDatabaseSize() {
    return _database.size();
}

/* The format of a pair is as below:
 * Expire_time: 8 bytes
 * Key_length: 4 bytes
 * Key_name: 
 * value_type: 1 byte
 * value_length: 4 bytes
 * value_raw_data: */ 
bool DmdbDatabaseManager::GetNPairsFormatRawSequential(uint8_t* buf, size_t bufLen, size_t &copiedSize,
                                                       size_t expectedAmount, size_t &actualAmount) {
    // static size_t pairsAmount = 0;
    actualAmount = 0;
    copiedSize = 0;
    static std::unordered_map<DmdbKey, DmdbValue*, HashFunction<DmdbKey>, EqualFunction<DmdbKey>>::iterator it = _database.begin();

    while(it != _database.end() && actualAmount < expectedAmount) {
        uint64_t expireTime = it->first.GetExpireTime();
        uint32_t keyLength = static_cast<uint32_t>(it->first.GetName().length());

        uint32_t valLength = static_cast<uint32_t>(it->second->GetValueSize());

        size_t pairTotalSize = 17+keyLength+valLength;
        if(bufLen - copiedSize < pairTotalSize) {
            break;
        }
        memcpy(buf+copiedSize, &expireTime, sizeof(expireTime));
        copiedSize += sizeof(expireTime);
        memcpy(buf+copiedSize, &keyLength, sizeof(keyLength));
        copiedSize += sizeof(keyLength);
        memcpy(buf+copiedSize, it->first.GetName().c_str(), keyLength);
        copiedSize += keyLength;
        uint8_t valType = static_cast<uint8_t>(it->second->GetValueType());
        memcpy(buf+copiedSize, &valType, sizeof(valType));
        copiedSize += sizeof(valType);
        memcpy(buf+copiedSize, &valLength, sizeof(valLength));
        copiedSize += sizeof(valLength);
        it->second->GetValueRawData(buf+copiedSize);
        copiedSize += valLength;

        actualAmount++;
        it++;
    }

    if(it == _database.end()) {
        it = _database.begin();
        return true;
    }
    return false;
}

}
