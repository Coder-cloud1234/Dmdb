#pragma once

#include <unordered_map>
#include <string>
#include <vector>


namespace Dmdb{

template<class T>
class EqualFunction {
public:
    bool operator()(const T &key1, const T &key2) const {
        return key1._key_name == key2._key_name;
    }  
};

template<class T>
class HashFunction {
public:
    size_t operator()(const T &key) const {
        std::hash<std::string> hash;
        return hash(key._key_name);
    }
};

class DmdbKey {
public:
    void SetExpireTime(uint64_t expireTime);
    uint64_t GetExpireTime() const;
    std::string GetName() const;
    DmdbKey(std::string name);
    DmdbKey(std::string name, uint64_t ms);
    
    friend class HashFunction<DmdbKey>;
    friend class EqualFunction<DmdbKey>;
private:
    std::string _key_name;
    uint64_t _expire_ms;
};

enum class DmdbValueType {
    STRING
};

class DmdbValue {
public:
    size_t GetValueSize();
    size_t GetValueRawData(uint8_t* buf);
    DmdbValueType GetValueType();
    std::string GetValueTypeString();
    std::string GetValueString();
    DmdbValue(void* value);
    DmdbValue(void* value, DmdbValueType valType);
    ~DmdbValue();
private:
    void* _value_ptr;
    DmdbValueType _val_type;
};

class DmdbDatabaseManager {
public:
    bool SetKeyValuePair(const std::string& keyStr, const std::vector<std::string> &valVec, DmdbValueType type, uint64_t ms);
    bool SetKeyExpireTime(const std::string& keyStr, uint64_t ms); 
    bool DelKey(const std::string &keyStr);
    DmdbValue* GetValueByKey(const std::string &keyStr);
    bool GetKeyByName(const std::string &name, DmdbKey &key);
    void GetKeysByPattern(const std::string &patternStr, std::vector<DmdbKey> &keys);
    size_t GetDatabaseSize();
    bool GetNPairsFormatRawSequential(uint8_t* buf, size_t bufLen, size_t &copiedSize, size_t expectedAmount, size_t &actualAmount);
    size_t RemoveExpiredKeys();
    void SetExpireIntervalForDB(uint64_t ms);
    void Destroy();
#ifdef MAKE_TEST
    void PrintDatabase();
#endif
    DmdbDatabaseManager();
    ~DmdbDatabaseManager();
private:
    std::unordered_map<DmdbKey, DmdbValue*, HashFunction<DmdbKey>, EqualFunction<DmdbKey>> _database;
    uint64_t _last_expire_ms;
    uint64_t _expire_interval_ms;
};

}