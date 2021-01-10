#pragma once

#include <string>
#include <typeinfo>
#include <inttypes.h>

namespace phxpaxos
{

//Paxoslib need to storage many datas, if you want to storage datas yourself,
//you must implememt all function in class LogStorage, and make sure that observe the writeoptions.

class WriteOptions
{
public:
    WriteOptions() : bSync(true) { }
    bool bSync;
};

class LogStorage
{
public:
    virtual ~LogStorage() {}

    virtual const std::string GetLogStorageDirPath(const int iGroupIdx) = 0;

    virtual int Get(const int iGroupIdx, const uint64_t llInstanceID, std::string & sValue) = 0;

    virtual int Put(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID, const std::string & sValue) = 0;

    virtual int Del(const WriteOptions & oWriteOptions, int iGroupIdx, const uint64_t llInstanceID) = 0;

    virtual int GetMaxInstanceID(const int iGroupIdx, uint64_t & llInstanceID) = 0;

    virtual int SetMinChosenInstanceID(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llMinInstanceID) = 0;

    virtual int GetMinChosenInstanceID(const int iGroupIdx, uint64_t & llMinInstanceID) = 0;

    virtual int ClearAllLog(const int iGroupIdx) = 0;

    virtual int SetSystemVariables(const WriteOptions & oWriteOptions, const int iGroupIdx, const std::string & sBuffer) = 0;

    virtual int GetSystemVariables(const int iGroupIdx, std::string & sBuffer) = 0;

    virtual int SetMasterVariables(const WriteOptions & oWriteOptions, const int iGroupIdx, const std::string & sBuffer) = 0;

    virtual int GetMasterVariables(const int iGroupIdx, std::string & sBuffer) = 0;
};

}
