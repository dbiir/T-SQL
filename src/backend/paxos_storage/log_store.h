#pragma once

#include <string>
#include <mutex>
#include "commdef.h"
#include "utils_include.h"
#include "commdef.h"
#include "comm_include.h"

namespace phxpaxos
{

class Database;

#define FILEID_LEN (sizeof(int) + sizeof(int) + sizeof(uint32_t))

class LogStoreLogger
{
public:
    LogStoreLogger();
    ~LogStoreLogger();

    void Init(const std::string & sPath);
    void Log(const char * pcFormat, ...);

private:
    int m_iLogFd;
};

class LogStore
{
public:
    LogStore();
    ~LogStore();

    int Init(const std::string & sPath, const int iMyGroupIdx, Database * poDatabase);

    int Append(const WriteOptions & oWriteOptions, const uint64_t llInstanceID, const std::string & sBuffer, std::string & sFileID);

    int Read(const std::string & sFileID, uint64_t & llInstanceID, std::string & sBuffer);

    int Del(const std::string & sFileID, const uint64_t llInstanceID);

    int ForceDel(const std::string & sFileID, const uint64_t llInstanceID);

    ////////////////////////////////////////////

    const bool IsValidFileID(const std::string & sFileID);

    ////////////////////////////////////////////
    
    int RebuildIndex(Database * poDatabase, int & iNowFileWriteOffset);

    int RebuildIndexForOneFile(const int iFileID, const int iOffset, 
            Database * poDatabase, int & iNowFileWriteOffset, uint64_t & llNowInstanceID);

private:
    void GenFileID(const int iFileID, const int iOffset, const uint32_t iCheckSum, std::string & sFileID);

    void ParseFileID(const std::string & sFileID, int & iFileID, int & iOffset, uint32_t & iCheckSum);

    int IncreaseFileID();

    int OpenFile(const int iFileID, int & iFd);

    int DeleteFile(const int iFileID);

    int GetFileFD(const int iNeedWriteSize, int & iFd, int & iFileID, int & iOffset);

    int ExpandFile(int iFd, int & iFileSize);
    
private:
    int m_iFd;
    int m_iMetaFd;
    int m_iFileID;
    std::string m_sPath;
    BytesBuffer m_oTmpBuffer;
    BytesBuffer m_oTmpAppendBuffer;

    std::mutex m_oMutex;
    std::mutex m_oReadMutex;

    int m_iDeletedMaxFileID;
    int m_iMyGroupIdx;

    int m_iNowFileSize;
    int m_iNowFileOffset;

private:
    TimeStat m_oTimeStat;
    LogStoreLogger m_oFileLogger;
};

}
