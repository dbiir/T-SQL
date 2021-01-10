#pragma once

#include <string>
#include <vector>
#include <typeinfo>
#include <stdint.h>
#include <inttypes.h>

namespace phxpaxos
{

class SMCtx
{
public:
    SMCtx();
    SMCtx(const int iSMID, void * pCtx);

    int m_iSMID;
    void * m_pCtx;
};

class CheckpointFileInfo
{
public:
    std::string m_sFilePath;
    size_t m_llFileSize;
};

typedef std::vector<CheckpointFileInfo> CheckpointFileInfoList;

const uint64_t NoCheckpoint = (uint64_t)-1; 

class StateMachine
{
public:
    virtual ~StateMachine() {}

    virtual const int SMID() const = 0;

    virtual bool Execute(const int iGroupIdx, const uint64_t llInstanceID, 
            const std::string & sPaxosValue, SMCtx * poSMCtx) = 0;

    virtual bool ExecuteForCheckpoint(const int iGroupIdx, const uint64_t llInstanceID, 
            const std::string & sPaxosValue);

    virtual const uint64_t GetCheckpointInstanceID(const int iGroupIdx) const;

    virtual int LockCheckpointState();

    virtual int GetCheckpointState(const int iGroupIdx, std::string & sDirPath, 
            std::vector<std::string> & vecFileList); 

    virtual void UnLockCheckpointState();

    virtual int LoadCheckpointState(const int iGroupIdx, const std::string & sCheckpointTmpFileDirPath,
            const std::vector<std::string> & vecFileList, const uint64_t llCheckpointInstanceID);

    virtual void BeforePropose(const int iGroupIdx, std::string & sValue);

    virtual const bool NeedCallBeforePropose();
};
    
}
