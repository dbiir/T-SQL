#pragma once

#include "commdef.h"
#include <vector>
#include "sm.h"

namespace phxpaxos
{

class BatchSMCtx
{
public:
    std::vector<SMCtx *> m_vecSMCtxList;
};

class SMFac
{
public:
    SMFac(const int iMyGroupIdx);
    ~SMFac();
    
    bool Execute(const int iGroupIdx, const uint64_t llInstanceID, 
            const std::string & sPaxosValue, SMCtx * poSMCtx);

    bool ExecuteForCheckpoint(const int iGroupIdx, const uint64_t llInstanceID, const std::string & sPaxosValue);

    void PackPaxosValue(std::string & sPaxosValue, const int iSMID = 0);

    void AddSM(StateMachine * poSM);

    void BeforePropose(const int iGroupIdx, std::string & sValue);

    void BeforeBatchPropose(const int iGroupIdx, std::string & sValue);

    void BeforeProposeCall(const int iGroupIdx, const int iSMID, std::string & sValue, bool & change);

    const uint64_t GetCheckpointInstanceID(const int iGroupIdx) const;

    std::vector<StateMachine *> GetSMList();

    bool BatchExecute(const int iGroupIdx, const uint64_t llInstanceID, 
            const std::string & sBodyValue, BatchSMCtx * poBatchSMCtx);

    bool DoExecute(const int iGroupIdx, const uint64_t llInstanceID, 
            const std::string & sBodyValue, const int iSMID, SMCtx * poSMCtx);

    bool BatchExecuteForCheckpoint(const int iGroupIdx, const uint64_t llInstanceID, 
            const std::string & sBodyValue);

    bool DoExecuteForCheckpoint(const int iGroupIdx, const uint64_t llInstanceID, 
            const std::string & sBodyValue, const int iSMID);

private:
    std::vector<StateMachine *> m_vecSMList;
    int m_iMyGroupIdx;
};
    
}
