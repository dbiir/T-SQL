#pragma once

#include <string>
#include "comm_include.h"
#include "config_include.h"

namespace phxpaxos
{

class StateMachine;

class CommitCtx
{
public:
    CommitCtx(Config * poConfig);
    ~CommitCtx();

    void NewCommit(std::string * psValue, SMCtx * poSMCtx, const int iTimeoutMs);
    
    const bool IsNewCommit() const;

    std::string & GetCommitValue();

    void StartCommit(const uint64_t llInstanceID);

    bool IsMyCommit(const uint64_t llInstanceID, const std::string & sLearnValue, SMCtx *& poSMCtx);

public:
    void SetResult(const int iCommitRet, const uint64_t llInstanceID, const std::string & sLearnValue);

    void SetResultOnlyRet(const int iCommitRet);

    int GetResult(uint64_t & llSuccInstanceID);

public:
    const int GetTimeoutMs() const;

private:
    Config * m_poConfig;

    uint64_t m_llInstanceID;
    int m_iCommitRet;
    bool m_bIsCommitEnd;
    int m_iTimeoutMs;

    std::string * m_psValue;
    SMCtx * m_poSMCtx;
    SerialLock m_oSerialLock;
};
}
