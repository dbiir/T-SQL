#pragma once

#include <string>
#include <inttypes.h>
#include "comm_include.h"
#include "sm_base.h"
#include "config_include.h"

namespace phxpaxos
{

class CommitCtx;
class IOLoop;

class Committer
{
public:
    Committer(Config * poConfig, CommitCtx * poCommitCtx, IOLoop * poIOLoop, SMFac * poSMFac);
    ~Committer();

public:
    int NewValueGetID(const std::string & sValue, uint64_t & llInstanceID);
    
    int NewValueGetID(const std::string & sValue, uint64_t & llInstanceID, SMCtx * poSMCtx);
    
    int NewValueGetIDNoRetry(const std::string & sValue, uint64_t & llInstanceID, SMCtx * poSMCtx);

    int NewValue(const std::string & sValue);

public:
    void SetTimeoutMs(const int iTimeoutMs);

    void SetMaxHoldThreads(const int iMaxHoldThreads);

    void SetProposeWaitTimeThresholdMS(const int iWaitTimeThresholdMS);

private:
    void LogStatus();

private:
    Config * m_poConfig;
    CommitCtx * m_poCommitCtx;
    IOLoop * m_poIOLoop;
    SMFac * m_poSMFac;

    WaitLock m_oWaitLock;
    int m_iTimeoutMs;

    uint64_t m_llLastLogTime;
};
    
}
