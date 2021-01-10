#include "commitctx.h"
#include "sm.h"

namespace phxpaxos
{

CommitCtx :: CommitCtx(Config * poConfig)
    : m_poConfig(poConfig)
{
    NewCommit(nullptr, nullptr, 0);
}

CommitCtx :: ~CommitCtx()
{
}

void CommitCtx :: NewCommit(std::string * psValue, SMCtx * poSMCtx, const int iTimeoutMs)
{
    m_oSerialLock.Lock();

    m_llInstanceID = (uint64_t)-1;
    m_iCommitRet = -1;
    m_bIsCommitEnd = false;
    m_iTimeoutMs = iTimeoutMs;

    m_psValue = psValue;
    m_poSMCtx = poSMCtx;

    if (psValue != nullptr)
    {
        PLGHead("OK, valuesize %zu", psValue->size());
    }

    m_oSerialLock.UnLock();
}


const bool CommitCtx :: IsNewCommit() const
{
    return m_llInstanceID == (uint64_t)-1 && m_psValue != nullptr;
}

std::string & CommitCtx :: GetCommitValue()
{
    return *m_psValue;
}

void CommitCtx :: StartCommit(const uint64_t llInstanceID)
{
    m_oSerialLock.Lock();
    m_llInstanceID = llInstanceID;
    m_oSerialLock.UnLock();
}

bool CommitCtx :: IsMyCommit(const uint64_t llInstanceID, const std::string & sLearnValue,  SMCtx *& poSMCtx)
{
    m_oSerialLock.Lock();

    bool bIsMyCommit = false;

    if ((!m_bIsCommitEnd) && (m_llInstanceID == llInstanceID))
    {
        bIsMyCommit = (sLearnValue == (*m_psValue));
    }

    if (bIsMyCommit)
    {
        poSMCtx = m_poSMCtx;
    }

    m_oSerialLock.UnLock();

    return bIsMyCommit;
}

void CommitCtx :: SetResultOnlyRet(const int iCommitRet)
{
    return SetResult(iCommitRet, (uint64_t)-1, "");
}

void CommitCtx :: SetResult(
        const int iCommitRet, 
        const uint64_t llInstanceID, 
        const std::string & sLearnValue)
{
    m_oSerialLock.Lock();

    if (m_bIsCommitEnd || (m_llInstanceID != llInstanceID))
    {
        m_oSerialLock.UnLock();
        return;
    }

    m_iCommitRet = iCommitRet;

    if (m_iCommitRet == 0)
    {
        if ((*m_psValue) != sLearnValue)
        {
            m_iCommitRet = PaxosTryCommitRet_Conflict;
        }
    }

    m_bIsCommitEnd = true;
    m_psValue = nullptr;

    m_oSerialLock.Interupt();
    m_oSerialLock.UnLock();
}


int CommitCtx :: GetResult(uint64_t & llSuccInstanceID)
{
    m_oSerialLock.Lock();

    while (!m_bIsCommitEnd)
    {
        m_oSerialLock.WaitTime(1000);
    }

    if (m_iCommitRet == 0)
    {
        llSuccInstanceID = m_llInstanceID;
        PLGImp("commit success, instanceid %lu", llSuccInstanceID);
    }
    else
    {
        PLGErr("commit fail, ret %d", m_iCommitRet);
    }
    
    m_oSerialLock.UnLock();

    return m_iCommitRet;
}

const int CommitCtx :: GetTimeoutMs() const
{
    return m_iTimeoutMs;
}

}


