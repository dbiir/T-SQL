#include "replayer.h"
#include "storage.h"
#include "sm_base.h"
#include "comm_include.h"
#include "config_include.h"
#include "cp_mgr.h"

namespace phxpaxos
{

Replayer :: Replayer(
    Config * poConfig, 
    SMFac * poSMFac, 
    LogStorage * poLogStorage, 
    CheckpointMgr * poCheckpointMgr)
    : m_poConfig(poConfig), 
      m_poSMFac(poSMFac), 
      m_oPaxosLog(poLogStorage), 
      m_poCheckpointMgr(poCheckpointMgr),
      m_bCanrun(false),
      m_bIsPaused(true),
      m_bIsEnd(false){}

Replayer :: ~Replayer(){}

void Replayer :: Stop()
{
    m_bIsEnd = true;
    join();
}

void Replayer :: Pause()
{
    m_bCanrun = false;
}

void Replayer :: Continue()
{
    m_bIsPaused = false;
    m_bCanrun = true;
}

const bool Replayer:: IsPaused() const
{
    return m_bIsPaused;
}

void Replayer :: run()
{
    PLGHead("Checkpoint.Replayer [START]");
    uint64_t llInstanceID = m_poSMFac->GetCheckpointInstanceID(m_poConfig->GetMyGroupIdx()) + 1;

    while (true)
    {
        if (m_bIsEnd)
        {
            PLGHead("Checkpoint.Replayer [END]");
            return;
        }
        
        if (!m_bCanrun)
        {
            m_bIsPaused = true;
            Time::MsSleep(1000);
            continue;
        }
        
        if (llInstanceID >= m_poCheckpointMgr->GetMaxChosenInstanceID())
        {
            Time::MsSleep(1000);
            continue;
        }
        
        bool bPlayRet = PlayOne(llInstanceID);
        if (bPlayRet)
        {
            PLGImp("Play one done, instanceid %lu", llInstanceID);
            llInstanceID++;
        }
        else
        {
            PLGErr("Play one fail, instanceid %lu", llInstanceID);
            Time::MsSleep(500);
        }
    }
}

bool Replayer :: PlayOne(const uint64_t llInstanceID)
{
    AcceptorStateData oState; 
    int ret = m_oPaxosLog.ReadState(m_poConfig->GetMyGroupIdx(), llInstanceID, oState);
    if (ret != 0)
    {
        return false;
    }

    bool bExecuteRet = m_poSMFac->ExecuteForCheckpoint(
            m_poConfig->GetMyGroupIdx(), llInstanceID, oState.acceptedvalue());
    if (!bExecuteRet)
    {
        PLGErr("Checkpoint sm excute fail, instanceid %lu", llInstanceID);
    }

    return bExecuteRet;
}

}
