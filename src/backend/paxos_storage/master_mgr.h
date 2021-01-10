#pragma once

#include "commdef.h"
#include "utils_include.h"
#include "node.h"
#include "master_sm.h"

namespace phxpaxos 
{

class MasterMgr : public Thread
{
public:
    MasterMgr(const Node * poPaxosNode, 
        const int iGroupIdx, 
        const LogStorage * poLogStorage,
        MasterChangeCallback pMasterChangeCallback);
    ~MasterMgr();

    void RunMaster();
    
    void StopMaster();

    int Init();

    void run();

    void SetLeaseTime(const int iLeaseTimeMs);

    void TryBeMaster(const int iLeaseTime);

    void DropMaster();

public:
    MasterStateMachine * GetMasterSM();

private:
    Node * m_poPaxosNode;

    MasterStateMachine m_oDefaultMasterSM;

private:
    int m_iLeaseTime;

    bool m_bIsEnd;
    bool m_bIsStarted;

    int m_iMyGroupIdx;

    bool m_bNeedDropMaster;
};
    
}
