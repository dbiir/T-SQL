#include "group.h"

namespace phxpaxos
{


Group :: Group(LogStorage * poLogStorage, 
            NetWork * poNetWork,    
            InsideSM * poMasterSM,
            const int iGroupIdx,
            const Options & oOptions) : 
    m_oCommunicate(&m_oConfig, oOptions.oMyNode.GetNodeID(), oOptions.iUDPMaxSize, poNetWork),
    m_oConfig(poLogStorage, oOptions.bSync, oOptions.iSyncInterval, oOptions.bUseMembership, 
            oOptions.oMyNode, oOptions.vecNodeInfoList, oOptions.vecFollowerNodeInfoList, 
            iGroupIdx, oOptions.iGroupCount, oOptions.pMembershipChangeCallback),
    m_oInstance(&m_oConfig, poLogStorage, &m_oCommunicate, oOptions),
    m_iInitRet(-1), m_poThread(nullptr)
{
    m_oConfig.SetMasterSM(poMasterSM);
}

Group :: ~Group()
{
}

void Group :: StartInit()
{
    m_poThread = new std::thread(&Group::Init, this);
    assert(m_poThread != nullptr);
}

void Group :: Init()
{
    m_iInitRet = m_oConfig.Init();
    if (m_iInitRet != 0)
    {
        return;
    }

    //inside sm
    AddStateMachine(m_oConfig.GetSystemVSM());
    AddStateMachine(m_oConfig.GetMasterSM());
    
    m_iInitRet = m_oInstance.Init();
}

int Group :: GetInitRet()
{
    m_poThread->join();
    delete m_poThread;

    return m_iInitRet;
}

void Group :: Start()
{
    m_oInstance.Start();
}

void Group :: Stop()
{
    m_oInstance.Stop();
}

Config * Group :: GetConfig()
{
    return &m_oConfig;
}

Instance * Group :: GetInstance()
{
    return &m_oInstance;
}

Committer * Group :: GetCommitter()
{
    return m_oInstance.GetCommitter();
}

Cleaner * Group :: GetCheckpointCleaner()
{
    return m_oInstance.GetCheckpointCleaner();
}

Replayer * Group :: GetCheckpointReplayer()
{
    return m_oInstance.GetCheckpointReplayer();
}

void Group :: AddStateMachine(StateMachine * poSM)
{
    m_oInstance.AddStateMachine(poSM);
}

}


