#include "breakpoint.h"

namespace phxpaxos
{

Breakpoint * Breakpoint :: m_poBreakpoint = nullptr;

Breakpoint :: Breakpoint() 
{
}

void Breakpoint :: SetInstance(Breakpoint * poBreakpoint)
{
    m_poBreakpoint = poBreakpoint;
}

Breakpoint * Breakpoint :: Instance()
{
    if (m_poBreakpoint != nullptr)
    {
        return m_poBreakpoint;
    }
    
    static Breakpoint oBreakpoint;
    return &oBreakpoint;
}
    
ProposerBP * Breakpoint :: GetProposerBP()
{
    return &m_oProposerBP;
}

AcceptorBP * Breakpoint :: GetAcceptorBP()
{
    return &m_oAcceptorBP;
}

LearnerBP * Breakpoint :: GetLearnerBP()
{
    return &m_oLearnerBP;
}

InstanceBP * Breakpoint :: GetInstanceBP()
{
    return &m_oInstanceBP;
}

CommiterBP * Breakpoint :: GetCommiterBP()
{
    return &m_oCommiterBP;
}

IOLoopBP * Breakpoint :: GetIOLoopBP()
{
    return &m_oIOLoopBP;
}

NetworkBP * Breakpoint :: GetNetworkBP()
{
    return &m_oNetworkBP;
}

LogStorageBP * Breakpoint :: GetLogStorageBP()
{
    return &m_oLogStorageBP;
}

AlgorithmBaseBP * Breakpoint :: GetAlgorithmBaseBP()
{
    return &m_oAlgorithmBaseBP;
}

CheckpointBP * Breakpoint :: GetCheckpointBP()
{
    return &m_oCheckpointBP;
}

MasterBP * Breakpoint :: GetMasterBP()
{
    return &m_oMasterBP;
}
    
}


