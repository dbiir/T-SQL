#include "msg_counter.h"
#include <math.h>
#include "config_include.h"

namespace phxpaxos
{

MsgCounter :: MsgCounter(const Config * poConfig)
{
    m_poConfig = (Config *)poConfig;
    StartNewRound();
}

MsgCounter :: ~MsgCounter()
{
}

void MsgCounter :: StartNewRound()
{
    m_setReceiveMsgNodeID.clear();
    m_setRejectMsgNodeID.clear();
    m_setPromiseOrAcceptMsgNodeID.clear();
}

void MsgCounter :: AddReceive(const nodeid_t iNodeID)
{
    if (m_setReceiveMsgNodeID.find(iNodeID) == m_setReceiveMsgNodeID.end())
    {
        m_setReceiveMsgNodeID.insert(iNodeID);
    }
}

void MsgCounter :: AddReject(const nodeid_t iNodeID)
{
    if (m_setRejectMsgNodeID.find(iNodeID) == m_setRejectMsgNodeID.end())
    {
        m_setRejectMsgNodeID.insert(iNodeID);
    }
}

void MsgCounter :: AddPromiseOrAccept(const nodeid_t iNodeID)
{
    if (m_setPromiseOrAcceptMsgNodeID.find(iNodeID) == m_setPromiseOrAcceptMsgNodeID.end())
    {
        m_setPromiseOrAcceptMsgNodeID.insert(iNodeID);
    }
}

bool MsgCounter :: IsPassedOnThisRound()
{
    return (int)m_setPromiseOrAcceptMsgNodeID.size() >= m_poConfig->GetMajorityCount();
}

bool MsgCounter :: IsRejectedOnThisRound()
{
    return (int)m_setRejectMsgNodeID.size() >= m_poConfig->GetMajorityCount();
}

bool MsgCounter :: IsAllReceiveOnThisRound()
{
    return (int)m_setReceiveMsgNodeID.size() == m_poConfig->GetNodeCount();
}

}


