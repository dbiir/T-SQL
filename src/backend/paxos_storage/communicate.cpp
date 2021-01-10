#include "communicate.h"
#include "comm_include.h"
#include "commdef.h"

namespace phxpaxos
{

Communicate :: Communicate(
        const Config * poConfig,
        const nodeid_t iMyNodeID, 
        const int iUDPMaxSize,
        NetWork * poNetwork)
    : m_poConfig((Config *)poConfig), m_poNetwork(poNetwork), m_iMyNodeID(iMyNodeID), m_iUDPMaxSize(iUDPMaxSize)
{
}

Communicate :: ~Communicate()
{
}

int Communicate :: Send(const int iGroupIdx, const nodeid_t iNodeID, 
        const NodeInfo & oNodeInfo, const std::string & sMessage, const int iSendType)
{
    if ((int)sMessage.size() > MAX_VALUE_SIZE)
    {
        BP->GetNetworkBP()->SendRejectByTooLargeSize();
        PLGErr("Message size too large %zu, max size %u, skip message", 
                sMessage.size(), MAX_VALUE_SIZE);
        return 0;
    }

    BP->GetNetworkBP()->Send(sMessage);
    
    if (sMessage.size() > m_iUDPMaxSize || iSendType == Message_SendType_TCP)
    {
        BP->GetNetworkBP()->SendTcp(sMessage);
        return m_poNetwork->SendMessageTCP(iGroupIdx, oNodeInfo.GetIP(), oNodeInfo.GetPort(), sMessage);
    }
    else
    {
        BP->GetNetworkBP()->SendUdp(sMessage);
        return m_poNetwork->SendMessageUDP(iGroupIdx, oNodeInfo.GetIP(), oNodeInfo.GetPort(), sMessage);
    }
}

int Communicate :: SendMessage(const int iGroupIdx, const nodeid_t iSendtoNodeID, const std::string & sMessage, const int iSendType)
{
    return Send(iGroupIdx, iSendtoNodeID, NodeInfo(iSendtoNodeID), sMessage, iSendType);
}

int Communicate :: BroadcastMessage(const int iGroupIdx, const std::string & sMessage, const int iSendType)
{
    const std::set<nodeid_t> & setNodeInfo = m_poConfig->GetSystemVSM()->GetMembershipMap();
    
    for (auto & it : setNodeInfo)
    {
        if (it != m_iMyNodeID)
        {
            Send(iGroupIdx, it, NodeInfo(it), sMessage, iSendType);
        }
    }

    return 0;
}

int Communicate :: BroadcastMessageFollower(const int iGroupIdx, const std::string & sMessage, const int iSendType)
{
    const std::map<nodeid_t, uint64_t> & mapFollowerNodeInfo = m_poConfig->GetMyFollowerMap(); 
    
    for (auto & it : mapFollowerNodeInfo)
    {
        if (it.first != m_iMyNodeID)
        {
            Send(iGroupIdx, it.first, NodeInfo(it.first), sMessage, iSendType);
        }
    }
    
    PLGDebug("%zu node", mapFollowerNodeInfo.size());

    return 0;
}

int Communicate :: BroadcastMessageTempNode(const int iGroupIdx, const std::string & sMessage, const int iSendType)
{
    const std::map<nodeid_t, uint64_t> & mapTempNode = m_poConfig->GetTmpNodeMap(); 
    
    for (auto & it : mapTempNode)
    {
        if (it.first != m_iMyNodeID)
        {
            Send(iGroupIdx, it.first, NodeInfo(it.first), sMessage, iSendType);
        }
    }
    
    PLGDebug("%zu node", mapTempNode.size());

    return 0;
}

void Communicate :: SetUDPMaxSize(const size_t iUDPMaxSize)
{
    m_iUDPMaxSize = iUDPMaxSize;
}

}


