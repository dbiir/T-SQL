#pragma once

#include "network.h"
#include "options.h"
#include <map>
#include "msg_transport.h"
#include "config_include.h"

namespace phxpaxos
{

class Communicate : public MsgTransport
{
public:
    Communicate(
            const Config * poConfig,
            const nodeid_t iMyNodeID, 
            const int iUDPMaxSize,
            NetWork * poNetwork);
    ~Communicate();

    int SendMessage(const int iGroupIdx, const nodeid_t iSendtoNodeID, const std::string & sMessage,
            const int iSendType = Message_SendType_UDP);

    int BroadcastMessage(const int iGroupIdx, const std::string & sMessage,
            const int iSendType = Message_SendType_UDP);

    int BroadcastMessageFollower(const int iGroupIdx, const std::string & sMessage,
            const int iSendType = Message_SendType_UDP);
    
    int BroadcastMessageTempNode(const int iGroupIdx, const std::string & sMessage,
            const int iSendType = Message_SendType_UDP);

public:
    void SetUDPMaxSize(const size_t iUDPMaxSize);

private:
    int Send(const int iGroupIdx, const nodeid_t iNodeID, 
            const NodeInfo & tNodeInfo, const std::string & sMessage, const int iSendType);

private:
    Config * m_poConfig;
    NetWork * m_poNetwork;

    nodeid_t m_iMyNodeID;
    size_t m_iUDPMaxSize; 
};
    
}
