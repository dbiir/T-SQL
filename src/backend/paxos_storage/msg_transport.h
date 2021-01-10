#pragma once

#include "options.h"

namespace phxpaxos
{

enum Message_SendType
{
    Message_SendType_UDP = 0,
    Message_SendType_TCP = 1,
};

class MsgTransport
{
public:
    virtual ~MsgTransport() {}

    virtual int SendMessage(const int iGroupIdx, const nodeid_t iSendtoNodeID, 
            const std::string & sBuffer, const int iSendType = Message_SendType_UDP) = 0;

    virtual int BroadcastMessage(const int iGroupIdx, const std::string & sBuffer, 
            const int iSendType = Message_SendType_UDP) = 0;
    
    virtual int BroadcastMessageFollower(const int iGroupIdx, const std::string & sBuffer, 
            const int iSendType = Message_SendType_UDP) = 0;
    
    virtual int BroadcastMessageTempNode(const int iGroupIdx, const std::string & sBuffer, 
            const int iSendType = Message_SendType_UDP) = 0;
};
    
}
