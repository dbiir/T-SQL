#pragma once

#include <string>
#include <set>
#include "commdef.h"

namespace phxpaxos
{

class Config;
class PaxosLog;

class MsgCounter
{
public:
    MsgCounter(const Config * poConfig);
    ~MsgCounter();

    void AddReceive(const nodeid_t iNodeID);
    void AddReject(const nodeid_t iNodeID);
    void AddPromiseOrAccept(const nodeid_t iNodeID);

    bool IsPassedOnThisRound();
    bool IsRejectedOnThisRound();
    bool IsAllReceiveOnThisRound();

    void StartNewRound();

public:
    Config * m_poConfig;

    std::set<nodeid_t> m_setReceiveMsgNodeID;
    std::set<nodeid_t> m_setRejectMsgNodeID;
    std::set<nodeid_t> m_setPromiseOrAcceptMsgNodeID;
};
    
}
