#pragma once

#include "utils_include.h"
#include "comm_include.h"
#include "config_include.h"
#include "paxos_log.h"

namespace phxpaxos
{

class Learner;

class LearnerSender : public Thread
{
public:
    LearnerSender(Config * poConfig, Learner * poLearner, PaxosLog * poPaxosLog);
    ~LearnerSender();

    void run();

    void Stop();

public:
    const bool Prepare(const uint64_t llBeginInstanceID, const nodeid_t iSendToNodeID);

    const bool Comfirm(const uint64_t llBeginInstanceID, const nodeid_t iSendToNodeID);

    void Ack(const uint64_t llAckInstanceID, const nodeid_t iFromNodeID);

private:
    void WaitToSend();

    void SendLearnedValue(const uint64_t llBeginInstanceID, const nodeid_t iSendToNodeID);

    int SendOne(const uint64_t llSendInstanceID, const nodeid_t iSendToNodeID, uint32_t & iLastChecksum);

    void SendDone();

    const bool IsIMSending();

    void ReleshSending();

    const bool CheckAck(const uint64_t llSendInstanceID);

    void CutAckLead();

private:
    Config * m_poConfig;
    Learner * m_poLearner;
    PaxosLog * m_poPaxosLog;
    SerialLock m_oLock;

    bool m_bIsIMSending;
    uint64_t m_llAbsLastSendTime;

    uint64_t m_llBeginInstanceID;
    nodeid_t m_iSendToNodeID;

    bool m_bIsComfirmed;

    uint64_t m_llAckInstanceID;
    uint64_t m_llAbsLastAckTime;
    int m_iAckLead;

    bool m_bIsEnd;
    bool m_bIsStart;
};
    
}
