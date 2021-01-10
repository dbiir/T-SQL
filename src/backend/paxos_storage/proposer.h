#pragma once

#include "base.h"
#include <string>
#include "ioloop.h"
#include "msg_counter.h"

namespace phxpaxos
{

class ProposerState
{
public:
    ProposerState(const Config * poConfig);
    ~ProposerState();

    void Init();

    void SetStartProposalID(const uint64_t llProposalID);

    void NewPrepare();

    void AddPreAcceptValue(const BallotNumber & oOtherPreAcceptBallot, const std::string & sOtherPreAcceptValue);

    const uint64_t GetProposalID();

    const std::string & GetValue();

    void SetValue(const std::string & sValue);

    void SetOtherProposalID(const uint64_t llOtherProposalID);

    void ResetHighestOtherPreAcceptBallot();

public:
    uint64_t m_llProposalID;
    uint64_t m_llHighestOtherProposalID;
    std::string m_sValue;

    BallotNumber m_oHighestOtherPreAcceptBallot;

    Config * m_poConfig;
};

class Learner;

class Proposer : public Base
{
public:
    Proposer(
            const Config * poConfig, 
            const MsgTransport * poMsgTransport,
            const Instance * poInstance,
            const Learner * poLearner,
            const IOLoop * poIOLoop);
    ~Proposer();

    void SetStartProposalID(const uint64_t llProposalID);

    virtual void InitForNewPaxosInstance();

    int NewValue(const std::string & sValue);
    
    bool IsWorking();

    void Prepare(const bool bNeedNewBallot = true);

    void OnPrepareReply(const PaxosMsg & oPaxosMsg);

    void OnExpiredPrepareReply(const PaxosMsg & oPaxosMsg);

    void Accept();

    void OnAcceptReply(const PaxosMsg & oPaxosMsg);

    void OnExpiredAcceptReply(const PaxosMsg & oPaxosMsg);

    void OnPrepareTimeout();

    void OnAcceptTimeout();

    void ExitPrepare();

    void ExitAccept();

    void CancelSkipPrepare();

    void AddPrepareTimer(const int iTimeoutMs = 0);
    
    void AddAcceptTimer(const int iTimeoutMs = 0);

public:
    ProposerState m_oProposerState;
    MsgCounter m_oMsgCounter;
    Learner * m_poLearner;

    bool m_bIsPreparing;
    bool m_bIsAccepting;

    IOLoop * m_poIOLoop;

    uint32_t m_iPrepareTimerID;
    int m_iLastPrepareTimeoutMs;
    uint32_t m_iAcceptTimerID;
    int m_iLastAcceptTimeoutMs;
    uint64_t m_llTimeoutInstanceID;

    bool m_bCanSkipPrepare;

    bool m_bWasRejectBySomeone;

    TimeStat m_oTimeStat;
};
    
}
