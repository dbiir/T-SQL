#pragma once

#include "base.h"
#include <string>
#include "comm_include.h"
#include "paxos_log.h"

namespace phxpaxos
{

class AcceptorState
{
public:
    AcceptorState(const Config * poConfig, const LogStorage * poLogStorage);
    ~AcceptorState();

    void Init();

    const BallotNumber & GetPromiseBallot() const;
    void SetPromiseBallot(const BallotNumber & oPromiseBallot);

    const BallotNumber & GetAcceptedBallot() const;
    void SetAcceptedBallot(const BallotNumber & oAcceptedBallot);

    const std::string & GetAcceptedValue();
    void SetAcceptedValue(const std::string & sAcceptedValue);

    const uint32_t GetChecksum() const;

    int Persist(const uint64_t llInstanceID, const uint32_t iLastChecksum);
    int Load(uint64_t & llInstanceID);

    BallotNumber m_oPromiseBallot;
    BallotNumber m_oAcceptedBallot;
    std::string m_sAcceptedValue;
    uint32_t m_iChecksum;

    Config * m_poConfig;
    PaxosLog m_oPaxosLog;

    int m_iSyncTimes;
};

class Acceptor : public Base
{
public:
    Acceptor(
        const Config * poConfig,
        const MsgTransport * poMsgTransport, 
        const Instance * poInstance,
        const LogStorage * poLogStorage);
    ~Acceptor();

    virtual void InitForNewPaxosInstance();

    int Init();

    AcceptorState * GetAcceptorState();

    int OnPrepare(const PaxosMsg & oPaxosMsg);

    void OnAccept(const PaxosMsg & oPaxosMsg);

    AcceptorState m_oAcceptorState;
};
    
}
