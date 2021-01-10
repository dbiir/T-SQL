#pragma once

#include "base.h"
#include <string>
#include "commdef.h"
#include "comm_include.h"
#include "paxos_log.h"
#include "ioloop.h"
#include "learner_sender.h"
#include "checkpoint_sender.h"
#include "checkpoint_receiver.h"

namespace phxpaxos
{

class LearnerState
{
public:
    LearnerState(const Config * poConfig, const LogStorage * poLogStorage);
    ~LearnerState();

    void Init();

    int LearnValue(const uint64_t llInstanceID, const BallotNumber & oLearnedBallot, 
            const std::string & sValue, const uint32_t iNewChecksum);

    void LearnValueWithoutWrite(const uint64_t llInstanceID, 
            const std::string & sValue, const uint32_t iNewChecksum);

    const std::string & GetLearnValue();

    const bool GetIsLearned();

    const uint32_t GetNewChecksum() const;

private:
    std::string m_sLearnedValue;
    bool m_bIsLearned;
    uint32_t m_iNewChecksum;

    Config * m_poConfig;
    PaxosLog m_oPaxosLog;
};

///////////////////////////////////////////////////////


class Acceptor;
class CheckpointMgr;
class SMFac;

class Learner : public Base
{
public:
    Learner(
            const Config * poConfig, 
            const MsgTransport * poMsgTransport,
            const Instance * poInstance,
            const Acceptor * poAcceptor,
            const LogStorage * poLogStorage,
            const IOLoop * poIOLoop,
            const CheckpointMgr * poCheckpointMgr,
            const SMFac * poSMFac);
    virtual ~Learner();

    void StartLearnerSender();

    virtual void InitForNewPaxosInstance();

    const bool IsLearned();

    const std::string & GetLearnValue();

    const uint32_t GetNewChecksum() const;

    void Stop();

    //prepare learn
    void AskforLearn();

    void OnAskforLearn(const PaxosMsg & oPaxosMsg);

    void SendNowInstanceID(const uint64_t llInstanceID, const nodeid_t iSendNode);

    void OnSendNowInstanceID(const PaxosMsg & oPaxosMsg);

    void AskforCheckpoint(const nodeid_t iSendNodeID);

    void OnAskforCheckpoint(const PaxosMsg & oPaxosMsg);

    //comfirm learn
    void ComfirmAskForLearn(const nodeid_t iSendNodeID);

    void OnComfirmAskForLearn(const PaxosMsg & oPaxosMsg);
    
    int SendLearnValue(
            const nodeid_t iSendNodeID, 
            const uint64_t llLearnInstanceID, 
            const BallotNumber & oLearnedBallot,
            const std::string & sLearnedValue,
            const uint32_t iChecksum,
            const bool bNeedAck = true);

    void OnSendLearnValue(const PaxosMsg & oPaxosMsg);

    void SendLearnValue_Ack(const nodeid_t iSendNodeID);

    void OnSendLearnValue_Ack(const PaxosMsg & oPaxosMsg);

    //success learn
    virtual void ProposerSendSuccess(
            const uint64_t llLearnInstanceID,
            const uint64_t llProposalID);

    void OnProposerSendSuccess(const PaxosMsg & oPaxosMsg);

    void TransmitToFollower();

    //learn noop
    void AskforLearn_Noop(const bool bIsStart = false);

    void Reset_AskforLearn_Noop(const int iTimeout = ASKFORLEARN_NOOP_INTERVAL);

    //checkpoint logic
    int SendCheckpointBegin(
            const nodeid_t iSendNodeID,
            const uint64_t llUUID,
            const uint64_t llSequence,
            const uint64_t llCheckpointInstanceID);
    
    int SendCheckpoint(
            const nodeid_t iSendNodeID,
            const uint64_t llUUID,
            const uint64_t llSequence,
            const uint64_t llCheckpointInstanceID,
            const uint32_t iChecksum,
            const std::string & sFilePath,
            const int iSMID,
            const uint64_t llOffset,
            const std::string & sBuffer);
    
    int SendCheckpointEnd(
            const nodeid_t iSendNodeID,
            const uint64_t llUUID,
            const uint64_t llSequence,
            const uint64_t llCheckpointInstanceID);

    void OnSendCheckpoint(const CheckpointMsg & oCheckpointMsg);

    int SendCheckpointAck(
            const nodeid_t iSendNodeID,
            const uint64_t llUUID,
            const uint64_t llSequence,
            const int iFlag);

    void OnSendCheckpointAck(const CheckpointMsg & oCheckpointMsg);

    CheckpointSender * GetNewCheckpointSender(const nodeid_t iSendNodeID);
    
    ///////////////////

    const bool IsIMLatest();

    const uint64_t GetSeenLatestInstanceID();

    void SetSeenInstanceID(const uint64_t llInstanceID, const nodeid_t llFromNodeID);

private:
    int OnSendCheckpoint_Begin(const CheckpointMsg & oCheckpointMsg);
    int OnSendCheckpoint_Ing(const CheckpointMsg & oCheckpointMsg);
    int OnSendCheckpoint_End(const CheckpointMsg & oCheckpointMsg);

private:
    LearnerState m_oLearnerState;

    Acceptor * m_poAcceptor;
    PaxosLog m_oPaxosLog;

    uint32_t m_iAskforlearn_noopTimerID;
    IOLoop * m_poIOLoop;

    uint64_t m_llHighestSeenInstanceID;
    nodeid_t m_iHighestSeenInstanceID_FromNodeID;

    bool m_bIsIMLearning;
    LearnerSender m_oLearnerSender;
    uint64_t m_llLastAckInstanceID;

    CheckpointMgr * m_poCheckpointMgr;
    SMFac * m_poSMFac;

    CheckpointSender * m_poCheckpointSender;
    CheckpointReceiver m_oCheckpointReceiver;
};

}

