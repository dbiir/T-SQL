#pragma once

#include "base.h"
#include "acceptor.h"
#include "learner.h"
#include "proposer.h"
#include "msg_transport.h"
#include "sm.h"
#include "sm_base.h"
#include "storage.h"
#include "comm_include.h"
#include "ioloop.h"
#include "commitctx.h"
#include "committer.h"
#include "cp_mgr.h"

namespace phxpaxos
{

class Instance
{
public:
    Instance(
            const Config * poConfig, 
            const LogStorage * poLogStorage,
            const MsgTransport * poMsgTransport,
            const Options & oOptions);
    ~Instance();

    int Init();

    void Start();

    void Stop();

    int InitLastCheckSum();

    const uint64_t GetNowInstanceID();

    const uint64_t GetMinChosenInstanceID();

    const uint32_t GetLastChecksum();

    int GetInstanceValue(const uint64_t llInstanceID, std::string & sValue, int & iSMID);

public:
    Committer * GetCommitter();

    Cleaner * GetCheckpointCleaner();

    Replayer * GetCheckpointReplayer();

public:
    void CheckNewValue();

    void OnNewValueCommitTimeout();

public:
    //this funciton only enqueue, do nothing.
    int OnReceiveMessage(const char * pcMessage, const int iMessageLen);

public:
    void OnReceive(const std::string & sBuffer);
    
    void OnReceiveCheckpointMsg(const CheckpointMsg & oCheckpointMsg);

    int OnReceivePaxosMsg(const PaxosMsg & oPaxosMsg, const bool bIsRetry = false);
    
    int ReceiveMsgForProposer(const PaxosMsg & oPaxosMsg);
    
    int ReceiveMsgForAcceptor(const PaxosMsg & oPaxosMsg, const bool bIsRetry);
    
    int ReceiveMsgForLearner(const PaxosMsg & oPaxosMsg);

public:
    void OnTimeout(const uint32_t iTimerID, const int iType);

public:
    void AddStateMachine(StateMachine * poSM);

    bool SMExecute(
        const uint64_t llInstanceID, 
        const std::string & sValue, 
        const bool bIsMyCommit,
        SMCtx * poSMCtx);

private:
    void ChecksumLogic(const PaxosMsg & oPaxosMsg);

    int PlayLog(const uint64_t llBeginInstanceID, const uint64_t llEndInstanceID);

    bool ReceiveMsgHeaderCheck(const Header & oHeader, const nodeid_t iFromNodeID);

    int ProtectionLogic_IsCheckpointInstanceIDCorrect(const uint64_t llCPInstanceID, const uint64_t llLogMaxInstanceID);

private:
    void NewInstance();

private:
    Config * m_poConfig;
    MsgTransport * m_poMsgTransport;

    SMFac m_oSMFac;

    IOLoop m_oIOLoop;

    Acceptor m_oAcceptor;
    Learner m_oLearner;
    Proposer m_oProposer;

    PaxosLog m_oPaxosLog;

    uint32_t m_iLastChecksum;

private:
    CommitCtx m_oCommitCtx;
    uint32_t m_iCommitTimerID;

    Committer m_oCommitter;

private:
    CheckpointMgr m_oCheckpointMgr;

private:
    TimeStat m_oTimeStat;
    Options m_oOptions;

    bool m_bStarted;
};
    
}
