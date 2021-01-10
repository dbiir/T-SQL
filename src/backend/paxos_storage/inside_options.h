#pragma once

#include <inttypes.h>
#include <typeinfo>

namespace phxpaxos
{

#define MAX_VALUE_SIZE (InsideOptions::Instance()->GetMaxBufferSize())
#define START_PREPARE_TIMEOUTMS (InsideOptions::Instance()->GetStartPrepareTimeoutMs())
#define START_ACCEPT_TIMEOUTMS (InsideOptions::Instance()->GetStartAcceptTimeoutMs())
#define MAX_PREPARE_TIMEOUTMS (InsideOptions::Instance()->GetMaxPrepareTimeoutMs())
#define MAX_ACCEPT_TIMEOUTMS (InsideOptions::Instance()->GetMaxAcceptTimeoutMs())
#define QUEUE_MAXLENGTH (InsideOptions::Instance()->GetMaxIOLoopQueueLen())
#define ASKFORLEARN_NOOP_INTERVAL (InsideOptions::Instance()->GetAskforLearnInterval())
#define LearnerSender_PREPARE_TIMEOUT (InsideOptions::Instance()->GetLearnerSenderPrepareTimeoutMs())
#define LearnerSender_ACK_TIMEOUT (InsideOptions::Instance()->GetLearnerSender_Ack_TimeoutMs())
#define LearnerSender_ACK_LEAD (InsideOptions::Instance()->GetLearnerSender_Ack_Lead())
#define LearnerReceiver_ACK_LEAD (InsideOptions::Instance()->GetLearnerReceiver_Ack_Lead())
#define TCP_QUEUE_MAXLEN (InsideOptions::Instance()->GetMaxQueueLen())
#define UDP_QUEUE_MAXLEN (InsideOptions::Instance()->GetMaxQueueLen())
#define TCP_OUTQUEUE_DROP_TIMEMS (InsideOptions::Instance()->GetTcpOutQueueDropTimeMs())
#define LOG_FILE_MAX_SIZE (InsideOptions::Instance()->GetLogFileMaxSize())
#define CONNECTTION_NONACTIVE_TIMEOUT (InsideOptions::Instance()->GetTcpConnectionNonActiveTimeout())
#define LearnerSender_SEND_QPS (InsideOptions::Instance()->GetLearnerSenderSendQps())
#define Cleaner_DELETE_QPS (InsideOptions::Instance()->GetCleanerDeleteQps())

class InsideOptions
{
public:
    InsideOptions();
    ~InsideOptions();

    static InsideOptions * Instance();

    void SetAsLargeBufferMode();

    void SetAsFollower();

    void SetGroupCount(const int iGroupCount);

public:
    const int GetMaxBufferSize();

    const int GetStartPrepareTimeoutMs();

    const int GetStartAcceptTimeoutMs();

    const int GetMaxPrepareTimeoutMs();

    const int GetMaxAcceptTimeoutMs();

    const int GetMaxIOLoopQueueLen();

    const int GetMaxQueueLen();

    const int GetAskforLearnInterval();

    const int GetLearnerReceiver_Ack_Lead();

    const int GetLearnerSenderPrepareTimeoutMs();

    const int GetLearnerSender_Ack_TimeoutMs();

    const int GetLearnerSender_Ack_Lead();

    const int GetTcpOutQueueDropTimeMs();

    const int GetLogFileMaxSize();

    const int GetTcpConnectionNonActiveTimeout();

    const int GetLearnerSenderSendQps();

    const int GetCleanerDeleteQps();

private:
    bool m_bIsLargeBufferMode;
    bool m_bIsIMFollower;
    int m_iGroupCount;
};
    
}
