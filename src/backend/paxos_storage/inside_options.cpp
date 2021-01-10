#include "inside_options.h"
#include "commdef.h"
#include "utils_include.h"

namespace phxpaxos
{

InsideOptions :: InsideOptions()
{
    m_bIsLargeBufferMode = false;
    m_bIsIMFollower = false;
    m_iGroupCount = 1;
}

InsideOptions :: ~InsideOptions()
{
}

InsideOptions * InsideOptions :: Instance()
{
    static InsideOptions oInsideOptions;
    return &oInsideOptions;
}

void InsideOptions :: SetAsLargeBufferMode()
{
    m_bIsLargeBufferMode = true;
}

void InsideOptions :: SetAsFollower()
{
    m_bIsIMFollower = true;
}

void InsideOptions :: SetGroupCount(const int iGroupCount)
{
    m_iGroupCount = iGroupCount;
}

const int InsideOptions :: GetMaxBufferSize()
{
    if (m_bIsLargeBufferMode)
    {
        return 52428800;
    }
    else
    {
        return 10485760;
    }
}

const int InsideOptions :: GetStartPrepareTimeoutMs()
{
    if (m_bIsLargeBufferMode)
    {
        return 15000;
    }
    else
    {
        return 2000;
    }
}

const int InsideOptions :: GetStartAcceptTimeoutMs()
{
    if (m_bIsLargeBufferMode)
    {
        return 15000;
    }
    else
    {
        return 1000;
    }
}

const int InsideOptions :: GetMaxPrepareTimeoutMs()
{
    if (m_bIsLargeBufferMode)
    {
        return 90000;
    }
    else
    {
        return 8000;
    }
}

const int InsideOptions :: GetMaxAcceptTimeoutMs()
{
    if (m_bIsLargeBufferMode)
    {
        return 90000;
    }
    else
    {
        return 8000;
    }
}

const int InsideOptions :: GetMaxIOLoopQueueLen()
{
    if (m_bIsLargeBufferMode)
    {
        return 1024 / m_iGroupCount + 100;
    }
    else
    {
        return 10240 / m_iGroupCount + 1000;
    }
}

const int InsideOptions :: GetMaxQueueLen()
{
    if (m_bIsLargeBufferMode)
    {
        return 1024;
    }
    else
    {
        return 10240;
    }
}

const int InsideOptions :: GetAskforLearnInterval()
{
    if (!m_bIsIMFollower)
    {
        if (m_bIsLargeBufferMode)
        {
            return 50000 + (OtherUtils::FastRand() % 10000);
        }
        else
        {
            return 2500 + (OtherUtils::FastRand() % 500);
        }
    }
    else
    {
        if (m_bIsLargeBufferMode)
        {
            return 30000 + (OtherUtils::FastRand() % 15000);
        }
        else
        {
            return 2000 + (OtherUtils::FastRand() % 1000);
        }
    }
}

const int InsideOptions :: GetLearnerReceiver_Ack_Lead()
{
    if (m_bIsLargeBufferMode)
    {
        return 2;
    }
    else
    {
        return 4;
    }
}

const int InsideOptions :: GetLearnerSenderPrepareTimeoutMs()
{
    if (m_bIsLargeBufferMode)
    {
        return 6000;
    }
    else
    {
        return 5000;
    }
}

const int InsideOptions :: GetLearnerSender_Ack_TimeoutMs()
{
    if (m_bIsLargeBufferMode)
    {
        return 60000;
    }
    else
    {
        return 5000;
    }
}

const int InsideOptions :: GetLearnerSender_Ack_Lead()
{
    if (m_bIsLargeBufferMode)
    {
        return 5;
    }
    else
    {
        return 21;
    }
}

const int InsideOptions :: GetTcpOutQueueDropTimeMs()
{
    if (m_bIsLargeBufferMode)
    {
        return 20000;
    }
    else
    {
        return 5000;
    }
}

const int InsideOptions :: GetLogFileMaxSize()
{
    if (m_bIsLargeBufferMode)
    {
        return 524288000;
    }
    else
    {
        return 104857600;
    }
}

const int InsideOptions :: GetTcpConnectionNonActiveTimeout()
{
    if (m_bIsLargeBufferMode)
    {
        return 600000;
    }
    else
    {
        return 60000;
    }
}

const int InsideOptions :: GetLearnerSenderSendQps()
{
    if (m_bIsLargeBufferMode)
    {
        return 10000 / m_iGroupCount;
    }
    else
    {
        return 100000 / m_iGroupCount;
    }
}

const int InsideOptions :: GetCleanerDeleteQps()
{
    if (m_bIsLargeBufferMode)
    {
        return 30000 / m_iGroupCount;
    }
    else
    {
        return 300000 / m_iGroupCount;
    }
}

}


