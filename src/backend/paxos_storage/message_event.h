#pragma once

#include <mutex>
#include "event_base.h"
#include "utils_include.h"
#include "commdef.h"
#include "comm_include.h"

namespace phxpaxos
{

class EventLoop;
class NetWork;

enum MessageEventType
{
    MessageEventType_RECV = 1,
    MessageEventType_SEND = 2,
};

enum MessageEventTimerType
{
    MessageEventTimerType_Reconnect = 1,
};

class MessageEvent : public Event
{
public:
    MessageEvent(
            const int Type,
            const int fd, 
            const SocketAddress & oAddr, 
            EventLoop * poEventLoop,
            NetWork * poNetWork);
    ~MessageEvent();

    int AddMessage(const std::string & sMessage);

    int GetSocketFd() const;
    
    const std::string & GetSocketHost();

    int OnRead();

    int OnWrite();

    void OnTimeout(const uint32_t iTimerID, const int iType);

    void OnError(bool & bNeedDelete);

    void OpenWrite();

    const bool IsActive();

private:
    int ReadLeft();

    void ReadDone(BytesBuffer & oBytesBuffer, const int iLen);
    
    int WriteLeft();

    void WriteDone();

    int DoOnWrite();

    void ReConnect();

private:
    Socket m_oSocket;    
    SocketAddress m_oAddr;
    std::string m_sHost;
    NetWork * m_poNetWork;
    int m_iType;

private:
    char m_sReadHeadBuffer[sizeof(int)];
    int m_iLastReadHeadPos;
    BytesBuffer m_oReadCacheBuffer;
    int m_iLastReadPos;
    int m_iLeftReadLen;

    BytesBuffer m_oWriteCacheBuffer;
    int m_iLastWritePos;
    int m_iLeftWriteLen;

    struct QueueData
    {
        uint64_t llEnqueueAbsTime;
        std::string * psValue;
    };    

    std::queue<QueueData> m_oInQueue;
    int m_iQueueMemSize;
    std::mutex m_oMutex;

    uint64_t m_llLastActiveTime;

private:
    uint32_t m_iReconnectTimeoutID;
};

}
