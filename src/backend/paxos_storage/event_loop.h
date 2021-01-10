#pragma once

#include <map>
#include <sys/epoll.h>
#include "timer.h"
#include "notify.h"

namespace phxpaxos
{

#define MAX_EVENTS 1024

class Event;
class TcpAcceptor;
class TcpClient;
class MessageEvent;
class NetWork;

class EventLoop
{
public:
    EventLoop(NetWork * poNetWork);
    virtual ~EventLoop();

    int Init(const int iEpollLength);

    void ModEvent(const Event * poEvent, const int iEvents);

    void RemoveEvent(const Event * poEvent);

    void StartLoop();

    void Stop();

    void OnError(const int iEvents, Event * poEvent);

    virtual void OneLoop(const int iTimeoutMs);

public:
    void SetTcpClient(TcpClient * poTcpClient);

    void JumpoutEpollWait();

public:
    bool AddTimer(const Event * poEvent, const int iTimeout, const int iType, uint32_t & iTimerID);

    void RemoveTimer(const uint32_t iTimerID);

    void DealwithTimeout(int & iNextTimeout);

    void DealwithTimeoutOne(const uint32_t iTimerID, const int iType);

public:
    void AddEvent(int iFD, SocketAddress oAddr);

    void CreateEvent();

    void ClearEvent();

    int GetActiveEventCount();

public:
    typedef struct EventCtx
    {
        Event * m_poEvent;
        int m_iEvents;
    } EventCtx_t;

private:
    bool m_bIsEnd;

protected:
    int m_iEpollFd;
    epoll_event m_EpollEvents[MAX_EVENTS];
    std::map<int, EventCtx_t> m_mapEvent;
    NetWork * m_poNetWork;
    TcpClient * m_poTcpClient;
    Notify * m_poNotify;

protected:
    Timer m_oTimer;
    std::map<uint32_t, int> m_mapTimerID2FD;

    std::queue<std::pair<int, SocketAddress> > m_oFDQueue;
    std::mutex m_oMutex;
    std::vector<MessageEvent *> m_vecCreatedEvent;
};
    
}
