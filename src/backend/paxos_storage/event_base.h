#pragma once

#include "commdef.h"

namespace phxpaxos
{

class EventLoop;

class Event
{
public:
    Event(EventLoop * poEventLoop);
    virtual ~Event();

    virtual int GetSocketFd() const = 0;

    virtual const std::string & GetSocketHost() = 0;

    virtual int OnRead();

    virtual int OnWrite();

    virtual void OnError(bool & bNeedDelete) = 0;

    virtual void OnTimeout(const uint32_t iTimerID, const int iType);

public:
    void AddEvent(const int iEvents);
    
    void RemoveEvent(const int iEvents);

    void JumpoutEpollWait();

    const bool IsDestroy() const;

    void Destroy();

public:
    void AddTimer(const int iTimeoutMs, const int iType, uint32_t & iTimerID);

    void RemoveTimer(const uint32_t iTimerID);

protected:
    int m_iEvents;
    EventLoop * m_poEventLoop;

    bool m_bIsDestroy;
};
    
}
