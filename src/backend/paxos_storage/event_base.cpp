#include "event_base.h"
#include "event_loop.h"

namespace phxpaxos
{

Event :: Event(EventLoop * poEventLoop) : 
    m_iEvents(0), m_poEventLoop(poEventLoop)
{
    m_bIsDestroy = false;
}

Event :: ~Event()
{
}

int Event :: OnRead()
{
    PLErr("Need Impl");
    return -1;
}

int Event :: OnWrite()
{
    PLErr("Need Impl");
    return -1;
}

void Event :: OnTimeout(const uint32_t iTimerID, const int iType)
{
    PLErr("Need Impl");
}

void Event :: JumpoutEpollWait()
{
    m_poEventLoop->JumpoutEpollWait();
}

void Event :: AddEvent(const int iEvents)
{
    int iBeforeEvent = m_iEvents;
    m_iEvents |= iEvents; 
    if (m_iEvents == iBeforeEvent)
    {
        return;
    }
    
    m_poEventLoop->ModEvent(this, m_iEvents);
}

void Event :: RemoveEvent(const int iEvents)
{
    int iBeforeEvent = m_iEvents;
    m_iEvents &= (~iEvents);
    if (m_iEvents == iBeforeEvent)
    {
        return;
    }
    
    if (m_iEvents == 0)
    {
        m_poEventLoop->RemoveEvent(this);
    }
    else
    {
        m_poEventLoop->ModEvent(this, m_iEvents);
    }
}

void Event :: AddTimer(const int iTimeoutMs, const int iType, uint32_t & iTimerID)
{
    m_poEventLoop->AddTimer(this, iTimeoutMs, iType, iTimerID);
}

void Event :: RemoveTimer(const uint32_t iTimerID)
{
    m_poEventLoop->RemoveTimer(iTimerID);
}

void Event :: Destroy()
{
    m_bIsDestroy = true;
}

const bool Event :: IsDestroy() const
{
    return m_bIsDestroy;
}

}


