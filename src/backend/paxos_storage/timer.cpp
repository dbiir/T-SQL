#include "timer.h"
#include "util.h"
#include <algorithm> 

namespace phxpaxos
{

Timer :: Timer() : m_iNowTimerID(1){}

Timer :: ~Timer() {}

void Timer :: AddTimer(const uint64_t llAbsTime, uint32_t & iTimerID)
{
    return AddTimerWithType(llAbsTime, 0, iTimerID);
}

void Timer :: AddTimerWithType(const uint64_t llAbsTime, const int iType, uint32_t & iTimerID)
{
    iTimerID = m_iNowTimerID++;

    TimerObj tObj(iTimerID, llAbsTime, iType);
    m_vecTimerHeap.push_back(tObj);
    push_heap(begin(m_vecTimerHeap), end(m_vecTimerHeap));
}

const int Timer :: GetNextTimeout() const
{
    if (m_vecTimerHeap.empty())
    {
        return -1;
    }

    int iNextTimeout = 0;

    TimerObj tObj = m_vecTimerHeap.front();
    uint64_t llNowTime = Time::GetSteadyClockMS();
    if (tObj.m_llAbsTime > llNowTime)
    {
        iNextTimeout = (int)(tObj.m_llAbsTime - llNowTime);
    }

    return iNextTimeout;
}

bool Timer :: PopTimeout(uint32_t & iTimerID, int & iType)
{
    if (m_vecTimerHeap.empty())
    {
        return false;
    }

    TimerObj tObj = m_vecTimerHeap.front();
    uint64_t llNowTime = Time::GetSteadyClockMS();
    if (tObj.m_llAbsTime > llNowTime)
    {
        return false;
    }
    
    pop_heap(begin(m_vecTimerHeap), end(m_vecTimerHeap));
    m_vecTimerHeap.pop_back();

    iTimerID = tObj.m_iTimerID;
    iType = tObj.m_iType;

    return true;
}    
    
}
