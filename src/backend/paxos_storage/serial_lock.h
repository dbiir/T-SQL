#pragma once

#include <mutex>
#include <condition_variable>

namespace phxpaxos
{

class SerialLock
{
public:
    SerialLock();
    ~SerialLock();

    void Lock();
    void UnLock();

    void Wait();
    void Interupt();

    bool WaitTime(const int iTimeMs);

private:
    std::mutex m_oMutex;
    std::condition_variable_any m_oCond;
};

}
