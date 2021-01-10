#pragma once

#include "event_base.h"
#include <string>

namespace phxpaxos
{

class EventLoop;

class Notify : public Event
{
public:
    Notify(EventLoop * poEventLoop);
    ~Notify();

    int Init();

    int GetSocketFd() const;
    
    const std::string & GetSocketHost();

    void SendNotify();

    int OnRead();

    void OnError(bool & bNeedDelete);

private:
    int m_iPipeFD[2];    
    std::string m_sHost;
};
    
}
