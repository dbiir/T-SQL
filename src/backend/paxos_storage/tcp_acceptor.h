#pragma once

#include <vector>
#include <mutex>
#include "utils_include.h"
#include "message_event.h"

namespace phxpaxos
{

class EventLoop;
class NetWork;

class TcpAcceptor : public Thread
{
public:
    TcpAcceptor();
    ~TcpAcceptor();

    void Listen(const std::string & sListenIP, const int iListenPort);

    void run();

    void Stop();

    void AddEventLoop(EventLoop * poEventLoop);

    void AddEvent(int iFD, SocketAddress oAddr);

private:
    ServerSocket m_oSocket;
    std::vector<EventLoop *> m_vecEventLoop;

private:
    bool m_bIsEnd;
    bool m_bIsStarted;
};
    
}
