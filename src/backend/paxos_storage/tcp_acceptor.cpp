#include <stdio.h>
#include "tcp_acceptor.h"
#include "commdef.h"
#include "event_loop.h"
#include "network.h"
#include "message_event.h"
#include "comm_include.h"
#include <poll.h>

namespace phxpaxos
{

TcpAcceptor :: TcpAcceptor()
{
    m_bIsEnd = false;
    m_bIsStarted = false;
}

TcpAcceptor :: ~TcpAcceptor(){}

void TcpAcceptor :: Listen(const std::string & sListenIP, const int iListenPort)
{
    m_oSocket.listen(SocketAddress(sListenIP, (unsigned short)iListenPort));
}

void TcpAcceptor :: Stop()
{
    if (m_bIsStarted)
    {
        m_bIsEnd = true;
        join();
    }
}

void TcpAcceptor :: run()
{
    m_bIsStarted = true;

    PLHead("start accept...");

    m_oSocket.setAcceptTimeout(500);
    m_oSocket.setNonBlocking(true);

    while (true)
    {
        struct pollfd pfd;
        int ret;

        pfd.fd =  m_oSocket.getSocketHandle();
        pfd.events = POLLIN;
        ret = poll(&pfd, 1, 500);

        if (ret != 0 && ret != -1)
        {
            SocketAddress oAddr;
            int fd = -1;
            try
            {
                fd = m_oSocket.acceptfd(&oAddr);
            }
            catch(...)
            {
                fd = -1;
            }
            
            if (fd >= 0)
            {
                BP->GetNetworkBP()->TcpAcceptFd();

                PLImp("accepted!, fd %d ip %s port %d",
                        fd, oAddr.getHost().c_str(), oAddr.getPort());

                AddEvent(fd, oAddr);
            }
        }

        if (m_bIsEnd)
        {
            PLHead("TCP.Acceptor [END]");
            return;
        }
    }
}

void TcpAcceptor :: AddEventLoop(EventLoop * poEventLoop)
{
    m_vecEventLoop.push_back(poEventLoop);
}

void TcpAcceptor :: AddEvent(int iFD, SocketAddress oAddr)
{
    EventLoop * poMinActiveEventLoop = nullptr;
    int iMinActiveEventCount = 1 << 30;

    for (auto & poEventLoop : m_vecEventLoop)
    {
        int iActiveCount = poEventLoop->GetActiveEventCount();
        if (iActiveCount < iMinActiveEventCount)
        {
            iMinActiveEventCount = iActiveCount;
            poMinActiveEventLoop = poEventLoop;
        }
    }

    poMinActiveEventLoop->AddEvent(iFD, oAddr);
}
    
}
