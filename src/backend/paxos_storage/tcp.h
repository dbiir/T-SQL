#pragma once

#include "event_loop.h"
#include "tcp_acceptor.h"
#include "tcp_client.h"
#include "utils_include.h"

namespace phxpaxos
{

class TcpRead : public Thread
{
public:
    TcpRead(NetWork * poNetWork);
    ~TcpRead();

    int Init();

    void run();

    void Stop();

    EventLoop * GetEventLoop();

private:
    EventLoop m_oEventLoop;
};

class TcpWrite : public Thread
{
public:
    TcpWrite(NetWork * poNetWork);
    ~TcpWrite();

    int Init();

    void run();

    void Stop();

    int AddMessage(const std::string & sIP, const int iPort, const std::string & sMessage);

private:
    TcpClient m_oTcpClient;
    EventLoop m_oEventLoop;
};

class TcpIOThread 
{
public:
    TcpIOThread(NetWork * poNetWork);
    ~TcpIOThread();

    int Init(const std::string & sListenIp, const int iListenPort, const int iIOThreadCount);

    void Start();

    void Stop();

    int AddMessage(const int iGroupIdx, const std::string & sIP, const int iPort, const std::string & sMessage);

private:
    NetWork * m_poNetWork;
    TcpAcceptor m_oTcpAcceptor;
    std::vector<TcpRead *> m_vecTcpRead;
    std::vector<TcpWrite *> m_vecTcpWrite;
    bool m_bIsStarted;
};
    
}
