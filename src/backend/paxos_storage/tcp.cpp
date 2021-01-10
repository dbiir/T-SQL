#include "tcp.h"
#include "network.h"

namespace phxpaxos
{

TcpRead :: TcpRead(NetWork * poNetWork): m_oEventLoop(poNetWork){}

TcpRead :: ~TcpRead(){}

int TcpRead :: Init()
{
    return m_oEventLoop.Init(20480);
}

void TcpRead :: run()
{
    m_oEventLoop.StartLoop();
}

void TcpRead :: Stop()
{
    m_oEventLoop.Stop();
    join();

    PLHead("TcpReadThread [END]");
}

EventLoop * TcpRead :: GetEventLoop()
{
    return &m_oEventLoop;
}

TcpWrite :: TcpWrite(NetWork * poNetWork)
    : m_oTcpClient(&m_oEventLoop, poNetWork),
      m_oEventLoop(poNetWork)
{
    m_oEventLoop.SetTcpClient(&m_oTcpClient);
}

TcpWrite :: ~TcpWrite(){}

int TcpWrite :: Init()
{
    return m_oEventLoop.Init(20480);
}

void TcpWrite :: run()
{
    m_oEventLoop.StartLoop();
}

void TcpWrite :: Stop()
{
    m_oEventLoop.Stop();
    join();

    PLHead("TcpWriteThread [END]");
}

int TcpWrite :: AddMessage(const std::string & sIP, const int iPort, const std::string & sMessage)
{
    return m_oTcpClient.AddMessage(sIP, iPort, sMessage);
}

TcpIOThread :: TcpIOThread(NetWork * poNetWork): m_poNetWork(poNetWork)
{
    m_bIsStarted = false;
    assert(signal(SIGPIPE, SIG_IGN) != SIG_ERR);
    assert(signal(SIGALRM, SIG_IGN) != SIG_ERR);
    assert(signal(SIGCHLD, SIG_IGN) != SIG_ERR);
}

TcpIOThread :: ~TcpIOThread()
{
    for (auto & poTcpRead : m_vecTcpRead)
    {
        delete poTcpRead;
    }

    for (auto & poTcpWrite : m_vecTcpWrite)
    {
        delete poTcpWrite;
    }
}

void TcpIOThread :: Stop()
{
    if (m_bIsStarted)
    {
        m_oTcpAcceptor.Stop();
        for (auto & poTcpRead : m_vecTcpRead)
        {
            poTcpRead->Stop();
        }

        for (auto & poTcpWrite : m_vecTcpWrite)
        {
            poTcpWrite->Stop();
        }
    }

    PLHead("TcpIOThread [END]");
}

int TcpIOThread :: Init(const std::string & sListenIp, const int iListenPort, const int iIOThreadCount)
{
    for (int i = 0; i < iIOThreadCount; i++)
    {
        TcpRead * poTcpRead = new TcpRead(m_poNetWork);
        assert(poTcpRead != nullptr);
        m_vecTcpRead.push_back(poTcpRead);
        m_oTcpAcceptor.AddEventLoop(poTcpRead->GetEventLoop());

        TcpWrite * poTcpWrite = new TcpWrite(m_poNetWork);
        assert(poTcpWrite != nullptr);
        m_vecTcpWrite.push_back(poTcpWrite);
    }

    m_oTcpAcceptor.Listen(sListenIp, iListenPort);
    int ret = -1;

    for (auto & poTcpRead : m_vecTcpRead)
    {
        ret = poTcpRead->Init();
        if (ret != 0)
        {
            return ret;
        }
    }

    for (auto & poTcpWrite: m_vecTcpWrite)
    {
        ret = poTcpWrite->Init();
        if (ret != 0)
        {
            return ret;
        }
    }

    return 0;
}

void TcpIOThread :: Start()
{
    m_oTcpAcceptor.start();
    for (auto & poTcpWrite : m_vecTcpWrite)
    {
        poTcpWrite->start();
    }

    for (auto & poTcpRead : m_vecTcpRead)
    {
        poTcpRead->start();
    }

    m_bIsStarted = true;
}

int TcpIOThread :: AddMessage(const int iGroupIdx, const std::string & sIP, const int iPort, const std::string & sMessage)
{
    int iIndex = iGroupIdx % (int)m_vecTcpWrite.size();
    return m_vecTcpWrite[iIndex]->AddMessage(sIP, iPort, sMessage);
}

}
