#include "dfnetwork.h"
#include "udp.h"

namespace phxpaxos 
{

DFNetWork :: DFNetWork() : m_oUDPRecv(this), m_oTcpIOThread(this)
{
}

DFNetWork :: ~DFNetWork()
{
    PLHead("NetWork Deleted!");
}

void DFNetWork :: StopNetWork()
{
    m_oUDPRecv.Stop();
    m_oUDPSend.Stop();
    m_oTcpIOThread.Stop();
}

int DFNetWork :: Init(const std::string & sListenIp, const int iListenPort, const int iIOThreadCount) 
{
    int ret = m_oUDPSend.Init();
    if (ret != 0)
    {
        return ret;
    }

    ret = m_oUDPRecv.Init(iListenPort);
    if (ret != 0)
    {
        return ret;
    }

    ret = m_oTcpIOThread.Init(sListenIp, iListenPort, iIOThreadCount);
    if (ret != 0)
    {
        PLErr("m_oTcpIOThread Init fail, ret %d", ret);
        return ret;
    }

    return 0;
}

void DFNetWork :: RunNetWork()
{
    m_oUDPSend.start();
    m_oUDPRecv.start();
    m_oTcpIOThread.Start();
}

int DFNetWork :: SendMessageTCP(const int iGroupIdx, const std::string & sIp, const int iPort, const std::string & sMessage)
{
    return m_oTcpIOThread.AddMessage(iGroupIdx, sIp, iPort, sMessage);
}

int DFNetWork :: SendMessageUDP(const int iGroupIdx, const std::string & sIp, const int iPort, const std::string & sMessage)
{
    return m_oUDPSend.AddMessage(sIp, iPort, sMessage);
}

}


