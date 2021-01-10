#pragma once

#include <string>
#include "udp.h"
#include "tcp.h"
#include "network.h"

namespace phxpaxos 
{

class DFNetWork : public NetWork
{
public:
    DFNetWork();
    virtual ~DFNetWork();

    int Init(const std::string & sListenIp, const int iListenPort, const int iIOThreadCount);

    void RunNetWork();

    void StopNetWork();

    int SendMessageTCP(const int iGroupIdx, const std::string & sIp, const int iPort, const std::string & sMessage);

    int SendMessageUDP(const int iGroupIdx, const std::string & sIp, const int iPort, const std::string & sMessage);

private:
    UDPRecv m_oUDPRecv;
    UDPSend m_oUDPSend;
    TcpIOThread m_oTcpIOThread;
};

}
