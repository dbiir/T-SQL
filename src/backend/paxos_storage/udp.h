#pragma once

#include "utils_include.h"

namespace phxpaxos 
{

class DFNetWork;

class UDPRecv : public Thread
{
public:
    UDPRecv(DFNetWork * poDFNetWork);
    ~UDPRecv();

    int Init(const int iPort);

    void run();

    void Stop();
    
private:
    DFNetWork * m_poDFNetWork;
    int m_iSockFD;
    bool m_bIsEnd;
    bool m_bIsStarted;
};

class UDPSend : public Thread
{
public:
    UDPSend();
    ~UDPSend();

    int Init();

    void run();

    void Stop();

    int AddMessage(const std::string & sIP, const int iPort, const std::string & sMessage);

    struct QueueData
    {
        std::string m_sIP;
        int m_iPort;
        std::string m_sMessage;
    };

private:
    void SendMessage(const std::string & sIP, const int iPort, const std::string & sMessage);

private:
    Queue<QueueData *> m_oSendQueue;
    int m_iSockFD;
    bool m_bIsEnd;
    bool m_bIsStarted;
};
    
}
