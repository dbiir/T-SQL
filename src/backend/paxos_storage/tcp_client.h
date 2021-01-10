#pragma once

#include <string>
#include <map>
#include <mutex>
#include "message_event.h"
#include "utils_include.h"

namespace phxpaxos
{

class EventLoop;
class NetWork;

class TcpClient
{
public:
    TcpClient(
        EventLoop * poEventLoop,
        NetWork * poNetWork);
    ~TcpClient();

    int AddMessage(const std::string & sIP, const int iPort, const std::string & sMessage);

    void DealWithWrite();

private:
    MessageEvent * GetEvent(const std::string & sIP, const int iPort);
    
    MessageEvent * CreateEvent(const uint64_t llNodeID, const std::string & sIP, const int iPort);

    EventLoop * m_poEventLoop;
    NetWork * m_poNetWork;
    std::map<uint64_t, MessageEvent *> m_mapEvent;
    std::vector<MessageEvent *> m_vecEvent;
    std::mutex m_oMutex;
};
    
}
