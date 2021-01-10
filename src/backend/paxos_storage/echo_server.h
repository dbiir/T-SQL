#pragma once

#include "node.h"
#include "echo_sm.h"
#include <string>
#include <vector>
#include "dfnetwork.h"
#include "options.h"

int parse_ipport(const char *pcStr, phxpaxos::NodeInfo &oNodeInfo);
int parse_ipport_list(const char *pcStr, phxpaxos::NodeInfoList &vecNodeInfoList);

namespace phxecho
{

class PhxEchoServer
{
public:
    PhxEchoServer(char *MyIPPort, char *NodeIPPortList);
    ~PhxEchoServer();

    int RunPaxos(int iGroupIndex, phxpaxos::DFNetWork *poNetwork);

    int Echo(const std::string & sEchoReqValue, std::string & sEchoRespValue);

    int AddMember(char *IPPort);

    int RemoveMember(char *IPPort);

private:
    int MakeLogStoragePath(int iGroupIndex, std::string & sLogStoragePath);

private:
    phxpaxos::NodeInfo m_oMyNode;
    phxpaxos::NodeInfoList m_vecNodeList;

    phxpaxos::Node * m_poPaxosNode;
    PhxEchoSM m_oEchoSM;
    int m_groupindex;
};
    
}


