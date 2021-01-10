#include "echo_server.h"
#include <assert.h>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace phxpaxos;
using namespace std;

int parse_ipport(const char *pcStr, NodeInfo &oNodeInfo) {
    char sIP[32] = {0};
    int iPort = -1;

    int count = sscanf(pcStr, "%[^':']:%d", sIP, &iPort);
    if (count != 2) {
        return -1;
    }

    oNodeInfo.SetIPPort(sIP, iPort);

    return 0;
}

int parse_ipport_list(const char *pcStr, NodeInfoList &vecNodeInfoList) {
    string sTmpStr;
    int iStrLen = strlen(pcStr);

    for (int i = 0; i < iStrLen; i++) {
        if (pcStr[i] == ',' || i == iStrLen - 1) {
            if (i == iStrLen - 1 && pcStr[i] != ',') {
                sTmpStr += pcStr[i];
            }

            NodeInfo oNodeInfo;
            int ret = parse_ipport(sTmpStr.c_str(), oNodeInfo);
            if (ret != 0) {
                return ret;
            }

            vecNodeInfoList.push_back(oNodeInfo);

            sTmpStr = "";
        } else {
            sTmpStr += pcStr[i];
        }
    }

    return 0;
}

namespace phxecho {

    PhxEchoServer::PhxEchoServer(char *MyIPPort, char *NodeIPPortList) : m_poPaxosNode(nullptr) {
        parse_ipport(MyIPPort, m_oMyNode);
        parse_ipport_list(NodeIPPortList, m_vecNodeList);
    }

    PhxEchoServer::~PhxEchoServer() {
        delete m_poPaxosNode;
    }

    int PhxEchoServer::MakeLogStoragePath(int iGroupIndex, std::string &sLogStoragePath) {
        char sTmp[128] = {0};
        snprintf(sTmp, sizeof(sTmp), "./logpath_%s_%d_%d", m_oMyNode.GetIP().c_str(), m_oMyNode.GetPort(), iGroupIndex);

        sLogStoragePath = string(sTmp);

        if (access(sLogStoragePath.c_str(), F_OK) == -1) {
            if (mkdir(sLogStoragePath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1) {
                printf("Create dir fail, path %s\n", sLogStoragePath.c_str());
                return -1;
            }
        }

        return 0;
    }

    int PhxEchoServer::RunPaxos(int iGroupIndex, phxpaxos::DFNetWork *poNetwork) {
        Options oOptions;

        int ret = MakeLogStoragePath(iGroupIndex, oOptions.sLogStoragePath);
        if (ret != 0) {
            return ret;
        }

        //this groupcount means run paxos group count.
        //every paxos group is independent, there are no any communicate between any 2 paxos group.
        oOptions.iGroupCount = 1;

        oOptions.oMyNode = m_oMyNode;
        oOptions.vecNodeInfoList = m_vecNodeList;
        oOptions.poNetWork = poNetwork;
        m_groupindex = iGroupIndex;

        GroupSMInfo oSMInfo;
        oSMInfo.iGroupIdx = iGroupIndex;
        //one paxos group can have multi state machine.
        oSMInfo.vecSMList.push_back(&m_oEchoSM);
        oOptions.vecGroupSMInfoList.push_back(oSMInfo);

        ret = Node::RunNode(iGroupIndex, oOptions, m_poPaxosNode);
        if (ret != 0) {
            printf("run paxos fail, ret %d\n", ret);
            return ret;
        }

        printf("run paxos ok\n");
        return 0;
    }

    int PhxEchoServer::Echo(const std::string &sEchoReqValue, std::string &sEchoRespValue) {
        SMCtx oCtx;
        PhxEchoSMCtx oEchoSMCtx;
        //smid must same to PhxEchoSM.SMID().
        oCtx.m_iSMID = 1;
        oCtx.m_pCtx = (void *) &oEchoSMCtx;

        uint64_t llInstanceID = 0;
        //printf("start propose\n");
        int ret = m_poPaxosNode->Propose(m_groupindex, sEchoReqValue, llInstanceID, &oCtx);
        if (ret != 0) {
            printf("paxos propose fail, ret %d\n", ret);
            return ret;
        }

        sEchoRespValue = oEchoSMCtx.sEchoRespValue;

        return 0;
    }

    int PhxEchoServer::AddMember(char *IPPort) {
        NodeInfo otempnode;
        if (parse_ipport(IPPort, otempnode) < 0) {
            return -1;
        }
        return m_poPaxosNode->AddMember(m_groupindex, otempnode);
    }

    int PhxEchoServer::RemoveMember(char *IPPort) {
        NodeInfo otempnode;
        if (parse_ipport(IPPort, otempnode) < 0) {
            return -1;
        }
        return m_poPaxosNode->RemoveMember(m_groupindex, otempnode);
    }

}

