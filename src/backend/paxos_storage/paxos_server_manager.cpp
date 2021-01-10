#include "echo_server.h"
#include <stdio.h>
#include <iostream>
#include <string>
#include <vector>
#include <string.h>
#include "options.h"
#include "dfnetwork.h"
#include "paxos/paxos_for_cpp_include.h"

using namespace phxecho;
using namespace phxpaxos;
using namespace std;

#include "paxos_server_manager.h"

std::map<int, PhxEchoServer *> server_manager;
DFNetWork *poNetwork = nullptr;

int Init(const std::string &ip, int port) {
    poNetwork = new DFNetWork();
    if (poNetwork->Init(ip, port, 1) < 0) {
        return -1;
    }
    poNetwork->RunNetWork();
    return 0;
}

int AddGroup(int iGroupIndex, char *MyIPPort, char *NodeIPPortList) {
    if (server_manager.find(iGroupIndex) != server_manager.end()) {
        return 0;
    }
    auto *poEchoServer = new PhxEchoServer(MyIPPort, NodeIPPortList);
    int ret = poEchoServer->RunPaxos(iGroupIndex, poNetwork);
    if (ret != 0) {
        return -1;
    }
    server_manager[iGroupIndex] = poEchoServer;
    return 0;
}

int echo(int iGroupIndex, const std::string &sEchoReqValue, std::string &sEchoRespValue) {
    return server_manager[iGroupIndex]->Echo(sEchoReqValue, sEchoRespValue);
}

int RemoveGroup(int iGroupIndex) {
    if (server_manager.find(iGroupIndex) == server_manager.end()) {
        return 0;
    }
    delete server_manager[iGroupIndex];
    server_manager.erase(iGroupIndex);
    return 0;
}

int paxos_storage_init(char *ip, int iPort) {
    return Init(ip, iPort);
}

int paxos_storage_runpaxos(char *pString, int iStringLen, unsigned int iGroupIndex) {
    if (server_manager.find(iGroupIndex) == server_manager.end()) {
        return -1;
    }
    string s;
    //printf("%d start echo.\n", iGroupIndex);
    return echo((int) iGroupIndex, std::string(pString, iStringLen), s);
}

std::map<uint32_t, BatchPaxosValues> m_mapWriteBatch;

int paxos_storage_save_to_batch(char *pString, int iStringLen, unsigned int iGroupIndex, unsigned int tid) {
    if (m_mapWriteBatch.find(tid) == m_mapWriteBatch.end()) {
        m_mapWriteBatch[tid] = BatchPaxosValues();
    }
    PaxosValue *pv = m_mapWriteBatch[tid].add_values();
    pv->set_value(std::string(pString, iStringLen));
    pv->set_smid(1);
    pv->set_groupid(iGroupIndex);
    return 0;
}

int paxos_storage_commit_batch(unsigned int tid) {
    if (m_mapWriteBatch.find(tid) == m_mapWriteBatch.end()) {
        return -1;
    }
    int ret;
    string s;
    for (int i = 0; i < m_mapWriteBatch[tid].values_size(); i++) {
        const PaxosValue &oValue = m_mapWriteBatch[tid].values(i);
        ret = server_manager[oValue.groupid()]->Echo(oValue.value(), s);
        if (ret)
            return -1;
    }
    return 0;
}

int paxos_storage_process_create_group_req(unsigned int iGroupIndex, char *MyIPPort, char *NodeIPPortList) {
    if (server_manager.find(iGroupIndex) != server_manager.end()) {
        return 0;
    }
    return AddGroup((int) iGroupIndex, MyIPPort, NodeIPPortList);
}

int paxos_storage_process_remove_group_req(unsigned int iGroupIndex) {
    if (server_manager.find(iGroupIndex) == server_manager.end()) {
        return 0;
    }
    delete server_manager[iGroupIndex];
    server_manager.erase(iGroupIndex);
    poNetwork->removenode(iGroupIndex);
    return 0;
}

int paxos_storage_process_add_group_member_req(unsigned int iGroupIndex, char *NodeIPPortToAdd) {
    if (server_manager.find(iGroupIndex) == server_manager.end()) {
        return -1;
    }
    return server_manager[iGroupIndex]->AddMember(NodeIPPortToAdd);
}

int paxos_storage_process_remove_group_member_req(unsigned int iGroupIndex, char *NodeIPPortToRemove) {
    if (server_manager.find(iGroupIndex) == server_manager.end()) {
        return -1;
    }
    return server_manager[iGroupIndex]->RemoveMember(NodeIPPortToRemove);
}
