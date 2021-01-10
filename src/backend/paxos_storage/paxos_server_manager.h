#include "echo_server.h"
#include <stdio.h>
#include <iostream>
#include <string>
#include <vector>
#include <string.h>
#include "options.h"
#include <map>

using namespace phxecho;
using namespace phxpaxos;

int Init(const std::string &ip, int port);

int AddGroup(int iGroupIndex, char *MyIPPort, char *NodeIPPortList);

int AddGroupMember(int iGroupIndex, char *IPPort);

int RemoveGroup(int iGroupIndex);

int RemoveGroupMember(int iGroupIndex, char *IPPort);

int echo(int iGroupIndex, const std::string &sEchoReqValue, std::string &sEchoRespValue);
