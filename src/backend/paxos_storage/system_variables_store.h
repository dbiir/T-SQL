#pragma once

#include <string>
#include <vector>
#include <inttypes.h>
#include "storage.h"
#include "paxos_msg.pb.h"

namespace phxpaxos
{

class SystemVariablesStore
{
public:
    SystemVariablesStore(const LogStorage * poLogStorage);
    ~SystemVariablesStore();

    int Write(const WriteOptions & oWriteOptions,  const int iGroupIdx, const SystemVariables & oVariables);

    int Read(const int iGroupIdx, SystemVariables & oVariables);

private:
    LogStorage * m_poLogStorage;
};

}
