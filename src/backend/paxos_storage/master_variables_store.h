#pragma once

#include <string>
#include <vector>
#include <inttypes.h>
#include "storage.h"
#include "paxos_msg.pb.h"

namespace phxpaxos
{

class MasterVariablesStore
{
public:
    MasterVariablesStore(const LogStorage * poLogStorage);
    ~MasterVariablesStore();

    int Write(const WriteOptions & oWriteOptions,  const int iGroupIdx, const MasterVariables & oVariables);

    int Read(const int iGroupIdx, MasterVariables & oVariables);

private:
    LogStorage * m_poLogStorage;
};

}
