#pragma once

#include "sm.h"
#include <string>

namespace phxpaxos
{

class InsideSM : public StateMachine
{
public:
    virtual ~InsideSM() {}

    virtual int GetCheckpointBuffer(std::string & sCPBuffer) = 0;

    virtual int UpdateByCheckpoint(const std::string & sCPBuffer, bool & bChange) = 0;
};
    
}
