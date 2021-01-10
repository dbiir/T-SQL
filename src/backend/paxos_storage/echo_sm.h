#pragma once

#include "sm.h"
#include "options.h"
#include <stdio.h>
#include <unistd.h>

namespace phxecho
{

class PhxEchoSMCtx
{
public:
    int iExecuteRet;
    std::string sEchoRespValue;

    PhxEchoSMCtx()
    {
        iExecuteRet = -1;
    }
};

class PhxEchoSM : public phxpaxos::StateMachine
{
public:
    PhxEchoSM();

    bool Execute(const int iGroupIdx, const uint64_t llInstanceID, 
            const std::string & sPaxosValue, phxpaxos::SMCtx * poSMCtx);

    const int SMID() const { return 1; }
};
    
}
