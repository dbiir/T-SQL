#include "echo_sm.h"

#include "paxos/cpp_parse_include.h"

using namespace phxpaxos;

namespace phxecho
{

PhxEchoSM :: PhxEchoSM()
{
}

bool PhxEchoSM :: Execute(const int iGroupIdx, const uint64_t llInstanceID, 
        const std::string & sPaxosValue, SMCtx * poSMCtx)
{
//    printf("[SM Execute] ok, smid %d instanceid %lu value %s\n",
//            SMID(), llInstanceID, sPaxosValue.c_str());
//
//    //only commiter node have SMCtx.
//    if (poSMCtx != nullptr && poSMCtx->m_pCtx != nullptr)
//    {
//        PhxEchoSMCtx * poPhxEchoSMCtx = (PhxEchoSMCtx *)poSMCtx->m_pCtx;
//        poPhxEchoSMCtx->iExecuteRet = 0;
//        poPhxEchoSMCtx->sEchoRespValue = sPaxosValue;
//    }
    //printf("exe %s\n",sPaxosValue.c_str());
    PaxosParse((void*)sPaxosValue.c_str(),sPaxosValue.size());

    return true;
}

}

