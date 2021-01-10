#include "system_variables_store.h"
#include "db.h"

namespace phxpaxos
{

SystemVariablesStore::SystemVariablesStore(const LogStorage * poLogStorage)
    : m_poLogStorage((LogStorage *)poLogStorage){}

SystemVariablesStore :: ~SystemVariablesStore(){}

int SystemVariablesStore :: Write(const WriteOptions & oWriteOptions, const int iGroupIdx, const SystemVariables & oVariables)
{
    const int m_iMyGroupIdx = iGroupIdx;

    string sBuffer;
    bool sSucc = oVariables.SerializeToString(&sBuffer);
    if (!sSucc)
    {
        PLG1Err("Variables.Serialize fail");
        return -1;
    }
    
    int ret = m_poLogStorage->SetSystemVariables(oWriteOptions, iGroupIdx, sBuffer);
    if (ret != 0)
    {
        PLG1Err("DB.Put fail, groupidx %d bufferlen %zu ret %d", 
                iGroupIdx, sBuffer.size(), ret);
        return ret;
    }

    return 0;
}

int SystemVariablesStore :: Read(const int iGroupIdx, SystemVariables & oVariables)
{
    const int m_iMyGroupIdx = iGroupIdx;

    string sBuffer;
    int ret = m_poLogStorage->GetSystemVariables(iGroupIdx, sBuffer);
    if (ret != 0 && ret != 1)
    {
        PLG1Err("DB.Get fail, groupidx %d ret %d", iGroupIdx, ret);
        return ret;
    }
    else if (ret == 1)
    {
        PLG1Imp("DB.Get not found, groupidx %d", iGroupIdx);
        return 1;
    }

    bool bSucc = oVariables.ParseFromArray(sBuffer.data(), sBuffer.size());
    if (!bSucc)
    {
        PLG1Err("Variables.ParseFromArray fail, bufferlen %zu", sBuffer.size());
        return -1;
    }

    return 0;
}

}
