#include "master_variables_store.h"
#include "db.h"

namespace phxpaxos
{

MasterVariablesStore :: MasterVariablesStore(const LogStorage * poLogStorage) : m_poLogStorage((LogStorage *)poLogStorage)
{
}

MasterVariablesStore :: ~MasterVariablesStore()
{
}

int MasterVariablesStore :: Write(const WriteOptions & oWriteOptions, const int iGroupIdx, const MasterVariables & oVariables)
{
    const int m_iMyGroupIdx = iGroupIdx;

    string sBuffer;
    bool sSucc = oVariables.SerializeToString(&sBuffer);
    if (!sSucc)
    {
        PLG1Err("Variables.Serialize fail");
        return -1;
    }
    
    int ret = m_poLogStorage->SetMasterVariables(oWriteOptions, iGroupIdx, sBuffer);
    if (ret != 0)
    {
        PLG1Err("DB.Put fail, groupidx %d bufferlen %zu ret %d", 
                iGroupIdx, sBuffer.size(), ret);
        return ret;
    }

    return 0;
}

int MasterVariablesStore :: Read(const int iGroupIdx, MasterVariables & oVariables)
{
    const int m_iMyGroupIdx = iGroupIdx;

    string sBuffer;
    int ret = m_poLogStorage->GetMasterVariables(iGroupIdx, sBuffer);
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


