#include "paxos_log.h"
#include "db.h"

namespace phxpaxos
{

PaxosLog :: PaxosLog(const LogStorage * poLogStorage) : m_poLogStorage((LogStorage *)poLogStorage)
{
}

PaxosLog :: ~PaxosLog()
{
}

int PaxosLog :: WriteLog(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID, const std::string & sValue)
{
    const int m_iMyGroupIdx = iGroupIdx;

    AcceptorStateData oState;
    oState.set_instanceid(llInstanceID);
    oState.set_acceptedvalue(sValue);
    oState.set_promiseid(0);
    oState.set_promisenodeid(nullnode);
    oState.set_acceptedid(0);
    oState.set_acceptednodeid(nullnode);

    int ret = WriteState(oWriteOptions, iGroupIdx, llInstanceID, oState);
    if (ret != 0)
    {
        PLG1Err("WriteState to db fail, groupidx %d instanceid %lu ret %d", iGroupIdx, llInstanceID, ret);
        return ret;
    }

    PLG1Imp("OK, groupidx %d InstanceID %lu valuelen %zu", 
            iGroupIdx, llInstanceID, sValue.size());

    return 0;
}

int PaxosLog :: ReadLog(const int iGroupIdx, const uint64_t llInstanceID, std::string & sValue)
{
    const int m_iMyGroupIdx = iGroupIdx;

    AcceptorStateData oState;
    int ret = ReadState(iGroupIdx, llInstanceID, oState); 
    if (ret != 0)
    {
        PLG1Err("ReadState from db fail, groupidx %d instanceid %lu ret %d", 
                iGroupIdx, llInstanceID, ret);
        return ret;

    }

    sValue = oState.acceptedvalue();

    PLG1Imp("OK, groupidx %d InstanceID %lu value %zu", 
            iGroupIdx, llInstanceID, sValue.size());

    return 0;
}

int PaxosLog :: WriteState(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID, const AcceptorStateData & oState)
{
    const int m_iMyGroupIdx = iGroupIdx;

    string sBuffer;
    bool sSucc = oState.SerializeToString(&sBuffer);
    if (!sSucc)
    {
        PLG1Err("State.Serialize fail");
        return -1;
    }
    
    int ret = m_poLogStorage->Put(oWriteOptions, iGroupIdx, llInstanceID, sBuffer);
    if (ret != 0)
    {
        PLG1Err("DB.Put fail, groupidx %d bufferlen %zu ret %d", 
                iGroupIdx, sBuffer.size(), ret);
        return ret;
    }

    return 0;
}

int PaxosLog :: ReadState(const int iGroupIdx, const uint64_t llInstanceID, AcceptorStateData & oState)
{
    const int m_iMyGroupIdx = iGroupIdx;

    string sBuffer;
    int ret = m_poLogStorage->Get(iGroupIdx, llInstanceID, sBuffer);
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

    bool bSucc = oState.ParseFromArray(sBuffer.data(), sBuffer.size());
    if (!bSucc)
    {
        PLG1Err("State.ParseFromArray fail, bufferlen %zu", sBuffer.size());
        return -1;
    }

    return 0;
}

int PaxosLog :: GetMaxInstanceIDFromLog(const int iGroupIdx, uint64_t & llInstanceID)
{
    const int m_iMyGroupIdx = iGroupIdx;

    int ret = m_poLogStorage->GetMaxInstanceID(iGroupIdx, llInstanceID);
    if (ret != 0 && ret != 1)
    {
        PLG1Err("DB.GetMax fail, groupidx %d ret %d", iGroupIdx, ret);
    }
    else if (ret == 1)
    {
        PLG1Debug("MaxInstanceID not exist, groupidx %d", iGroupIdx);
    }
    else
    {
        PLG1Imp("OK, MaxInstanceID %llu groupidsx %d", llInstanceID, iGroupIdx);
    }

    return ret;
}

}


