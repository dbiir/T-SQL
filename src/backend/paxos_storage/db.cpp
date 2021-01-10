#include "db.h"
#include "commdef.h"
#include "utils_include.h"

namespace phxpaxos
{

int PaxosComparator :: Compare(const rocksdb::Slice & a, const rocksdb::Slice & b) const
{
    return PCompare(a, b);
}

int PaxosComparator :: PCompare(const rocksdb::Slice & a, const rocksdb::Slice & b) 
{
    if (a.size() != sizeof(uint64_t))
    {
        NLErr("assert a.size %zu b.size %zu", a.size(), b.size());
        assert(a.size() == sizeof(uint64_t));
    }

    if (b.size() != sizeof(uint64_t))
    {
        NLErr("assert a.size %zu b.size %zu", a.size(), b.size());
        assert(b.size() == sizeof(uint64_t));
    }
    
    uint64_t lla = 0;
    uint64_t llb = 0;

    memcpy(&lla, a.data(), sizeof(uint64_t));
    memcpy(&llb, b.data(), sizeof(uint64_t));

    if (lla == llb)
    {
        return 0;
    }

    return lla < llb ? -1 : 1;
}

////////////////////////

Database :: Database() : m_poRocksDB(nullptr), m_poValueStore(nullptr)
{
    m_bHasInit = false;
    m_iMyGroupIdx = -1;
}

Database :: ~Database()
{
    delete m_poValueStore;
    delete m_poRocksDB;

    PLG1Head("RocksDB Deleted. Path %s", m_sDBPath.c_str());
}

int Database :: ClearAllLog()
{
    string sSystemVariablesBuffer;
    int ret = GetSystemVariables(sSystemVariablesBuffer);
    if (ret != 0 && ret != 1)
    {
        PLG1Err("GetSystemVariables fail, ret %d", ret);
        return ret;
    }

    string sMasterVariablesBuffer;
    ret = GetMasterVariables(sMasterVariablesBuffer);
    if (ret != 0 && ret != 1)
    {
        PLG1Err("GetMasterVariables fail, ret %d", ret);
        return ret;
    }

    m_bHasInit = false;

    delete m_poRocksDB;
    m_poRocksDB = nullptr;

    delete m_poValueStore;
    m_poValueStore = nullptr;

    string sBakPath = m_sDBPath + ".bak";

    ret = FileUtils::DeleteDir(sBakPath);
    if (ret != 0)
    {
        PLG1Err("Delete bak dir fail, dir %s", sBakPath.c_str());
        return -1;
    }

    ret = rename(m_sDBPath.c_str(), sBakPath.c_str());
    assert(ret == 0);

    ret = Init(m_sDBPath, m_iMyGroupIdx);
    if (ret != 0)
    {
        PLG1Err("Init again fail, ret %d", ret);
        return ret;
    }

    WriteOptions oWriteOptions;
    oWriteOptions.bSync = true;
    if (sSystemVariablesBuffer.size() > 0)
    {
        ret = SetSystemVariables(oWriteOptions, sSystemVariablesBuffer);
        if (ret != 0)
        {
            PLG1Err("SetSystemVariables fail, ret %d", ret);
            return ret;
        }
    }

    if (sMasterVariablesBuffer.size() > 0)
    {
        ret = SetMasterVariables(oWriteOptions, sMasterVariablesBuffer);
        if (ret != 0)
        {
            PLG1Err("SetMasterVariables fail, ret %d", ret);
            return ret;
        }
    }

    return 0;
}

int Database :: Init(const std::string & sDBPath, const int iMyGroupIdx)
{
    if (m_bHasInit)
    {
        return 0;
    }

    m_iMyGroupIdx = iMyGroupIdx;

    m_sDBPath = sDBPath;
    
    rocksdb::Options oOptions;
    oOptions.create_if_missing = true;
    oOptions.comparator = &m_oPaxosCmp;
    //every group have different buffer size to avoid all group compact at the same time.
    oOptions.write_buffer_size = 1024 * 1024 + iMyGroupIdx * 10 * 1024;

    rocksdb::Status oStatus = rocksdb::DB::Open(oOptions, sDBPath, &m_poRocksDB);

    if (!oStatus.ok())
    {
        PLG1Err("Open rocksdb fail, db_path %s", sDBPath.c_str());
        return -1;
    }

    m_poValueStore = new LogStore(); 
    assert(m_poValueStore != nullptr);

    int ret = m_poValueStore->Init(sDBPath, iMyGroupIdx, (Database *)this);
    if (ret != 0)
    {
        PLG1Err("value store init fail, ret %d", ret);
        return -1;
    }

    m_bHasInit = true;

    PLG1Imp("OK, db_path %s", sDBPath.c_str());

    return 0;
}

const std::string Database :: GetDBPath()
{
    return m_sDBPath;
}

int Database :: GetMaxInstanceIDFileID(std::string & sFileID, uint64_t & llInstanceID)
{
    uint64_t llMaxInstanceID = 0;
    int ret = GetMaxInstanceID(llMaxInstanceID);
    if (ret != 0 && ret != 1)
    {
        return ret;
    }

    if (ret == 1)
    {
        sFileID = "";
        return 0;
    }

    string sKey = GenKey(llMaxInstanceID);
    
    rocksdb::Status oStatus = m_poRocksDB->Get(rocksdb::ReadOptions(), sKey, &sFileID);
    if (!oStatus.ok())
    {
        if (oStatus.IsNotFound())
        {
            BP->GetLogStorageBP()->RocksDBGetNotExist();
            //PLG1Err("RocksDB.Get not found %s", sKey.c_str());
            return 1;
        }
        
        BP->GetLogStorageBP()->RocksDBGetFail();
        PLG1Err("RocksDB.Get fail");
        return -1;
    }

    llInstanceID = llMaxInstanceID;

    return 0;
}

int Database :: RebuildOneIndex(const uint64_t llInstanceID, const std::string & sFileID)
{
    string sKey = GenKey(llInstanceID);

    rocksdb::WriteOptions oRocksDBWriteOptions;
    oRocksDBWriteOptions.sync = false;

    rocksdb::Status oStatus = m_poRocksDB->Put(oRocksDBWriteOptions, sKey, sFileID);
    if (!oStatus.ok())
    {
        BP->GetLogStorageBP()->RocksDBPutFail();
        PLG1Err("RocksDB.Put fail, instanceid %lu valuelen %zu", llInstanceID, sFileID.size());
        return -1;
    }

    return 0;
}

////////////////////////////////////////////////////////////////////////

int Database :: GetFromRocksDB(const uint64_t llInstanceID, std::string & sValue)
{
    string sKey = GenKey(llInstanceID);
    
    rocksdb::Status oStatus = m_poRocksDB->Get(rocksdb::ReadOptions(), sKey, &sValue);
    if (!oStatus.ok())
    {
        if (oStatus.IsNotFound())
        {
            BP->GetLogStorageBP()->RocksDBGetNotExist();
            PLG1Debug("RocksDB.Get not found, instanceid %lu", llInstanceID);
            return 1;
        }
        
        BP->GetLogStorageBP()->RocksDBGetFail();
        PLG1Err("RocksDB.Get fail, instanceid %lu", llInstanceID);
        return -1;
    }

    return 0;
}

int Database :: Get(const uint64_t llInstanceID, std::string & sValue)
{
    if (!m_bHasInit)
    {
        PLG1Err("no init yet");
        return -1;
    }

    string sFileID;
    int ret = GetFromRocksDB(llInstanceID, sFileID);
    if (ret != 0)
    {
        return ret;
    }

    uint64_t llFileInstanceID = 0;
    ret = FileIDToValue(sFileID, llFileInstanceID, sValue);
    if (ret != 0)
    {
        BP->GetLogStorageBP()->FileIDToValueFail();
        return ret;
    }

    if (llFileInstanceID != llInstanceID)
    {
        PLG1Err("file instanceid %lu not equal to key.instanceid %lu", llFileInstanceID, llInstanceID);
        return -2;
    }

    return 0;
}

int Database :: ValueToFileID(const WriteOptions & oWriteOptions, const uint64_t llInstanceID, const std::string & sValue, std::string & sFileID)
{
    int ret = m_poValueStore->Append(oWriteOptions, llInstanceID, sValue, sFileID);
    if (ret != 0)
    {
        BP->GetLogStorageBP()->ValueToFileIDFail();
        PLG1Err("fail, ret %d", ret);
        return ret;
    }

    return 0;
}

int Database :: FileIDToValue(const std::string & sFileID, uint64_t & llInstanceID, std::string & sValue)
{
    int ret = m_poValueStore->Read(sFileID, llInstanceID, sValue);
    if (ret != 0)
    {
        PLG1Err("fail, ret %d", ret);
        return ret;
    }

    return 0;
}

int Database :: PutToRocksDB(const bool bSync, const uint64_t llInstanceID, const std::string & sValue)
{
    string sKey = GenKey(llInstanceID);

    rocksdb::WriteOptions oRocksDBWriteOptions;
    oRocksDBWriteOptions.sync = bSync;

    m_oTimeStat.Point();

    rocksdb::Status oStatus = m_poRocksDB->Put(oRocksDBWriteOptions, sKey, sValue);
    if (!oStatus.ok())
    {
        BP->GetLogStorageBP()->RocksDBPutFail();
        PLG1Err("RocksDB.Put fail, instanceid %lu valuelen %zu", llInstanceID, sValue.size());
        return -1;
    }

    BP->GetLogStorageBP()->RocksDBPutOK(m_oTimeStat.Point());

    return 0;
}

int Database :: Put(const WriteOptions & oWriteOptions, const uint64_t llInstanceID, const std::string & sValue)
{
    if (!m_bHasInit)
    {
        PLG1Err("no init yet");
        return -1;
    }

    std::string sFileID;
    int ret = ValueToFileID(oWriteOptions, llInstanceID, sValue, sFileID);
    if (ret != 0)
    {
        return ret;
    }

    ret = PutToRocksDB(false, llInstanceID, sFileID);
    
    return ret;
}

int Database :: ForceDel(const WriteOptions & oWriteOptions, const uint64_t llInstanceID)
{
    if (!m_bHasInit)
    {
        PLG1Err("no init yet");
        return -1;
    }

    string sKey = GenKey(llInstanceID);
    string sFileID;
    
    rocksdb::Status oStatus = m_poRocksDB->Get(rocksdb::ReadOptions(), sKey, &sFileID);
    if (!oStatus.ok())
    {
        if (oStatus.IsNotFound())
        {
            PLG1Debug("RocksDB.Get not found, instanceid %lu", llInstanceID);
            return 0;
        }
        
        PLG1Err("RocksDB.Get fail, instanceid %lu", llInstanceID);
        return -1;
    }

    int ret = m_poValueStore->ForceDel(sFileID, llInstanceID);
    if (ret != 0)
    {
        return ret;
    }

    rocksdb::WriteOptions oRocksDBWriteOptions;
    oRocksDBWriteOptions.sync = oWriteOptions.bSync;
    
    oStatus = m_poRocksDB->Delete(oRocksDBWriteOptions, sKey);
    if (!oStatus.ok())
    {
        PLG1Err("RocksDB.Delete fail, instanceid %lu", llInstanceID);
        return -1;
    }

    return 0;
}

int Database :: Del(const WriteOptions & oWriteOptions, const uint64_t llInstanceID)
{
    if (!m_bHasInit)
    {
        PLG1Err("no init yet");
        return -1;
    }

    string sKey = GenKey(llInstanceID);

    if (OtherUtils::FastRand() % 100 < 1)
    {
        //no need to del vfile every times.
        string sFileID;
        rocksdb::Status oStatus = m_poRocksDB->Get(rocksdb::ReadOptions(), sKey, &sFileID);
        if (!oStatus.ok())
        {
            if (oStatus.IsNotFound())
            {
                PLG1Debug("RocksDB.Get not found, instanceid %lu", llInstanceID);
                return 0;
            }
            
            PLG1Err("RocksDB.Get fail, instanceid %lu", llInstanceID);
            return -1;
        }

        int ret = m_poValueStore->Del(sFileID, llInstanceID);
        if (ret != 0)
        {
            return ret;
        }
    }

    rocksdb::WriteOptions oRocksDBWriteOptions;
    oRocksDBWriteOptions.sync = oWriteOptions.bSync;
    
    rocksdb::Status oStatus = m_poRocksDB->Delete(oRocksDBWriteOptions, sKey);
    if (!oStatus.ok())
    {
        PLG1Err("RocksDB.Delete fail, instanceid %lu", llInstanceID);
        return -1;
    }

    return 0;
}

int Database :: GetMaxInstanceID(uint64_t & llInstanceID)
{
    llInstanceID = MINCHOSEN_KEY;

    rocksdb::Iterator * it = m_poRocksDB->NewIterator(rocksdb::ReadOptions());
    
    it->SeekToLast();

    while (it->Valid())
    {
        llInstanceID = GetInstanceIDFromKey(it->key().ToString());
        if (llInstanceID == MINCHOSEN_KEY
                || llInstanceID == SYSTEMVARIABLES_KEY
                || llInstanceID == MASTERVARIABLES_KEY)
        {
            it->Prev();
        }
        else
        {
            delete it;
            return 0;
        }
    }

    delete it;
    return 1;
}

std::string Database :: GenKey(const uint64_t llInstanceID)
{
    string sKey;
    sKey.append((char *)&llInstanceID, sizeof(uint64_t));
    return sKey;
}

const uint64_t Database :: GetInstanceIDFromKey(const std::string & sKey)
{
    uint64_t llInstanceID = 0;
    memcpy(&llInstanceID, sKey.data(), sizeof(uint64_t));

    return llInstanceID;
}

int Database :: GetMinChosenInstanceID(uint64_t & llMinInstanceID)
{
    if (!m_bHasInit)
    {
        PLG1Err("no init yet");
        return -1;
    }

    static uint64_t llMinKey = MINCHOSEN_KEY;
    std::string sValue;
    int ret = GetFromRocksDB(llMinKey, sValue);
    if (ret != 0 && ret != 1)
    {
        PLG1Err("fail, ret %d", ret);
        return ret;
    }

    if (ret == 1)
    {
        PLG1Err("no min chosen instanceid");
        llMinInstanceID = 0;
        return 0;
    }

    //old version, minchonsenid store in logstore.
    //new version, minchonsenid directly store in rocksdb.
    if (m_poValueStore->IsValidFileID(sValue))
    {
        ret = Get(llMinKey, sValue);
        if (ret != 0 && ret != 1)
        {
            PLG1Err("Get from log store fail, ret %d", ret);
            return ret;
        }
    }

    if (sValue.size() != sizeof(uint64_t))
    {
        PLG1Err("fail, mininstanceid size wrong");
        return -2;
    }

    memcpy(&llMinInstanceID, sValue.data(), sizeof(uint64_t));

    PLG1Imp("ok, min chosen instanceid %lu", llMinInstanceID);

    return 0;
}

int Database :: SetMinChosenInstanceID(const WriteOptions & oWriteOptions, const uint64_t llMinInstanceID)
{
    if (!m_bHasInit)
    {
        PLG1Err("no init yet");
        return -1;
    }

    static uint64_t llMinKey = MINCHOSEN_KEY;
    char sValue[sizeof(uint64_t)] = {0};
    memcpy(sValue, &llMinInstanceID, sizeof(uint64_t));

    int ret = PutToRocksDB(true, llMinKey, string(sValue, sizeof(uint64_t)));
    if (ret != 0)
    {
        return ret;
    }

    PLG1Imp("ok, min chosen instanceid %lu", llMinInstanceID);

    return 0;
}


int Database :: SetSystemVariables(const WriteOptions & oWriteOptions, const std::string & sBuffer)
{
    static uint64_t llSystemVariablesKey = SYSTEMVARIABLES_KEY;
    return PutToRocksDB(true, llSystemVariablesKey, sBuffer);
}

int Database :: GetSystemVariables(std::string & sBuffer)
{
    static uint64_t llSystemVariablesKey = SYSTEMVARIABLES_KEY;
    return GetFromRocksDB(llSystemVariablesKey, sBuffer);
}

int Database :: SetMasterVariables(const WriteOptions & oWriteOptions, const std::string & sBuffer)
{
    static uint64_t llMasterVariablesKey = MASTERVARIABLES_KEY;
    return PutToRocksDB(true, llMasterVariablesKey, sBuffer);
}

int Database :: GetMasterVariables(std::string & sBuffer)
{
    static uint64_t llMasterVariablesKey = MASTERVARIABLES_KEY;
    return GetFromRocksDB(llMasterVariablesKey, sBuffer);
}

////////////////////////////////////////////////////

MultiDatabase :: MultiDatabase()
{
}

MultiDatabase :: ~MultiDatabase()
{
    for (auto & poDB : m_vecDBList)
    {
        delete poDB;
    }
}

int MultiDatabase :: Init(const std::string & sDBPath, int iGroupIndex)
{
    if (access(sDBPath.c_str(), F_OK) == -1)
    {
        PLErr("DBPath not exist or no limit to open, %s", sDBPath.c_str());
        return -1;
    }

    std::string sNewDBPath = sDBPath;

    if (sDBPath[sDBPath.size() - 1] != '/')
    {
        sNewDBPath += '/';
    }

    for (int iGroupIdx = iGroupIndex; iGroupIdx < iGroupIndex+1; iGroupIdx++)
    {
        char sGroupDBPath[512] = {0};
        snprintf(sGroupDBPath, sizeof(sGroupDBPath), "%sg%d", sNewDBPath.c_str(), iGroupIdx);

        Database * poDB = new Database();
        assert(poDB != nullptr);
        m_vecDBList.push_back(poDB);

        if (poDB->Init(sGroupDBPath, iGroupIdx) != 0)
        {
            return -1;
        }
    }

    return 0;
}

const std::string MultiDatabase :: GetLogStorageDirPath(const int iGroupIdx)
{

    return m_vecDBList[0]->GetDBPath();
}

int MultiDatabase :: Get(const int iGroupIdx, const uint64_t llInstanceID, std::string & sValue)
{
    return m_vecDBList[0]->Get(llInstanceID, sValue);
}

int MultiDatabase :: Put(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID, const std::string & sValue)
{
    
    return m_vecDBList[0]->Put(oWriteOptions, llInstanceID, sValue);
}

int MultiDatabase :: Del(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID)
{
    
    return m_vecDBList[0]->Del(oWriteOptions, llInstanceID);
}

int MultiDatabase :: ForceDel(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID)
{
    return m_vecDBList[0]->ForceDel(oWriteOptions, llInstanceID);
}

int MultiDatabase :: GetMaxInstanceID(const int iGroupIdx, uint64_t & llInstanceID)
{

    
    return m_vecDBList[0]->GetMaxInstanceID(llInstanceID);
}
    
int MultiDatabase :: SetMinChosenInstanceID(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llMinInstanceID)
{

    return m_vecDBList[0]->SetMinChosenInstanceID(oWriteOptions, llMinInstanceID);
}

int MultiDatabase :: GetMinChosenInstanceID(const int iGroupIdx, uint64_t & llMinInstanceID)
{


    return m_vecDBList[0]->GetMinChosenInstanceID(llMinInstanceID);
}

int MultiDatabase :: ClearAllLog(const int iGroupIdx)
{


    return m_vecDBList[0]->ClearAllLog();
}

int MultiDatabase :: SetSystemVariables(const WriteOptions & oWriteOptions, const int iGroupIdx, const std::string & sBuffer)
{


    return m_vecDBList[0]->SetSystemVariables(oWriteOptions, sBuffer);
}

int MultiDatabase :: GetSystemVariables(const int iGroupIdx, std::string & sBuffer)
{


    return m_vecDBList[0]->GetSystemVariables(sBuffer);
}

int MultiDatabase :: SetMasterVariables(const WriteOptions & oWriteOptions, const int iGroupIdx, const std::string & sBuffer)
{


    return m_vecDBList[0]->SetMasterVariables(oWriteOptions, sBuffer);
}

int MultiDatabase :: GetMasterVariables(const int iGroupIdx, std::string & sBuffer)
{


    return m_vecDBList[0]->GetMasterVariables(sBuffer);
}

}


