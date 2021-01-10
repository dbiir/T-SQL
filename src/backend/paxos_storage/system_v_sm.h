#pragma once

#include "system_variables_store.h"
#include "sm.h"
#include "commdef.h"
#include "inside_sm.h"
#include <set>

namespace phxpaxos
{

class MsgTransport;

class SystemVSM : public InsideSM 
{
public:
    SystemVSM(
        const int iGroupIdx, 
        const nodeid_t iMyNodeID,
        const LogStorage * poLogStorage,
        MembershipChangeCallback pMembershipChangeCallback);

    ~SystemVSM();

    int Init();

    bool Execute(const int iGroupIdx, const uint64_t llInstanceID, const std::string & sValue, SMCtx * poSMCtx);

    const int SMID() const {return SYSTEM_V_SMID;}

    const uint64_t GetGid() const;

    void GetMembership(NodeInfoList & vecNodeInfoList, uint64_t & llVersion); 

    int CreateGid_OPValue(const uint64_t llGid, std::string & sOpValue);
    
    int Membership_OPValue(const NodeInfoList & vecNodeInfoList, const uint64_t llVersion, std::string & sOpValue);

    void AddNodeIDList(const NodeInfoList & vecNodeInfoList);

    void RefleshNodeID();

    const int GetNodeCount() const;

    const int GetMajorityCount() const;

    const bool IsValidNodeID(const nodeid_t iNodeID);

    const bool IsIMInMembership();

    const uint64_t GetCheckpointInstanceID(const int iGroupIdx) const { return m_oSystemVariables.version(); }

    bool ExecuteForCheckpoint(const int iGroupIdx, const uint64_t llInstanceID, 
                              const std::string & sPaxosValue) { return true; }

    int GetCheckpointState(const int iGroupIdx, std::string & sDirPath, 
                           std::vector<std::string> & vecFileList) { return 0; }    
    
    int LoadCheckpointState(const int iGroupIdx, const std::string & sCheckpointTmpFileDirPath,
                            const std::vector<std::string> & vecFileList, const uint64_t llCheckpointInstanceID)
    {
        return 0;
    }

    int LockCheckpointState() { return 0; }

    void UnLockCheckpointState() {}

    int GetCheckpointBuffer(std::string & sCPBuffer);

    int UpdateByCheckpoint(const std::string & sCPBuffer, bool & bChange);

    void GetSystemVariables(SystemVariables & oVariables);
    
    int UpdateSystemVariables(const SystemVariables & oVariables);

    const std::set<nodeid_t> & GetMembershipMap();

private:
    int m_iMyGroupIdx;
    SystemVariables m_oSystemVariables;
    SystemVariablesStore m_oSystemVStore;

    std::set<nodeid_t> m_setNodeID;

    nodeid_t m_iMyNodeID;

    MembershipChangeCallback m_pMembershipChangeCallback;
};
    
}
