#pragma once

namespace phxpaxos
{

#define SYSTEM_V_SMID 100000000
#define MASTER_V_SMID 100000001
#define BATCH_PROPOSE_SMID 100000002

enum PaxosTryCommitRet
{
    PaxosTryCommitRet_OK = 0,
    PaxosTryCommitRet_Reject = -2,
    PaxosTryCommitRet_Conflict = 14,
    PaxosTryCommitRet_ExecuteFail = 15,
    PaxosTryCommitRet_Follower_Cannot_Commit = 16,
    PaxosTryCommitRet_Im_Not_In_Membership  = 17,
    PaxosTryCommitRet_Value_Size_TooLarge = 18,
    PaxosTryCommitRet_Timeout = 404,
    PaxosTryCommitRet_TooManyThreadWaiting_Reject = 405,
};

enum PaxosNodeFunctionRet
{
    Paxos_SystemError = -1,
    Paxos_GroupIdxWrong = -5,
    Paxos_MembershipOp_GidNotSame = -501,
    Paxos_MembershipOp_VersionConflit = -502,
    Paxos_MembershipOp_NoGid = 1001,
    Paxos_MembershipOp_Add_NodeExist = 1002,
    Paxos_MembershipOp_Remove_NodeNotExist = 1003,
    Paxos_MembershipOp_Change_NoChange = 1004,
    Paxos_GetInstanceValue_Value_NotExist = 1005,
    Paxos_GetInstanceValue_Value_Not_Chosen_Yet = 1006,
};

}

