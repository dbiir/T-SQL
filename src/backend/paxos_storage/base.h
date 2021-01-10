#pragma once

#include "commdef.h"
#include "comm_include.h"
#include "config_include.h"
#include "msg_transport.h"

namespace phxpaxos 
{

#define GROUPIDXLEN (sizeof(int))
#define HEADLEN_LEN (sizeof(uint16_t))
#define CHECKSUM_LEN (sizeof(uint32_t))


class BallotNumber
{
public:
    BallotNumber() : m_llProposalID(0), m_llNodeID(nullnode) { }

    BallotNumber(const uint64_t llProposalID, const nodeid_t llNodeID) :
        m_llProposalID(llProposalID), m_llNodeID(llNodeID) { }

    ~BallotNumber() { }

    bool operator >= (const BallotNumber & other) const
    {
        if (m_llProposalID == other.m_llProposalID)
        {
            return m_llNodeID >= other.m_llNodeID;
        }
        else
        {
            return m_llProposalID >= other.m_llProposalID;
        }
    }
    
    bool operator != (const BallotNumber & other) const
    {
        return m_llProposalID != other.m_llProposalID 
            || m_llNodeID != other.m_llNodeID;
    }
    
    bool operator == (const BallotNumber & other) const
    {
        return m_llProposalID == other.m_llProposalID 
            && m_llNodeID == other.m_llNodeID;
    }

    bool operator > (const BallotNumber & other) const
    {
        if (m_llProposalID == other.m_llProposalID)
        {
            return m_llNodeID > other.m_llNodeID;
        }
        else
        {
            return m_llProposalID > other.m_llProposalID;
        }
    }

    const bool isnull() const
    {
        return m_llProposalID == 0;
    }

    void reset()
    {
        m_llProposalID = 0;
        m_llNodeID = 0;
    }

    uint64_t m_llProposalID;
    nodeid_t m_llNodeID;
};

///////////////////////////////////////////////////////////

class Instance;

enum BroadcastMessage_Type
{
    BroadcastMessage_Type_RunSelf_First = 1,
    BroadcastMessage_Type_RunSelf_Final = 2,
    BroadcastMessage_Type_RunSelf_None = 3,
};

class Base
{
public:
    Base(const Config * poConfig, const MsgTransport * poMsgTransport, const Instance * poInstance);
    virtual ~Base();

public:
    uint64_t GetInstanceID();

    void NewInstance();

    virtual void InitForNewPaxosInstance() = 0;

    void SetInstanceID(const uint64_t llInstanceID);

    int PackMsg(const PaxosMsg & oPaxosMsg, std::string & sBuffer);
    
    int PackCheckpointMsg(const CheckpointMsg & oCheckpointMsg, std::string & sBuffer);

public:
    const uint32_t GetLastChecksum() const;
    
    void PackBaseMsg(const std::string & sBodyBuffer, const int iCmd, std::string & sBuffer);

    static int UnPackBaseMsg(const std::string & sBuffer, Header & oHeader, size_t & iBodyStartPos, size_t & iBodyLen);

    void SetAsTestMode();

protected:
    virtual int SendMessage(const nodeid_t iSendtoNodeID, const PaxosMsg & oPaxosMsg, const int iSendType = Message_SendType_UDP);

    virtual int BroadcastMessage(
            const PaxosMsg & oPaxosMsg, 
            const int bRunSelfFirst = BroadcastMessage_Type_RunSelf_First,
            const int iSendType = Message_SendType_UDP);
    
    int BroadcastMessageToFollower(
            const PaxosMsg & oPaxosMsg, 
            const int iSendType = Message_SendType_TCP);
    
    int BroadcastMessageToTempNode(
            const PaxosMsg & oPaxosMsg, 
            const int iSendType = Message_SendType_UDP);

protected:
    int SendMessage(const nodeid_t iSendtoNodeID, const CheckpointMsg & oCheckpointMsg, 
            const int iSendType = Message_SendType_TCP);

protected:
    Config * m_poConfig;
    MsgTransport * m_poMsgTransport;
    Instance * m_poInstance;

private:
    uint64_t m_llInstanceID;

    bool m_bIsTestMode;
};
    
}
