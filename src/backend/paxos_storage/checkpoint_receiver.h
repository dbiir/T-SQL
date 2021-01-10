#pragma once

#include <map>
#include <string>
#include "options.h"
#include "comm_include.h"
#include "config_include.h"

namespace phxpaxos
{

class Config;
class LogStorage;

class CheckpointReceiver
{
public:
    CheckpointReceiver(Config * poConfig, LogStorage * poLogStorage);
    ~CheckpointReceiver();

    void Reset();

    int NewReceiver(const nodeid_t iSenderNodeID, const uint64_t llUUID);

    const bool IsReceiverFinish(const nodeid_t iSenderNodeID, const uint64_t llUUID, const uint64_t llEndSequence);

    const std::string GetTmpDirPath(const int iSMID);

    int ReceiveCheckpoint(const CheckpointMsg & oCheckpointMsg);

    int InitFilePath(const std::string & sFilePath, std::string & sFormatFilePath);
private:

    int ClearCheckpointTmp();

    int CreateDir(const std::string & sDirPath);

private:
    Config * m_poConfig;
    LogStorage * m_poLogStorage;

private:
    nodeid_t m_iSenderNodeID; 
    uint64_t m_llUUID;
    uint64_t m_llSequence;

private:
    std::map<std::string, bool> m_mapHasInitDir;
};
    
}
