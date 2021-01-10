#pragma once

#include "utils_include.h"
#include "paxos_log.h"

namespace phxpaxos
{

class Config;
class SMFac;
class LogStorage;
class CheckpointMgr;
    
class Replayer : public Thread
{
public:
    Replayer(
        Config * poConfig,
        SMFac * poSMFac, 
        LogStorage * poLogStorage,
        CheckpointMgr * poCheckpointMgr);

    ~Replayer();

    void Stop();

    void run();

    void Pause();

    void Continue();

    const bool IsPaused() const;

    bool PlayOne(const uint64_t llInstanceID);

private:
    Config * m_poConfig;
    SMFac * m_poSMFac;
    PaxosLog m_oPaxosLog;
    CheckpointMgr * m_poCheckpointMgr;

    bool m_bCanrun;
    bool m_bIsPaused;
    bool m_bIsEnd;
};

}
