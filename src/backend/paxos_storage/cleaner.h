#pragma once

#include <typeinfo>
#include "utils_include.h"

namespace phxpaxos
{

#define CAN_DELETE_DELTA 1000000 
#define DELETE_SAVE_INTERVAL 100

class Config;
class SMFac;
class LogStorage;
class CheckpointMgr;

class Cleaner : public Thread
{
public:
    Cleaner(
            Config * poConfig,
            SMFac * poSMFac, 
            LogStorage * poLogStorage,
            CheckpointMgr * poCheckpointMgr);

    ~Cleaner();

    void Stop();

    void run();

    void Pause();

    void Continue();

    const bool IsPaused() const;

public:
    void SetHoldPaxosLogCount(const uint64_t llHoldCount);

    int FixMinChosenInstanceID(const uint64_t llOldMinChosenInstanceID);

private:
    bool DeleteOne(const uint64_t llInstanceID);

private:
    Config * m_poConfig;
    SMFac * m_poSMFac;
    LogStorage * m_poLogStorage;
    CheckpointMgr * m_poCheckpointMgr;

    uint64_t m_llLastSave;

    bool m_bCanrun;
    bool m_bIsPaused;

    bool m_bIsEnd;
    bool m_bIsStart;

    uint64_t m_llHoldCount;
};
    
}
