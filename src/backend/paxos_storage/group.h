#pragma once

#include <thread>
#include "comm_include.h"
#include "config_include.h"
#include "instance.h"
#include "cleaner.h"
#include "communicate.h"
#include "options.h"
#include "network.h"

namespace phxpaxos
{

class Group
{
public:
    Group(LogStorage * poLogStorage, 
            NetWork * poNetWork,    
            InsideSM * poMasterSM,
            const int iGroupIdx,
            const Options & oOptions);

    ~Group();

    void StartInit();

    void Init();

    int GetInitRet();

    void Start();

    void Stop();

    Config * GetConfig();

    Instance * GetInstance();

    Committer * GetCommitter();

    Cleaner * GetCheckpointCleaner();

    Replayer * GetCheckpointReplayer();

    void AddStateMachine(StateMachine * poSM);

private:
    Communicate m_oCommunicate;
    Config m_oConfig;
    Instance m_oInstance;

    int m_iInitRet;
    std::thread * m_poThread;
};
    
}
