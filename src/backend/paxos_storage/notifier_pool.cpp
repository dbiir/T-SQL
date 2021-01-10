#include "notifier_pool.h"
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>

namespace phxpaxos
{

Notifier :: Notifier()
{
    m_iPipeFD[0] = -1;
    m_iPipeFD[1] = -1;
}

Notifier :: ~Notifier()
{
    for (int i = 0; i < 2; i++)
    {   
        if (m_iPipeFD[i] != -1) 
        {   
            close(m_iPipeFD[i]);
        }   
    }
}

int Notifier :: Init()
{
    int ret = pipe(m_iPipeFD);
    if (ret != 0)
    {   
        return ret;
    } 

    return 0;
}

void Notifier :: SendNotify(const int ret)
{
    int iWriteLen = write(m_iPipeFD[1], (char *)&ret, sizeof(int));
    assert(iWriteLen == sizeof(int));
}

void Notifier :: WaitNotify(int & ret)
{
    ret = -1;
    int iReadLen = read(m_iPipeFD[0], (char *)&ret, sizeof(int));
    assert(iReadLen == sizeof(int));
}

///////////////////////////////////

NotifierPool :: NotifierPool()
{
}

NotifierPool :: ~NotifierPool()
{
    for (auto & it : m_mapPool)
    {
        delete it.second;
    }
}

int NotifierPool :: GetNotifier(const uint64_t iID, Notifier *& poNotifier)
{
    poNotifier = nullptr;
    std::lock_guard<std::mutex> oLock(m_oMutex);

    auto it = m_mapPool.find(iID);
    if (it != end(m_mapPool))
    {
        poNotifier = it->second;
        return 0;
    }

    poNotifier = new Notifier();
    assert(poNotifier != nullptr);
    int ret = poNotifier->Init();
    if (ret != 0)
    {
        return ret;
    }

    m_mapPool[iID] = poNotifier;
    return 0;
}

}

