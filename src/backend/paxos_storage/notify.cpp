#include "notify.h"
#include "commdef.h"
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>

namespace phxpaxos
{

Notify :: Notify(EventLoop * poEventLoop)
    : Event(poEventLoop)
{
    m_iPipeFD[0] = -1;
    m_iPipeFD[1] = -1;
    m_sHost = "Notify";
}

Notify :: ~Notify()
{
    for (int i = 0; i < 2; i++)
    {
        if (m_iPipeFD[i] != -1)
        {
            close(m_iPipeFD[i]);
        }
    }
}

int Notify :: Init()
{
    int ret = pipe(m_iPipeFD);
    if (ret != 0)
    {
        PLErr("create pipe fail, ret %d", ret);
        return ret;
    }

    fcntl(m_iPipeFD[0], F_SETFL, O_NONBLOCK);
    fcntl(m_iPipeFD[1], F_SETFL, O_NONBLOCK);

    AddEvent(EPOLLIN);
    return 0;
}

int Notify :: GetSocketFd() const
{
    return m_iPipeFD[0];
}

const std::string & Notify :: GetSocketHost()
{
    return m_sHost;
}

void Notify :: SendNotify()
{
    ssize_t iWriteLen = write(m_iPipeFD[1], (void *)"a", 1);
    if (iWriteLen != 1)
    {
        //PLErr("notify error, writelen %d", iWriteLen);
    }
}

int Notify :: OnRead()
{
    char sTmp[2] = {0};
    int iReadLen = read(m_iPipeFD[0], sTmp, 1);
    if (iReadLen < 0)
    {
        return -1;
    }

    return 0;
}

void Notify :: OnError(bool & bNeedDelete)
{
    bNeedDelete = false;
}
    
}


