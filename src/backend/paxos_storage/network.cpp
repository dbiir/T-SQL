#include "network.h"
#include "node.h"
#include "commdef.h"

namespace phxpaxos
{

NetWork :: NetWork()
{
}
    
int NetWork :: OnReceiveMessage(int iGroupIndex,const char * pcMessage, const int iMessageLen)
{
    if (m_mapNode.find(iGroupIndex)!=m_mapNode.end())
    {
        m_mapNode[iGroupIndex]->OnReceiveMessage(pcMessage, iMessageLen);
    }
    else
    {
        PLHead("receive msglen %d", iMessageLen);
    }

    return 0;
}
void NetWork :: removenode(int iGroupindex){
    if(m_mapNode.find(iGroupindex)==m_mapNode.end()){
        return;
    }
    m_mapNode.erase(iGroupindex);
}


}


