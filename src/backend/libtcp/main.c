#include "libtcpforcpp.h"

#include <unistd.h>

#include <arpa/inet.h>
#include <sys/socket.h>

#include <string>
#include "libtcpforc.h"
#include "libtcp/ip.h"

int main()
{
    int result = connectTo(LTS_TCP_IP, 62389);
    if (result == -1)
    {
        printf("connection failed!\n");
        return 0;
    }
    for (int i = 0; i < 10; i++)
    {
        uint64_t ts = getTimestamp();
        printf("ts:%lld\n",ts);
    }

    result = closeConnection();
    return 0;
}