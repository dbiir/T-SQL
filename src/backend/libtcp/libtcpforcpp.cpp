#include "libtcpforcpp.h"

#include <unistd.h>

#include <arpa/inet.h>
#include <sys/socket.h>

#include <string>
#include <errno.h>
#include "libtcpforcpp.h"
#include "ltsrpc.pb.h"
#include "libtcp/ip.h"

static int s = -1;

int connectTo(const char* ip, uint16_t port)
{
	s = socket(AF_INET, SOCK_STREAM, 0);
	if (s < 0)
	{
		return -1;
	}
	struct sockaddr_in addr;
	bzero(&addr, sizeof(struct sockaddr_in));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	addr.sin_addr.s_addr = inet_addr(ip);
	if(connect(s, (sockaddr *)&addr, sizeof(addr)) < 0)
	{
		return -1;
	}
	struct timeval tv;
	tv.tv_sec = 1;
	tv.tv_usec = 0;
	setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(struct timeval));

	return 0;
}

const int headerLen = 2;
const int minSendLength = 24;
char *req = NULL;
char *res = NULL;
char *buffer = NULL;
int ind = 0;
int result = 0;
int sendresult = 0;
int errorno = 0;
uint64_t getTimestamp()
{
	if (s == -1)
	{
		while(connectTo(LTS_TCP_IP, 62389));
	}
	ltsrpc::GetTxnTimestampCtx lts;
	lts.set_txn_id(12345678);

	std::string message;
	lts.SerializeToString(&message);
	auto size = (uint16_t)message.size();
	uint16_t cpsize = htons(size);
	if (!req)
	{
		req = (char *)malloc(minSendLength);
	}
	memcpy(req, (void *)&cpsize, sizeof(uint16_t));
	memcpy(req + headerLen, message.data(), size);
	memset(req + headerLen + size, 0, minSendLength - headerLen - size);
resend:
	sendresult = send(s, req, minSendLength, 0);

	if (!res)
	{
		res = (char *)malloc(minSendLength);
	}
	ind = 0;
	buffer = (char*)malloc(18);
	for (;;)
	{
rerecv:
		result = recv(s, buffer, 18, 0);
		if (result == -1)
		{
			errorno = errno;
			switch (errorno)
			{
				case 104:
					break;
				case EINTR:
				case EAGAIN:
					goto rerecv;
				default:
					break;
			}
			closeConnection();
			while(connectTo(LTS_TCP_IP, 62389));
			goto resend;
		}
		if (ind + result > 18)
		{
			ind = 0;
		}
		memcpy(res + ind, buffer, result);
		ind += result;
		if (ind == 18)
		{
			ind = 0;
			break;
		}
	}
	free(buffer);

	uint16_t olen = *(uint16_t *)res;
	uint16_t respLen = ntohs(olen);
	lts.ParseFromArray(res + 2, respLen);
	return lts.txn_ts();
}

int closeConnection()
{
	if (s == -1)
		return 0;
	int re = close(s);
	s = -1;
	return re;
}
