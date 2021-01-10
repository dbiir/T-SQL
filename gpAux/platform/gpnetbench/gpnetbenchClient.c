#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>

#define INIT_RETRIES 5

static void send_buffer(int fd, char* buffer, int bytes);
static void print_headers(void);
static double subtractTimeOfDay(struct timeval* begin, struct timeval* end);
static void usage(void);

static void
usage(void)
{
	printf("usage: gpnetbenchClient -p PORT -H HOST [OPTIONS]\n\n");

	printf(" -p PORT        port to connect to for the server\n");
	printf(" -H HOST        hostname to connect to for the server\n");
	printf(" -l SECONDS     number of seconds to sample the network, default is 60\n");
	printf(" -P {0|1}       0 (don't) or 1 (do) display headers in the output, default is 1\n");
	printf(" -b SIZE        size of the send buffer in kilobytes, default is 32\n");
	printf(" -h             show this help message\n");
}

int
main(int argc, char** argv)
{
	int socketFd;
	int retVal;
	int c;
	int i;
	int displayHeaders = 1;
	int serverPort = 0;
	int duration = 60;
	double actual_duration;
	char* hostname = NULL;
	char* sendBuffer = NULL;
	int kilobytesBufSize = 32;
	int bytesBufSize;
	struct sockaddr_in address;
	struct hostent* host_entry;
	time_t start_time;
	time_t end_time;
	unsigned int buffers_sent = 0;
	double megaBytesSent;
	double megaBytesPerSecond;
    struct timeval beginTimeDetails;
    struct timeval endTimeDetails;

	while ((c = getopt (argc, argv, "p:l:b:P:H:f:t:h")) != -1)
	{
		switch (c)
		{
			case 'p':
				serverPort = atoi(optarg);
				break;
			case 'l':
				duration = atoi(optarg);
				break;
			case 'b':
				kilobytesBufSize = atoi(optarg);
				break;
			case 'P':
				displayHeaders = atoi(optarg);
				if (displayHeaders)
					displayHeaders = 1;
				break;
			case 'H':
				hostname = optarg;
				break;
			case 'f':
				fprintf(stderr, "NOTICE: -f is deprecated, and has no effect\n");
				break;
			case 't':
				fprintf(stderr, "NOTICE: -t is deprecated, and has no effect\n");
				break;
			case 'h':
			case '?':
        	default:
				usage();
				return 1;
		}
	}

	if (!serverPort)
	{
		fprintf(stderr, "-p port not specified\n");
		usage();
		return 1;
	}
	if (!hostname)
	{
		fprintf(stderr, "-H hostname not specified\n");
		usage();
		return 1;
	}

	// validate a sensible value for duration
	if (duration < 5 || duration > 3600)
	{
		fprintf(stderr, "duration must be between 5 and 3600 seconds\n");
		return 1;
	}

	// validate a sensible value for buffer size
	if (kilobytesBufSize < 1 || kilobytesBufSize > 10240)
	{
		fprintf(stderr, "buffer size for sending must be between 1 and 10240 KB\n");
		return 1;
	}
	bytesBufSize = kilobytesBufSize * 1024;

	sendBuffer = calloc(1, bytesBufSize);
	if (!sendBuffer)
	{
		fprintf(stderr, "buffer allocation failed\n");
		return 1;
	}

	socketFd = socket(PF_INET, SOCK_STREAM, 0); 
	if (socketFd < 0)
	{ 
		fprintf(stderr, "socket call failed\n");
		return 1;
	}   

	host_entry = gethostbyname(hostname);
	memset(&address, 0, sizeof(struct sockaddr_in));
	address.sin_family = AF_INET;
	memcpy((char *)&address.sin_addr,(char *)host_entry->h_addr, host_entry->h_length);
	address.sin_port = htons(serverPort);

	for (i = 0; i < INIT_RETRIES; ++i)
	{
		retVal = connect(socketFd,(struct sockaddr *)&address, sizeof(address));
		if (retVal == 0)
			break;
		sleep(1);
	}

	if (retVal < 0)
	{
		fprintf(stderr, "Could not connect to server after %d retries\n", INIT_RETRIES);
		return 1;
	}
	printf("Connected to server\n");

	start_time = time(NULL);
	end_time = start_time + duration;
    gettimeofday(&beginTimeDetails, NULL);
	while (time(NULL) < end_time)
	{
		send_buffer(socketFd, sendBuffer, bytesBufSize);
		buffers_sent++;
	}
    gettimeofday(&endTimeDetails, NULL);

	actual_duration = subtractTimeOfDay(&beginTimeDetails, &endTimeDetails);
	megaBytesSent = buffers_sent * (double)bytesBufSize / (1024.0*1024.0);
	megaBytesPerSecond = megaBytesSent / actual_duration;

	if (displayHeaders)
		print_headers();

	printf("0     0        %d       %.2f     %.2f\n", bytesBufSize, (double)actual_duration, megaBytesPerSecond);
	return 0;
}

static void
send_buffer(int fd, char* buffer, int bytes)
{
	ssize_t retval;

	while(bytes > 0)
	{
		retval = send(fd, buffer, bytes, 0);
		if (retval < 0)
		{
			perror("error on send call");
			exit(1);
		}
		if (retval > bytes)
		{
			fprintf(stderr, "unexpected large return code from send %d with only %d bytes in send buffer\n", (int)retval, bytes);
		}

		// advance the  buffer by number of bytes sent and reduce number of bytes remaining to be sent
		bytes -= retval;
		buffer += retval;
	}
}

static double
subtractTimeOfDay(struct timeval* begin, struct timeval* end)
{
	double seconds;

	if (end->tv_usec < begin->tv_usec)
	{
		end->tv_usec += 1000000;
		end->tv_sec -= 1;
	}

	seconds = end->tv_usec - begin->tv_usec;
	seconds /= 1000000.0;

	seconds += (end->tv_sec - begin->tv_sec);
	return seconds;
}

static void
print_headers()
{
	printf("               Send\n");
	printf("               Message  Elapsed\n");
	printf("               Size     Time     Throughput\n");
	printf("n/a   n/a      bytes    secs.    MBytes/sec\n");
}
