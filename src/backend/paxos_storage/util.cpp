#include "util.h"
#include <unistd.h>
#include <stdio.h>
#include <sys/time.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <math.h>
#include <thread>

namespace phxpaxos {

using namespace std;

const uint64_t Time :: GetTimestampMS() 
{
    auto now_time = chrono::system_clock::now();
    uint64_t now = (chrono::duration_cast<chrono::milliseconds>(now_time.time_since_epoch())).count();
    return now;
}

const uint64_t Time :: GetSteadyClockMS() 
{
    auto now_time = chrono::steady_clock::now();
    uint64_t now = (chrono::duration_cast<chrono::milliseconds>(now_time.time_since_epoch())).count();
    return now;
}

void Time :: MsSleep(const int iTimeMs)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(iTimeMs));
}

int FileUtils :: IsDir(const std::string & sPath, bool & bIsDir)
{
    bIsDir = false;
    struct stat tStat;
    int ret = stat(sPath.c_str(), &tStat);
    if (ret != 0)
    {
        return ret;
    }
    if (tStat.st_mode & S_IFDIR)
    {
        bIsDir = true;
    }

    return 0;
}

int FileUtils :: DeleteDir(const std::string & sDirPath)
{
    DIR * dir = nullptr;
    struct dirent  * ptr;

    dir = opendir(sDirPath.c_str());
    if (dir == nullptr)
    {
        return 0;
    }

    int ret = 0;
    while ((ptr = readdir(dir)) != nullptr)
    {
        if (strcmp(ptr->d_name, ".") == 0 ||
            strcmp(ptr->d_name, "..") == 0)
        {
            continue;
        }

        char sChildPath[1024] = {0};
        snprintf(sChildPath, sizeof(sChildPath), "%s/%s", sDirPath.c_str(), ptr->d_name);

        bool bIsDir = false;
        ret = FileUtils::IsDir(sChildPath, bIsDir);
        if (ret != 0)
        {
            break;
        }

        if (bIsDir)
        {
            ret = DeleteDir(sChildPath);
            if (ret != 0)
            {
                break;
            }
        }
        else
        {
            ret = remove(sChildPath);
            if (ret != 0)
            {
                break;
            }
        }
    }

    closedir(dir);

    if (ret == 0)
    {
        ret = remove(sDirPath.c_str());
    }

    return ret;
}

int FileUtils :: IterDir(const std::string & sDirPath, std::vector<std::string> & vecFilePathList)
{
    DIR * dir = nullptr;
    struct dirent  * ptr;

    dir = opendir(sDirPath.c_str());
    if (dir == nullptr)
    {
        return 0;
    }

    int ret = 0;
    while ((ptr = readdir(dir)) != nullptr)
    {
        if (strcmp(ptr->d_name, ".") == 0 ||
            strcmp(ptr->d_name, "..") == 0)
        {
            continue;
        }

        char sChildPath[1024] = {0};
        snprintf(sChildPath, sizeof(sChildPath), "%s/%s", sDirPath.c_str(), ptr->d_name);

        bool bIsDir = false;
        ret = FileUtils::IsDir(sChildPath, bIsDir);
        if (ret != 0)
        {
            break;
        }

        if (bIsDir)
        {
            ret = IterDir(sChildPath, vecFilePathList);
            if (ret != 0)
            {
                break;
            }
        }
        else
        {
            vecFilePathList.push_back(sChildPath);
        }
    }

    closedir(dir);

    return ret;
}

TimeStat :: TimeStat()
{
    m_llTime = Time::GetSteadyClockMS();
}

int TimeStat :: Point()
{
    uint64_t llNowTime = Time::GetSteadyClockMS();
    int llPassTime = 0;
    if (llNowTime > m_llTime)
    {
        llPassTime = llNowTime - m_llTime;
    }

    m_llTime = llNowTime;

    return llPassTime;
}

uint64_t OtherUtils :: GenGid(const uint64_t llNodeID)
{
    return (llNodeID ^ FastRand()) + FastRand();
}

__inline__ uint64_t rdtsc()
{

    uint64_t a, d;
    __asm__ volatile ("rdtsc" : "=a" (a), "=d" (d));
    return (d<<32) | a;
}

struct FastRandomSeed {
    bool init;
    unsigned int seed;
};

static __thread FastRandomSeed seed_thread_safe = { false, 0 };

static void ResetFastRandomSeed()
{
    seed_thread_safe.seed = rdtsc();
    seed_thread_safe.init = true; 
}

static void InitFastRandomSeedAtFork()
{
    pthread_atfork(ResetFastRandomSeed, ResetFastRandomSeed, ResetFastRandomSeed);
}

static void InitFastRandomSeed()
{
    static pthread_once_t once = PTHREAD_ONCE_INIT;
    pthread_once(&once, InitFastRandomSeedAtFork);

    ResetFastRandomSeed();
}

const uint32_t OtherUtils :: FastRand()
{
    if (!seed_thread_safe.init)
    {
        InitFastRandomSeed();
    }

    return rand_r(&seed_thread_safe.seed);
}

}
