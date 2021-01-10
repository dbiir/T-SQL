#pragma once

#include <mutex>
#include "log.h"
#include "utils_include.h"
#include <string>

namespace phxpaxos
{

#define LOGGER (Logger::Instance())
#define LOG_ERROR(format, args...)\
       LOGGER->LogError(format, ## args);
#define LOG_STATUS(format, args...)\
       LOGGER->LogStatus(format, ## args);
#define LOG_WARNING(format, args...)\
       LOGGER->LogWarning(format, ## args);
#define LOG_INFO(format, args...)\
       LOGGER->LogInfo(format, ## args);
#define LOG_VERBOSE(format, args...)\
       LOGGER->LogVerbose(format, ## args);
#define LOG_SHOWY(format, args...)\
       LOGGER->LogShowy(format, ## args);

class Logger
{
public:
    Logger();
    ~Logger();

    static Logger * Instance();

    void InitLogger(const LogLevel eLogLevel);

    void SetLogFunc(LogFunc pLogFunc);

    void LogError(const char * pcFormat, ...);

    void LogStatus(const char * pcFormat, ...);

    void LogWarning(const char * pcFormat, ...);
    
    void LogInfo(const char * pcFormat, ...);
    
    void LogVerbose(const char * pcFormat, ...);

    void LogShowy(const char * pcFormat, ...);

private:
    LogFunc m_pLogFunc;
    LogLevel m_eLogLevel;
    std::mutex m_oMutex;
};
    
}
