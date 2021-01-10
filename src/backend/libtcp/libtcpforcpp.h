#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <cinttypes>

extern int connectTo(const char *ip, uint16_t port);

extern uint64_t getTimestamp();

extern int closeConnection();

#ifdef __cplusplus
}
#endif
