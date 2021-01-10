#pragma once

#include <inttypes.h>

int connectTo(const char *ip, uint16_t port);

uint64_t getTimestamp();

int closeConnection();
