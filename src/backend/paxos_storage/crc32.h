#pragma once

#include <stdint.h>

uint32_t crc32(uint32_t crc, const uint8_t *buf, int len, int skiplen = 1);
