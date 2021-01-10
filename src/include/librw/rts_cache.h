// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#ifndef RTS_CACHE
#define RTS_CACHE
#include <limits>
#include <string>
#include <vector>
#include <map>

#include "librw/spin_lock.h"
#include "librw/key_range.h"
#include "librw/hash.h"

namespace librw
{

class RtsEnt : public KeyRangeEnt
{
public:
    RtsEnt() : KeyRangeEnt(), rts(0){};
    RtsEnt(const Slice Sk, const Slice Ek, uint64_t timestamp) : KeyRangeEnt(Sk, Ek), rts(timestamp){};
    uint64_t rts;
};

class RtsCache
{
public:
    RtsCache(){};
    //C use
    int addWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t timestamp);
    uint64_t getRts(char *skey, int skey_size, char *ekey, int ekey_size);
    //C++ use
    int addWithMutex(const Slice &StartKey, const Slice &EndKey, uint64_t timestamp);
    int add(const Slice &StartKey, const Slice &EndKey, uint64_t timestamp);
    uint64_t getRts(const Slice &StartKey, const Slice &EndKey);

private:
    spin_mutex lock_;
    std::map<const Slice, RtsEnt, cmp_slice> RtsCaches;
};

} // namespace librw
#endif // !RTS_CACHE
