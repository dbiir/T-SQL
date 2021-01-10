// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#ifndef KEY_RANGE
#define KEY_RANGE
#include <limits>
#include <string>
#include <vector>
#include <map>

#include "librw/spin_lock.h"
#include "librw/hash.h"
#include "librw/slice.h"
namespace librw
{

Slice copySlice(const Slice &a);
Slice nextSlice(const Slice &a);
void clearKey(Slice key);
inline int StringUseSliceCompare(const Slice a, const Slice b)
{
    return a.compare(b);
}

class KeyRangeEnt
{
public:
    Slice StartKey;
    Slice EndKey;
    KeyRangeEnt() : StartKey(), EndKey(){};
    KeyRangeEnt(const Slice Sk, const Slice Ek)
    {
        StartKey = copySlice(Sk);
        if (Ek == NULL)
            EndKey = nextSlice(Sk);
        else
            EndKey = copySlice(Ek);
    };
    ~KeyRangeEnt()
    {
    }
    bool getOverlaps(KeyRangeEnt anotherEnt);
    void clear();
};

struct cmp_slice
{
    bool operator()(const Slice &a, const Slice &b)
    {
        return a.compare(b) < 0;
    }
};

struct key_range_slice
{
    bool operator()(const KeyRangeEnt &a, const KeyRangeEnt &b)
    {
        int result = a.StartKey.compare(b.StartKey);
        if (result < 0)
        {
            return true;
        }
        else if (result > 0)
        {
            return false;
        }
        else
        {
            return a.EndKey.compare(b.EndKey);
        }
    }
};

struct key_range_hash
{
    unsigned int operator()(const KeyRangeEnt &pk) const
    {
        return hash_any((unsigned char *)pk.StartKey.data_, pk.StartKey.size_);
    }
};

struct key_range_equal
{
    bool operator()(const KeyRangeEnt &a, const KeyRangeEnt &b) const
    {
        return a.StartKey.compare(b.StartKey) == 0 && a.EndKey.compare(b.EndKey) == 0;
    }
};

} // namespace librw
#endif // !RTS_CACHE
