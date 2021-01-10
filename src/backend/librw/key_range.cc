//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <limits>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include "librw/spin_lock.h"
#include "librw/key_range.h"
#include "librw/hash.h"
namespace librw
{

Slice copySlice(const Slice &a)
{
    size_t len = a.size_;
    char *buf = (char *)malloc(len);
    memcpy(buf, a.data_, a.size_);
    char *temp = buf + a.size_;
    return Slice(buf, len);
}

Slice nextSlice(const Slice &a)
{
    size_t len = a.size_ + 1;
    char *buf = (char *)malloc(len);
    memcpy(buf, a.data_, a.size_);
    char *temp = buf + a.size_;
    *temp = 0;
    return Slice(buf, len);
}

void clearKey(Slice key)
{
    if (key.data_ != nullptr)
    {
        char *edata = const_cast<char *>(key.data_);
        free(edata);
    }
}

bool KeyRangeEnt::getOverlaps(KeyRangeEnt anotherEnt)
{
    int se = StartKey.compare(anotherEnt.EndKey);
    int es = EndKey.compare(anotherEnt.StartKey);
    if (es > 0 && se < 0)
        return true;
    else
        return false;
}

void KeyRangeEnt::clear()
{
    if (StartKey.data_ != nullptr)
    {
        char *sdata = const_cast<char *>(StartKey.data_);
        free(sdata);
    }
    if (EndKey.data_ != nullptr)
    {
        char *edata = const_cast<char *>(EndKey.data_);
        free(edata);
    }
}
} // namespace librw
