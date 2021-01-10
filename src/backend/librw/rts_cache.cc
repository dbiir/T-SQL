//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "librw/rts_cache.h"

#include <limits>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include "librw/spin_lock.h"
#include "librw/librw_cpp.h"

using namespace std;

namespace librw
{

int RtsCache::add(const Slice &StartKey, const Slice &EndKey, uint64_t timestamp)
{
    int ret = 0;
    map<const Slice, RtsEnt, cmp_slice>::iterator it;
    RtsEnt newent;
    newent.StartKey = copySlice(StartKey);
    if (EndKey == NULL || EndKey.size_ == 0)
        newent.EndKey = nextSlice(StartKey);
    else
        newent.EndKey = copySlice(EndKey);
    newent.rts = timestamp;

    map<const Slice, RtsEnt, cmp_slice>::iterator startIt, endIt;
    startIt = RtsCaches.lower_bound(newent.StartKey);
    if (startIt != RtsCaches.begin())
        startIt--;
    endIt = RtsCaches.upper_bound(newent.EndKey);
    if (endIt != RtsCaches.end())
        endIt++;

    for (it = startIt; it != endIt;)
    {
        RtsEnt &rtsent = it->second;
        if (!newent.getOverlaps(rtsent))
        {
            it++;
            continue;
        }
        int ss = StringUseSliceCompare(newent.StartKey, rtsent.StartKey);
        int ee = StringUseSliceCompare(newent.EndKey, rtsent.EndKey);

        if (rtsent.rts < timestamp)
        {
            // New: ------------
            // Old: ------------
            //
            // New: ------------
            // Old:
            if (ss == 0 && ee == 0)
            {
                rtsent.rts = timestamp;
                return ret;
            }
            // New: ------------
            // Old:   --------
            //
            // New: ------------
            // Old:
            else if (ss <= 0 && ee >= 0)
            {
                rtsent.clear();
                RtsCaches.erase(it++);
            }
            // New:   --------
            // Old: ------------
            //
            // New:   --------
            // Old: --        --
            else if (ss > 0 && ee < 0)
            {
                Slice oldEnd = rtsent.EndKey;
                rtsent.EndKey = copySlice(newent.StartKey);

                RtsEnt entl(newent.EndKey, oldEnd, timestamp);
                RtsCaches.emplace(entl.StartKey, std::move(entl));

                clearKey(oldEnd);
                it++;
            }
            // New:     --------          --------
            // Old: --------      or  ------------
            //
            // New:     --------          --------
            // Old: ----              ----
            else if (ee >= 0)
            {
                rtsent.EndKey = copySlice(newent.StartKey);
                it++;
            }
            // New: --------          --------
            // Old:     --------  or  ------------
            //
            // New: --------          --------
            // Old:         ----              ----
            else if (ss <= 0)
            {
                RtsEnt entr(newent.EndKey, rtsent.EndKey, rtsent.rts);
                RtsCaches.emplace(entr.StartKey, std::move(entr));

                rtsent.clear();
                RtsCaches.erase(it++);
            }
            else
            {
                // here means that the two range have no overlap.
            }
        }
        else if (rtsent.rts > timestamp)
        {
            // Old: -----------      -----------      -----------      -----------
            // New:    -----     or  -----------  or  --------     or     --------
            //
            // Old: -----------      -----------      -----------      -----------
            // New:
            if (ss >= 0 && ee <= 0)
            {
                return ret;
            }
            // Old:    ------
            // New: ------------
            //
            // Old:    ------
            // New: ---      ---
            else if (ss < 0 && ee > 0)
            {
                RtsEnt entl(newent.StartKey, rtsent.StartKey, timestamp);
                RtsCaches.emplace(entl.StartKey, std::move(entl));

                newent.StartKey = copySlice(rtsent.EndKey);
                it++;
            }
            // Old: --------          --------
            // New:     --------  or  ------------
            //
            // Old: --------          --------
            // New:         ----              ----
            else if (ee > 0)
            {
                newent.StartKey = copySlice(rtsent.EndKey);
                it++;
            }
            // Old:     --------          --------
            // New: --------      or  ------------
            //
            // Old:     --------          --------
            // New: ----              ----
            else if (ss < 0)
            {
                newent.EndKey = copySlice(rtsent.StartKey);
                it++;
            }
            else
            {
                // here no overlap;
                it++;
            }
        }
        else
        {
            if (ss == 0 && ee == 0)
            {
                // New: ------------
                // Old: ------------
                //
                // New: ------------
                // Old:

                //do nothing
                return ret;
            }
            else if (ss == 0 && ee > 0)
            {
                // New: ------------
                // Old: ----------
                //
                // New:           --
                // Old: ==========
                newent.StartKey = copySlice(rtsent.EndKey);
                it++;
            }
            else if (ss < 0 && ee == 0)
            {
                // New: ------------
                // Old:   ----------
                //
                // New: --
                // Old:   ==========
                newent.EndKey = copySlice(rtsent.StartKey);
                it++;
            }
            else if (ss < 0 && ee > 0)
            {
                // New: ------------
                // Old:   --------
                //
                // New: --        --
                // Old:   ========
                RtsEnt entl(newent.StartKey, rtsent.StartKey, timestamp);
                RtsCaches.emplace(entl.StartKey, std::move(entl));

                newent.StartKey = copySlice(rtsent.EndKey);
                it++;
            }
            else if (ss >= 0 && ee <= 0)
            {
                // New:     ----
                // Old: ------------
                //
                // New:
                // Old: ------------

                //do nothing
                return ret;
            }
            else if (ee > 0)
            {
                // New:     --------
                // Old: --------
                //
                // New:         ----
                // Old: --------
                newent.StartKey = copySlice(rtsent.EndKey);
                it++;
            }
            else if (ss < 0)
            {
                // New: --------
                // Old:     --------
                //
                // New: ----
                // Old:     ====----
                newent.EndKey = copySlice(rtsent.StartKey);
                it++;
            }
            else
            {
                //no overlap
                it++;
            }
        }
    }
    RtsCaches.emplace(newent.StartKey, std::move(newent));
    return ret;
}

int RtsCache::addWithMutex(const Slice &StartKey, const Slice &EndKey, uint64_t timestamp)
{
    lock_.lock();
    int ret = add(StartKey, EndKey, timestamp);
    lock_.unlock();
    return ret;
}

int RtsCache::addWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t timestamp)
{
    Slice StartKey(skey, skey_size);
    Slice EndKey(ekey, ekey_size);
    int ret = addWithMutex(StartKey, EndKey, timestamp);
    return ret;
}

uint64_t RtsCache::getRts(const Slice &StartKey, const Slice &EndKey)
{
    lock_.lock();
    map<const Slice, RtsEnt, cmp_slice>::iterator it;
    uint64_t rts = 0;
    RtsEnt newent;
    newent.StartKey = copySlice(StartKey);
    if (EndKey == NULL || EndKey.size_ == 0)
        newent.EndKey = nextSlice(StartKey);
    else
        newent.EndKey = copySlice(EndKey);

    map<const Slice, RtsEnt, cmp_slice>::iterator startIt, endIt;
    startIt = RtsCaches.lower_bound(newent.StartKey);
    if (startIt != RtsCaches.begin())
        startIt--;
    endIt = RtsCaches.upper_bound(newent.EndKey);
    if (endIt != RtsCaches.end())
        endIt++;

    for (it = startIt; it != endIt; it++)
    {
        RtsEnt rtsent = it->second;
        if (newent.getOverlaps(rtsent) &&
            rts < rtsent.rts)
            rts = rtsent.rts;
    }
    lock_.unlock();
    return rts;
}

uint64_t RtsCache::getRts(char *skey, int skey_size, char *ekey, int ekey_size)
{
    Slice StartKey(skey, skey_size);
    Slice EndKey(ekey, ekey_size);
    return getRts(StartKey, EndKey);
}
} // namespace librw

librw::RtsCache *MyRtsCache = nullptr;

int RtsCache_init()
{
    MyRtsCache = new librw::RtsCache();
    return 0;
}

int RtsCache_addWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t timestamp)
{
    return MyRtsCache->addWithMutex(skey, skey_size, ekey, ekey_size, timestamp);
}

uint64_t RtsCache_getRts(char *skey, int skey_size, char *ekey, int ekey_size)
{
    return MyRtsCache->getRts(skey, skey_size, ekey, ekey_size);
}
