//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <limits>
#include <vector>
#include <map>
#include <algorithm>
#include <unordered_map>
#include "librw/key_xid.h"
#include "librw/spin_lock.h"
#include "librw/librw_cpp.h"

using namespace std;

namespace librw
{

int KeyXidCache::addReadXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn)
{
    Slice StartKey(skey, skey_size);
    Slice EndKey(ekey, ekey_size);
    return addReadXidWithMutex(StartKey, EndKey, txn, 0);
}

int KeyXidCache::addReadXidWithMutex(const Slice &StartKey, const Slice &EndKey, uint64_t txn, int32_t isolation)
{
    int ret = 0;
    lock_.lock();
    if (EndKey == NULL || EndKey.size_ == 0)
        ret = addReadXid(StartKey, txn);
    else
        ret = addReadXid(StartKey, EndKey, txn);
    lock_.unlock();
    return ret;
}

uint64_t *KeyXidCache::addWriteXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn, bool is_delete, int* size)
{
    Slice StartKey(skey, skey_size);
    Slice EndKey(ekey, ekey_size);
    thread_local static std::vector<uint64_t> result;

    result = addWriteXidWithMutex(StartKey, EndKey, txn, 0, is_delete);
    // uint64_t arr[result.size()];
    // uint64_t *arr = new uint64_t[result.size()];
    // std::copy(result.begin(), result.end(), *arr);
    *size = result.size();
    
    return result.data();
}

std::vector<uint64_t> KeyXidCache::addWriteXidWithMutex(const Slice &StartKey, const Slice &EndKey, uint64_t txn, int32_t isolation, bool is_delete)
{
    std::vector<uint64_t> result;
    lock_.lock();
    result = getWriteXid(StartKey, EndKey, is_delete);
    if (result.size() == 0)
    {
        if (EndKey == NULL || EndKey.size_ == 0)
            addWriteXid(StartKey, txn);
        else
            addWriteXid(StartKey, EndKey, txn);
    }
    lock_.unlock();
    return result;
}

int KeyXidCache::removeReadXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn)
{
    Slice StartKey(skey, skey_size);
    Slice EndKey(ekey, ekey_size);
    return removeReadXidWithMutex(StartKey, EndKey, txn, 0);
}

int KeyXidCache::removeWriteXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn)
{
    Slice StartKey(skey, skey_size);
    Slice EndKey(ekey, ekey_size);
    return removeWriteXidWithMutex(StartKey, EndKey, txn, 0);
}

int KeyXidCache::removeReadXidWithMutex(const Slice &StartKey, const Slice &EndKey, uint64_t txn, int32_t isolation)
{
    int ret = 0;
    lock_.lock();
    if (StartKey == NULL)
        ret = removeAllReadXid(txn);
    else if (EndKey == NULL || EndKey.size_ == 0)
        ret = removeReadXid(StartKey, txn);
    else
        ret = removeReadXid(StartKey, EndKey, txn);
    lock_.unlock();
    return ret;
}

int KeyXidCache::removeWriteXidWithMutex(const Slice &StartKey, const Slice &EndKey, uint64_t txn, int32_t isolation)
{
    int ret = 0;
    lock_.lock();
    if (StartKey == NULL)
        ret = removeAllWriteXid(txn);
    else if (EndKey == NULL || EndKey.size_ == 0)
        ret = removeWriteXid(StartKey, txn);
    else
        ret = removeWriteXid(StartKey, EndKey, txn);
    lock_.unlock();
    return ret;
}

int KeyXidCache::addReadXid(const Slice &StartKey, const Slice &EndKey, uint64_t txn)
{
    int ret = 0;
    KeyRangeEnt newent(StartKey, EndKey);
    unordered_map<KeyRangeEnt, KeyXidEnt, key_range_hash, key_range_equal>::iterator it = KeyXids_multi.find(newent);

    if (it != KeyXids_multi.end())
    {
        KeyXidEnt &xident = it->second;
        xident.readTxn.insert(xident.readTxn.end(), txn);
    }
    else
    {
        KeyXidEnt newxid(newent, std::vector<uint64_t>(1, txn), 0);
        KeyXids_multi.emplace(newent, std::move(newxid));
        // TRANS_LOG_DEBUG("addReadXid do not find StartKey:%s EndKey:%s", StartKey.TO_HEX(), EndKey.TO_HEX());
    }
    return ret;
}

int KeyXidCache::addReadXid(const Slice &Key, uint64_t txn)
{
    int ret = 0;

    map<Slice, KeyXidEnt, cmp_slice>::iterator it = KeyXids_simple.find(Key);
    if (it != KeyXids_simple.end())
    {
        KeyXidEnt &xident = it->second;
        std::vector<uint64_t>::iterator exist = std::find(xident.readTxn.begin(), xident.readTxn.end(), txn);
        if (exist == xident.readTxn.end())
            xident.readTxn.insert(xident.readTxn.end(), txn);
    }
    else
    {
        KeyXidEnt newxid(Key, NULL, std::vector<uint64_t>(1, txn), 0);
        KeyXids_simple.emplace(newxid.StartKey, std::move(newxid));
        // TRANS_LOG_DEBUG("addReadXid do not find key:%s", Key.TO_HEX());
    }
    return ret;
}

uint64_t KeyXidCache::addWriteXid(const Slice &StartKey, const Slice &EndKey, uint64_t txn)
{
    uint64_t nullWriteTrans = 0;
    KeyRangeEnt newent(StartKey, EndKey);
    unordered_map<KeyRangeEnt, KeyXidEnt, key_range_hash, key_range_equal>::iterator it = KeyXids_multi.find(newent);

    if (it != KeyXids_multi.end())
    {
        KeyXidEnt &xident = it->second;
        if (xident.writeTxn != 0 && xident.writeTxn != txn)
        {
            nullWriteTrans = xident.writeTxn;
        }
        else
        {
            xident.writeTxn = txn;
        }
    }
    else
    {
        KeyXidEnt newxid(newent);
        newxid.writeTxn = txn;
        KeyXids_multi.emplace(newent, std::move(newxid));
        // TRANS_LOG_DEBUG("addWriteXid do not find StartKey:%s EndKey:%s", StartKey.TO_HEX(), EndKey.TO_HEX());
    }
    return nullWriteTrans;
}

uint64_t KeyXidCache::addWriteXid(const Slice &Key, uint64_t txn)
{
    uint64_t nullWriteTrans = 0;

    map<Slice, KeyXidEnt, cmp_slice>::iterator it = KeyXids_simple.find(Key);
    if (it != KeyXids_simple.end())
    {
        KeyXidEnt &xident = it->second;
        if (xident.writeTxn != 0 && xident.writeTxn != txn)
        {
            nullWriteTrans = xident.writeTxn;
        }
        else
        {
            xident.writeTxn = txn;
        }
    }
    else
    {
        KeyXidEnt newxid(Key, NULL);
        newxid.writeTxn = txn;
        KeyXids_simple.emplace(newxid.StartKey, std::move(newxid));
        // TRANS_LOG_DEBUG("addWriteXid do not find key:%s", Key.TO_HEX());
    }
    return nullWriteTrans;
}

int KeyXidCache::removeAllReadXid(uint64_t txn)
{
    int ret = 0;
    map<Slice, KeyXidEnt, cmp_slice>::iterator sit;

    for (sit = KeyXids_simple.begin(); sit != KeyXids_simple.end(); sit++)
    {
        // Slice key = sit->first;
        KeyXidEnt &xident = sit->second;
        // TRANS_LOG_DEBUG("removeAllReadXid txn:%lu key:%s, value:%s", txn, key.TO_HEX(), xident.StartKey.TO_HEX());
        xident.readTxn.erase(remove(xident.readTxn.begin(), xident.readTxn.end(), txn), xident.readTxn.end());
    }
    std::unordered_map<KeyRangeEnt, KeyXidEnt, key_range_hash, key_range_equal>::iterator mit;
    for (mit = KeyXids_multi.begin(); mit != KeyXids_multi.end(); mit++)
    {
        KeyXidEnt &xident = mit->second;
        xident.readTxn.erase(remove(xident.readTxn.begin(), xident.readTxn.end(), txn), xident.readTxn.end());
    }
    return ret;
}

int KeyXidCache::removeReadXid(const Slice &StartKey, const Slice &EndKey, uint64_t txn)
{
    int ret = 0;
    KeyRangeEnt newent(StartKey, EndKey);
    unordered_map<KeyRangeEnt, KeyXidEnt, key_range_hash, key_range_equal>::iterator it = KeyXids_multi.find(newent);

    if (it != KeyXids_multi.end())
    {
        KeyXidEnt &xident = it->second;
        xident.readTxn.erase(remove(xident.readTxn.begin(), xident.readTxn.end(), txn), xident.readTxn.end());
        // TRANS_LOG_DEBUG("removeReadXid xid_count:%d StartKey:%s EndKey:%s", xident.readTxn.size(), StartKey.TO_HEX(), EndKey.TO_HEX());
        if (xident.readTxn.size() == 0 && xident.writeTxn == 0)
        {
            xident.clear();
            KeyXids_multi.erase(it);
        }
    }
    return ret;
}

int KeyXidCache::removeReadXid(const Slice &Key, uint64_t txn)
{
    int ret = 0;

    map<Slice, KeyXidEnt, cmp_slice>::iterator it = KeyXids_simple.find(Key);
    if (it != KeyXids_simple.end())
    {
        KeyXidEnt &xident = it->second;
        xident.readTxn.erase(remove(xident.readTxn.begin(), xident.readTxn.end(), txn), xident.readTxn.end());
        // TRANS_LOG_DEBUG("removeReadXid xid_count:%d Key:%s", xident.readTxn.size(), Key.TO_HEX());
        if (xident.isDelete && xident.readTxn.size() == 0 && xident.writeTxn == 0)
        {
            xident.clear();
            KeyXids_simple.erase(it);
        }
    }
    return ret;
}

int KeyXidCache::removeAllWriteXid(uint64_t txn)
{
    int ret = 0;
    map<Slice, KeyXidEnt, cmp_slice>::iterator sit;

    for (sit = KeyXids_simple.begin(); sit != KeyXids_simple.end(); sit++)
    {
        KeyXidEnt &xident = sit->second;
        if (xident.writeTxn == txn)
            xident.writeTxn = 0;
    }
    std::unordered_map<KeyRangeEnt, KeyXidEnt, key_range_hash, key_range_equal>::iterator mit;
    for (mit = KeyXids_multi.begin(); mit != KeyXids_multi.end(); mit++)
    {
        KeyXidEnt &xident = mit->second;
        if (xident.writeTxn == txn)
            xident.writeTxn = 0;
    }
    return ret;
}

int KeyXidCache::removeWriteXid(const Slice &StartKey, const Slice &EndKey, uint64_t txn)
{
    int ret = 0;
    KeyRangeEnt newent(StartKey, EndKey);
    unordered_map<KeyRangeEnt, KeyXidEnt, key_range_hash, key_range_equal>::iterator it = KeyXids_multi.find(newent);

    if (it != KeyXids_multi.end())
    {
        KeyXidEnt &xident = it->second;
        if (xident.writeTxn == txn)
        {
            xident.writeTxn = 0;
        }
        if (xident.readTxn.size() == 0 && xident.writeTxn == 0)
        {
            xident.clear();
            KeyXids_multi.erase(it);
        }
    }
    return ret;
}

int KeyXidCache::removeWriteXid(const Slice &Key, uint64_t txn)
{
    int ret = 0;

    map<Slice, KeyXidEnt, cmp_slice>::iterator it = KeyXids_simple.find(Key);
    if (it != KeyXids_simple.end())
    {
        KeyXidEnt &xident = it->second;
        if (xident.writeTxn == txn)
        {
            xident.writeTxn = 0;
        }
        if (xident.isDelete && xident.readTxn.size() == 0 && xident.writeTxn == 0)
        {
            xident.clear();
            KeyXids_simple.erase(it);
        }
    }
    return ret;
}

std::vector<uint64_t> KeyXidCache::getSingleReadXid(const Slice &StartKey, const Slice &EndKey, bool is_delete)
{
    map<Slice, KeyXidEnt, cmp_slice>::iterator it;
    std::vector<uint64_t> rtxn;
    KeyXidEnt newxid;
    newxid.StartKey = copySlice(StartKey);
    if (EndKey == NULL || EndKey.size_ == 0)
        newxid.EndKey = nextSlice(StartKey);
    else
        newxid.EndKey = copySlice(EndKey);

    map<Slice, KeyXidEnt, cmp_slice>::iterator startIt, endIt;
    startIt = KeyXids_simple.lower_bound(newxid.StartKey);
    if (startIt != KeyXids_simple.begin())
        startIt--;
    endIt = KeyXids_simple.upper_bound(newxid.EndKey);

    for (it = startIt; it != endIt; it++)
    {
        KeyXidEnt &xident = it->second;
        if (newxid.getOverlaps(xident))
        {
            rtxn.insert(rtxn.end(), xident.readTxn.begin(), xident.readTxn.end());
            if (is_delete == true)
                xident.isDelete = is_delete;
        }
    }
    return rtxn;
}

std::vector<uint64_t> KeyXidCache::getMultiReadXid(const Slice &StartKey, const Slice &EndKey)
{
    unordered_map<KeyRangeEnt, KeyXidEnt, key_range_hash, key_range_equal>::iterator it;
    std::vector<uint64_t> rtxn;
    KeyXidEnt newxid;
    newxid.StartKey = copySlice(StartKey);
    if (EndKey == NULL || EndKey.size_ == 0)
        newxid.EndKey = nextSlice(StartKey);
    else
        newxid.EndKey = copySlice(EndKey);

    for (it = KeyXids_multi.begin(); it != KeyXids_multi.end(); it++)
    {
        KeyXidEnt xident = it->second;
        if (newxid.getOverlaps(xident))
        {
            rtxn.insert(rtxn.end(), xident.readTxn.begin(), xident.readTxn.end());
        }
    }
    return rtxn;
}

uint64_t *KeyXidCache::getReadXid(char *skey, int skey_size, char *ekey, int ekey_size, bool is_delete, int* size)
{
    Slice StartKey(skey, skey_size);
    Slice EndKey(ekey, ekey_size);
    thread_local static std::vector<uint64_t> result;
    result = getReadXid(StartKey, EndKey, is_delete);
    // uint64_t *arr = new uint64_t[result.size()];
    // std::copy(result.begin(), result.end(), *arr);
    *size = result.size();
    return result.data();
}

std::vector<uint64_t> KeyXidCache::getReadXid(const Slice &StartKey, const Slice &EndKey, bool is_delete)
{
    lock_.lock();
    map<Slice, KeyXidEnt, cmp_slice>::iterator it;
    std::vector<uint64_t> rtxns = getSingleReadXid(StartKey, EndKey, is_delete);
    std::vector<uint64_t> rtxnm = getMultiReadXid(StartKey, EndKey);
    rtxns.insert(rtxns.end(), rtxnm.begin(), rtxnm.end());
    lock_.unlock();
    return rtxns;
}

std::vector<uint64_t> KeyXidCache::getSingleWriteXid(const Slice &StartKey, const Slice &EndKey, bool is_delete)
{
    map<Slice, KeyXidEnt, cmp_slice>::iterator it;
    std::vector<uint64_t> rtxn;
    KeyXidEnt newxid;
    newxid.StartKey = copySlice(StartKey);
    if (EndKey == NULL || EndKey.size_ == 0)
        newxid.EndKey = nextSlice(StartKey);
    else
        newxid.EndKey = copySlice(EndKey);

    map<Slice, KeyXidEnt, cmp_slice>::iterator startIt, endIt;
    startIt = KeyXids_simple.lower_bound(newxid.StartKey);
    if (startIt != KeyXids_simple.begin())
        startIt--;
    endIt = KeyXids_simple.upper_bound(newxid.EndKey);

    for (it = startIt; it != endIt; it++)
    {
        KeyXidEnt &xident = it->second;
        if (newxid.getOverlaps(xident))
        {
            if (is_delete == true)
                xident.isDelete = is_delete;
            if (xident.writeTxn != 0)
                rtxn.push_back(xident.writeTxn);
        }
    }
    return rtxn;
}

std::vector<uint64_t> KeyXidCache::getMultiWriteXid(const Slice &StartKey, const Slice &EndKey)
{
    unordered_map<KeyRangeEnt, KeyXidEnt, key_range_hash, key_range_equal>::iterator it;
    std::vector<uint64_t> rtxn;
    KeyXidEnt newxid;
    newxid.StartKey = copySlice(StartKey);
    if (EndKey == NULL || EndKey.size_ == 0)
        newxid.EndKey = nextSlice(StartKey);
    else
        newxid.EndKey = copySlice(EndKey);

    for (it = KeyXids_multi.begin(); it != KeyXids_multi.end(); it++)
    {
        KeyXidEnt xident = it->second;
        if (newxid.getOverlaps(xident) && xident.writeTxn != 0)
        {
            rtxn.push_back(xident.writeTxn);
        }
    }
    return rtxn;
}

uint64_t *KeyXidCache::getWriteXid(char *skey, int skey_size, char *ekey, int ekey_size, bool is_delete, int* size)
{
    Slice StartKey(skey, skey_size);
    Slice EndKey(ekey, ekey_size);
    thread_local static std::vector<uint64_t> result;
    result = getWriteXid(StartKey, EndKey, is_delete);
    // uint64_t *arr = new uint64_t[result.size()];
    // std::copy(result.begin(), result.end(), *arr);
    *size = result.size();
    return result.data();
}

std::vector<uint64_t> KeyXidCache::getWriteXid(const Slice &StartKey, const Slice &EndKey, bool is_delete)
{
    // lock_.lock();
    map<Slice, KeyXidEnt, cmp_slice>::iterator it;
    std::vector<uint64_t> wtxns = getSingleWriteXid(StartKey, EndKey, is_delete);
    std::vector<uint64_t> wtxnm = getMultiWriteXid(StartKey, EndKey);
    wtxns.insert(wtxns.end(), wtxnm.begin(), wtxnm.end());
    // lock_.unlock();
    return wtxns;
}
} // namespace librw

librw::KeyXidCache *MyKeyXidCache = nullptr;

int KeyXidCache_init()
{
    MyKeyXidCache = new librw::KeyXidCache();
    return 0;
}

int KeyXidCache_addReadXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn)
{
    return MyKeyXidCache->addReadXidWithMutex(skey, skey_size, ekey, ekey_size, txn);
}

uint64_t *KeyXidCache_addWriteXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn, int is_delete, int* size)
{
    return MyKeyXidCache->addWriteXidWithMutex(skey, skey_size, ekey, ekey_size, txn, (bool)is_delete, size);
}
int KeyXidCache_removeReadXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn)
{
    return MyKeyXidCache->removeReadXidWithMutex(skey, skey_size, ekey, ekey_size, txn);
}

int KeyXidCache_removeWriteXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn)
{
    return MyKeyXidCache->removeWriteXidWithMutex(skey, skey_size, ekey, ekey_size, txn);
}

uint64_t *KeyXidCache_getReadXid(char *skey, int skey_size, char *ekey, int ekey_size, int is_delete, int* size)
{
    return MyKeyXidCache->getReadXid(skey, skey_size, ekey, ekey_size, (bool)is_delete, size);
}

uint64_t *KeyXidCache_getWriteXid(char *skey, int skey_size, char *ekey, int ekey_size, int is_delete, int* size)
{
    return MyKeyXidCache->getWriteXid(skey, skey_size, ekey, ekey_size, (bool)is_delete, size);
}
