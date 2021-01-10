// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#ifndef KEY_XID
#define KEY_XID
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
class KeyXidEnt : public KeyRangeEnt
{
public:
    KeyXidEnt() : writeTxn(0), isDelete(false){};
    KeyXidEnt(const Slice Sk, const Slice Ek) : KeyRangeEnt(Sk, Ek), writeTxn(0), isDelete(false){};
    KeyXidEnt(const Slice Sk, const Slice Ek, const std::vector<uint64_t> &readTxn_, uint64_t writeTxn_) : KeyRangeEnt(Sk, Ek), readTxn(readTxn_), writeTxn(writeTxn_), isDelete(false){};
    KeyXidEnt(const KeyRangeEnt &ent)
    {
        StartKey = ent.StartKey;
        EndKey = ent.EndKey;
    }
    KeyXidEnt(const KeyRangeEnt &ent, const std::vector<uint64_t> &readTxn_, uint64_t writeTxn_) : readTxn(readTxn_), writeTxn(writeTxn_)
    {
        StartKey = ent.StartKey;
        EndKey = ent.EndKey;
    }
    std::vector<uint64_t> readTxn;
    uint64_t writeTxn;
    bool isDelete = false;
};

class KeyXidCache
{
public:
    KeyXidCache(){};
    int getKeyXidSize() { return KeyXids_simple.size() + KeyXids_multi.size(); }
    int getMultiKeyXidSize() { return KeyXids_multi.size(); }
    // C use
    int addReadXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn);
    uint64_t *addWriteXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn, bool is_delete, int* size);
    int removeReadXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn);
    int removeWriteXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn);
    uint64_t *getReadXid(char *skey, int skey_size, char *ekey, int ekey_size, bool is_delete, int* size);
    uint64_t *getWriteXid(char *skey, int skey_size, char *ekey, int ekey_size, bool is_delete, int* size);
    //C++ use
    int addReadXidWithMutex(const Slice &StartKey, const Slice &EndKey, uint64_t txn, int32_t isolation);
    std::vector<uint64_t> addWriteXidWithMutex(const Slice &StartKey, const Slice &EndKey, uint64_t txn, int32_t isolation, bool is_delete);
    int removeReadXidWithMutex(const Slice &StartKey, const Slice &EndKey, uint64_t txn, int32_t isolation);
    int removeWriteXidWithMutex(const Slice &StartKey, const Slice &EndKey, uint64_t txn, int32_t isolation);
    std::vector<uint64_t> getReadXid(const Slice &StartKey, const Slice &EndKey, bool is_delete);
    std::vector<uint64_t> getWriteXid(const Slice &StartKey, const Slice &EndKey, bool is_delete);

private:
    spin_mutex lock_;
    std::map<Slice, KeyXidEnt, cmp_slice> KeyXids_simple;
    std::unordered_map<KeyRangeEnt, KeyXidEnt, key_range_hash, key_range_equal> KeyXids_multi;
    int addReadXid(const Slice &Key, uint64_t txn);
    int addReadXid(const Slice &StartKey, const Slice &EndKey, uint64_t txn);
    uint64_t addWriteXid(const Slice &Key, uint64_t txn);
    uint64_t addWriteXid(const Slice &StartKey, const Slice &EndKey, uint64_t txn);
    int removeReadXid(const Slice &Key, uint64_t txn);
    int removeReadXid(const Slice &StartKey, const Slice &EndKey, uint64_t txn);
    int removeWriteXid(const Slice &Key, uint64_t txn);
    int removeWriteXid(const Slice &StartKey, const Slice &EndKey, uint64_t txn);
    int removeAllReadXid(uint64_t txn);
    int removeAllWriteXid(uint64_t txn);

    std::vector<uint64_t> getSingleReadXid(const Slice &StartKey, const Slice &EndKey, bool is_delete);
    std::vector<uint64_t> getMultiReadXid(const Slice &StartKey, const Slice &EndKey);
    std::vector<uint64_t> getSingleWriteXid(const Slice &StartKey, const Slice &EndKey, bool is_delete);
    std::vector<uint64_t> getMultiWriteXid(const Slice &StartKey, const Slice &EndKey);
};

} // namespace librw
#endif // !RTS_CACHE
