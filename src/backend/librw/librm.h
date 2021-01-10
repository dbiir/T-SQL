#pragma once
#include <inttypes.h>

int KeyXidCache_init();

int KeyXidCache_addReadXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn);

uint64_t *KeyXidCache_addWriteXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn, int is_delete, int* size);

int KeyXidCache_removeReadXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn);

int KeyXidCache_removeWriteXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn);

uint64_t *KeyXidCache_getReadXid(char *skey, int skey_size, char *ekey, int ekey_size, int is_delete, int* size);

uint64_t *KeyXidCache_getWriteXid(char *skey, int skey_size, char *ekey, int ekey_size, int is_delete, int* size);

int RtsCache_init();

int RtsCache_addWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t timestamp);

uint64_t RtsCache_getRts(char *skey, int skey_size, char *ekey, int ekey_size);
