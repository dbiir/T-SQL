#pragma once

#ifdef __cplusplus
extern "C"
{
#endif

#include <inttypes.h>

    extern int KeyXidCache_init();

    extern int KeyXidCache_addReadXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn);

    extern uint64_t *KeyXidCache_addWriteXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn, int is_delete, int* size);

    extern int KeyXidCache_removeReadXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn);

    extern int KeyXidCache_removeWriteXidWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t txn);

    extern uint64_t *KeyXidCache_getReadXid(char *skey, int skey_size, char *ekey, int ekey_size, int is_delete, int* size);

    extern uint64_t *KeyXidCache_getWriteXid(char *skey, int skey_size, char *ekey, int ekey_size, int is_delete, int* size);

    extern int RtsCache_init();

    extern int RtsCache_addWithMutex(char *skey, int skey_size, char *ekey, int ekey_size, uint64_t timestamp);

    extern uint64_t RtsCache_getRts(char *skey, int skey_size, char *ekey, int ekey_size);

#ifdef __cplusplus
}
#endif
