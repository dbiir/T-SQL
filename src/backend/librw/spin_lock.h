
#ifndef SPIN_LOCK
#define SPIN_LOCK
#include <atomic>

namespace librw
{

class spin_mutex
{
    std::atomic<bool> flag = ATOMIC_VAR_INIT(false);

public:
    spin_mutex() = default;
    spin_mutex(const spin_mutex &) = delete;
    spin_mutex &operator=(const spin_mutex &) = delete;
    void lock()
    {
        bool expected = false;
        while (!flag.compare_exchange_strong(expected, true))
            expected = false;
    }
    void unlock()
    {
        flag.store(false);
    }
};

} // namespace librw

#endif
