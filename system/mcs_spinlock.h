//
// Implemented based on MCS_lock in IC3
// Modified based on http://libfbp.blogspot.com/2018/01/c-mellor-crummey-scott-mcs-lock.html
//

#ifndef _MCS_SPINLOCK
#define _MCS_SPINLOCK

#include <atomic>
#include "amd64.h"

class mcslock {

  public:
    mcslock(): tail(nullptr) {};

    struct mcs_node {
        volatile bool locked;
        uint8_t pad0[64 - sizeof(bool)];
        // padding to separate next and locked into two cache lines
        volatile mcs_node* volatile next;
        uint8_t pad1[64 - sizeof(mcs_node *)];
        mcs_node(): locked(true), next(nullptr) {}
    };

    void acquire(mcs_node * me) {
        auto prior_node = tail.exchange(me, std::memory_order_acquire);
        // Any one there?
        if (prior_node != nullptr) {
            // memory_barrier();
            // Someone there, need to link in
            me->locked = true;
            prior_node->next = me;
            // Make sure we do the above setting of next.
            // memory_barrier();
            // Spin on my spin variable
            while (me->locked){
                // memory_barrier();
                nop_pause();
            }
            assert(!me->locked);
        }
    };

    void release(mcs_node * me) {
        if (me->next == nullptr) {
            mcs_node * expected = me;
            // No successor yet
            if (tail.compare_exchange_strong(expected, nullptr,
                                         std::memory_order_release,
                                         std::memory_order_relaxed)) {
                return;
            }
            // otherwise, another thread is in the process of trying to
            // acquire the lock, so spins waiting for it to finish
            while (me->next == nullptr) {};
        }
        // memory_barrier();
        // Unlock next one
        me->next->locked = false;
        me->next = nullptr;
    };

  private:
    std::atomic<mcs_node*> tail;
};

#endif

