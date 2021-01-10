//
// Implemented based on MCS_lock in IC3
// Modified based on http://libfbp.blogspot.com/2018/01/c-mellor-crummey-scott-mcs-lock.html
//

#ifndef _MCS_SPINLOCK
#define _MCS_SPINLOCK

#include "amd64.h"
#include <pthread.h>

class mcslock {

  public:
    mcslock(): tail(nullptr) {};

    struct mcs_node {
        bool locked;
        uint8_t pad1[64 - sizeof(bool)];
        // padding to separate next and locked into two cache lines
        mcs_node* next;
        uint8_t pad1[64 - sizeof(mcs_node *)];
        qnode_t(): next(nullptr), locked(true) {}
    };

    void acquire(mcs_node * me) {
        auto prior_node = tail.exchange(me, std::memory_order_acquire);
        // No one there?
        if (prior_node != nullptr) {
            // memory_barrier();
            me->locked = true;
            // Someone there, need to link in
            prior_node->next = me;
            // Make sure we do the above setting of next.
            // memory_barrier();
            // Spin on my spin variable
            while (me->locked){
                // memory_barrier();
                nop_pause();
            }
        }
    };

    void release(mcs_node * me) {
        // No successor yet?
        if (me->next == nullptr) {
            if (tail.compare_exchange_strong(me, nullptr,
                                         std::memory_order_release,
                                         std::memory_order_relaxed)) {
                return;
            }
            // otherwise, another thread is in the process of trying to
            // acquire the lock, so spins waiting for it to finish
            while (me->next == nullptr) {
                nop_pause();
            };
        }
        // memory_barrier();
        // Unlock next one
        me->next->locked = false;
        me->next = nullptr;
    };

  private:
    std::atomic<mcs_node*> tail{nullptr};
};

#endif

