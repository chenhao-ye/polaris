//
// Implemented by authors of IC3
//

#ifndef _MCS_SPINLOCK
#define _MCS_SPINLOCK

#include "amd64.h"
#include <pthread.h>

class mcslock {

  public:
    struct qnode_t {
        volatile qnode_t * volatile next;
        volatile bool locked;
        constexpr qnode_t(): next(NULL), locked(true){}
    };

  private:
    struct lock_t {
        qnode_t* tail;
        lock_t(): tail(NULL){}
    };

    volatile lock_t lock_;
  public:

    static void acquire(qnode_t * me) {
        qnode_t * tail = NULL;
        me->next = NULL;
        tail = __sync_lock_test_and_set(&lock_.tail, me);
        // memory_barrier();
        // No one there?
        if (tail) {
            // memory_barrier();
            me->locked = true;
            // Someone there, need to link in
            tail->next = me;
            // Make sure we do the above setting of next.
            // memory_barrier();
            // Spin on my spin variable
            while (me->locked){
                // memory_barrier();
                nop_pause();
            }
        }
    }

    void release(qnode_t* me) {
        // No successor yet?
        if (!me->next) {
            // Try to atomically unlock
            if (__sync_bool_compare_and_swap(&lock_.tail, me, NULL))
                return;
        }
        // Wait for successor to appear
        while (!me->next){
            nop_pause();
        }
        memory_barrier();
        // Unlock next one
        me->next->locked = false;
    }

};

template <typename MCSLockable>
class mcs_lock_guard {

  public:
    mcs_lock_guard(MCSLockable *l, mcslock::qnode_t* args)
        : l(l), node(args) {
        if (likely(l)) {
            l->mcs_lock(node);
        }
    }

    ~mcs_lock_guard() {
        if (likely(l))
            l->mcs_unlock(node);
    }

  private:
    MCSLockable* l;
    mcslock::qnode_t *node;
};

#endif

