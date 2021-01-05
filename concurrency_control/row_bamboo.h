#ifndef ROW_BAMBOO_H
#define ROW_BAMBOO_H

// note: RW (Write-After-Read does not form commit dependency)
#define RECHECK_RETIRE_INFO(en, prev) { \
  bool is_cohead = en->is_cohead; \
  if (prev) { \
    if (prev->type == LOCK_SH) { \
      en->delta = false;  \
      en->is_cohead = prev->is_cohead; \
      if (!en->is_cohead && is_cohead) \
        en->txn->increment_commit_barriers(); \
      if (en->is_cohead && !is_cohead) {\
        en->txn->decrement_commit_barriers(); \
        record_benefit(get_sys_clock() - en->txn->start_ts); } \
    } else { \
      en->delta = true; \
      en->is_cohead = false; \
      if (is_cohead) \
        en->txn->increment_commit_barriers(); } \
  } else { \
    en->is_cohead = true; \
    en->delta = false; \
    if (!is_cohead) \
      en->txn->decrement_commit_barriers(); \
      record_benefit(get_sys_clock() - en->txn->start_ts);} \
}

#define UPDATE_RETIRE_INFO(en, prev) { \
  if (prev) { \
    if (prev->type == LOCK_SH) { \
      en->delta = false;  \
      en->is_cohead = prev->is_cohead; \
      if (!en->is_cohead) { \
        en->start_ts = get_sys_clock(); \
        en->txn->increment_commit_barriers(); \
      } else { \
        record_benefit(0);} \
    } else { \
      en->delta = true; \
      en->is_cohead = false; \
      en->start_ts = get_sys_clock(); \
      en->txn->increment_commit_barriers(); } \
  } else { \
    record_benefit(0); \
    en->is_cohead = true; \
    en->delta = false; } }

// used by lock_retire() (move from owners to retired)
// or by lock_acquire(), used when has no owners but directly enters retired
// for the latter need to call UPDATE_RETIRE_INFO(to_insert, retired_tail);
#define ADD_TO_RETIRED_TAIL(to_retire) { \
  LIST_PUT_TAIL(retired_head, retired_tail, to_retire); \
  to_retire->status = LOCK_RETIRED; \
  retired_cnt++; }


#define INSERT_TO_RETIRED_TAIL(to_insert) { \
  UPDATE_RETIRE_INFO(to_insert, retired_tail); \
  RECHECK_RETIRE_INFO(owners, to_insert); \
  LIST_PUT_TAIL(retired_head, retired_tail, to_insert); \
  to_insert->status = LOCK_RETIRED; \
  retired_cnt++; }


#define INSERT_TO_RETIRED(to_insert, en) { \
  UPDATE_RETIRE_INFO(to_insert, en->prev); \
  RECHECK_RETIRE_INFO(en, to_insert); \
  LIST_INSERT_BEFORE_CH(retired_head, en, to_insert); \
  to_insert->status = LOCK_RETIRED; \
  retired_cnt++; \
}

#define ADD_TO_WAITERS(en, to_insert) { \
  rc = WAIT; \
  en = waiters_head; \
  while (en != NULL) { \
    if (ts < en->txn->get_ts()) \
      break; \
    en = en->next; \
  } \
  if (en) { \
    LIST_INSERT_BEFORE(en, to_insert); \
    if (en == waiters_head) \
      waiters_head = to_insert; \
  } else { \
    LIST_PUT_TAIL(waiters_head, waiters_tail, to_insert); \
  } \
  to_insert->status = LOCK_WAITER; \
  waiter_cnt ++; \
}

#define ADD_TO_WAITERS_TAIL(to_insert) { \
  rc = WAIT; \
  LIST_PUT_TAIL(waiters_head, waiters_tail, to_insert); \
  to_insert->status = LOCK_WAITER; \
  waiter_cnt ++; \
}

#define RETIRE_ENTRY(to_retire) { \
  to_retire = owners; \
  owners = NULL; \
  to_retire->next=NULL; \
  to_retire->prev=NULL; \
  ADD_TO_RETIRED_TAIL(to_retire); }

#define CHECK_ROLL_BACK(en) { \
    en->access->orig_row->copy(en->access->orig_data); \
  }

// try_wound(to_wound, wounder), if commited, wound failed, return wounder
#define TRY_WOUND(to_wound, wounder) { \
  if (wounder->txn->wound_txn(to_wound->txn) == COMMITED) {\
    return_entry(wounder); \
    rc = Abort; \
    goto final; \
  } \
}

// NOTE: it is unrealistic to have completely ordered read with
// dynamically assigned ts. e.g. [0,0,0] -> [12, 11, 5]
#define WOUND_RETIRED(en, to_insert) { \
    en = retired_head; \
    for (UInt32 i = 0; i < retired_cnt; i++) { \
        if (en->type == LOCK_EX && (en->txn->get_ts() < ts)) { \
            TRY_WOUND(en, to_insert); \
            en = rm_from_retired(en, true, txn); \
        } else \
            en = en->next; \
    } \
}

// owner's type is always LOCK_EX
#define WOUND_OWNER(to_insert) { \
    TRY_WOUND(owners, to_insert); \
    return_entry(owners); \
    owners = NULL; \
}

#define BRING_OUT_WAITER(entry) { \
	LIST_RM(waiters_head, waiters_tail, entry, waiter_cnt); \
	entry->txn->lock_ready = true; \
	if (txn == entry->txn) \
		has_txn = true; \
} 

struct BBLockEntry {
    // type of lock: EX or SH
    lock_t type;
    lock_status status;
    bool is_cohead;
    bool delta; // if conflict with prev
    txn_man * txn;
    BBLockEntry * next;
    BBLockEntry * prev;
    Access * access;
    ts_t start_ts;
#if LATCH == LH_MCSLOCK
    mcslock::qnode_t * m_node;
    BBLockEntry(): type(LOCK_NONE), status(LOCK_DROPPED), is_cohead(false),
                   delta(true), txn(NULL), next(NULL), prev(NULL), access(NULL),
                   start_ts(0), m_node(NULL){};
#else
    BBLockEntry(): type(LOCK_NONE), status(LOCK_DROPPED), is_cohead(false),
    delta(true), txn(NULL), next(NULL), prev(NULL), access(NULL) start_ts(0) {};
#endif
};

class Row_bamboo {
  public:
    void init(row_t * row);
    RC lock_get(lock_t type, txn_man * txn, Access * access);
    RC lock_release(void * en, RC rc);
    RC lock_retire(void * en);

  private:

    // data structure
    BBLockEntry * owners;
    BBLockEntry * retired_head;
    BBLockEntry * retired_tail;
    BBLockEntry * waiters_head;
    BBLockEntry * waiters_tail;
    row_t * _row;
    UInt32 waiter_cnt;
    UInt32 retired_cnt;
    UInt32 benefit_cnt1;
    UInt32 benefit_cnt2;
    UInt32 benefit1;
    UInt32 benefit2;
    bool curr_benefit1;

    // helper functions
    bool              bring_next(txn_man * txn);
    void              update_entry(BBLockEntry * en);
    BBLockEntry *     rm_from_retired(BBLockEntry * en, bool is_abort, txn_man * txn);
    BBLockEntry *     remove_descendants(BBLockEntry * en, txn_man * txn);
    void              lock(BBLockEntry * en);
    void              unlock(BBLockEntry * en);
	RC                insert_read_to_retired(BBLockEntry * to_insert, ts_t ts, Access * access);

    // latches
#if LATCH == LH_SPINLOCK
    pthread_spinlock_t * latch;
#elif LATCH == LH_MUTEX
    pthread_mutex_t * latch;
#else
    mcslock * latch;
#endif
    bool blatch;

    // helper functions
    inline static int assign_ts(ts_t ts, txn_man * txn) {
        if (ts == 0) {
            ts = txn->set_next_ts(1);
            // if fail to assign, reload
            if ( ts == 0 )
                ts = txn->get_ts();
        }
        return ts;
    }

    // init a lock entry (pre-allocated in each txn's access)
    BBLockEntry * get_entry(Access * access) {
        BBLockEntry * entry = (BBLockEntry *) access->lock_entry;
        entry->txn->lock_ready = false;
        entry->txn->lock_abort = false;
        entry->next = NULL;
        entry->prev = NULL;
        entry->status = LOCK_DROPPED;
        entry->is_cohead = false;
        entry->delta = true;
        return entry;
    };

    // clean the lock entry
    void return_entry(BBLockEntry * entry) {
        entry->next = NULL;
        entry->prev = NULL;
        entry->status = LOCK_DROPPED;
    }

    // record benefit
    inline void record_benefit(uint64_t time) {
        if (curr_benefit1) {
            if (benefit_cnt1 == 10) {
                curr_benefit1 = false;
                if (benefit_cnt2 == 10) {
                    // clear benefit2
                    benefit2 = time;
                    benefit_cnt2 = 1;
                } else {
                    benefit2 += time;
                    benefit_cnt2 += 1;
                }
            } else {
                benefit1 += time;
                benefit_cnt1 += 1;
            }
        } else {
            if (benefit_cnt2 == 10) {
                curr_benefit1 = true;
                if (benefit_cnt1 == 10) {
                    // clear benefit1
                    benefit1 = time;
                    benefit_cnt1 = 1;
                } else {
                    benefit1 += time;
                    benefit_cnt1 += 1;
                }
            } else {
                benefit2 += time;
                benefit_cnt2 += 1;
            }
        };
    };
};

#endif
