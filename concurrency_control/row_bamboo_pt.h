#ifndef ROW_BAMBOO_PT_H
#define ROW_BAMBOO_PT_H

// note: RW (Write-After-Read does not form commit dependency)
#define UPDATE_RETIRE_INFO(en, prev) { \
    if (prev) { \
      if (prev->type == LOCK_SH) { \
        en->delta = false;  \
        en->is_cohead = prev->is_cohead; \
        if (!en->is_cohead) \
          en->txn->increment_commit_barriers(); \
      } else { \
        en->delta = true; \
        en->is_cohead = false; \
        en->txn->increment_commit_barriers(); } \
    } else { \
      en->is_cohead = true; \
      en->delta = false; } }

#define RETIRE_ENTRY(to_retire) { \
  LIST_RM(owners, owners_tail, to_retire, owner_cnt); \
  to_retire->next=NULL; \
  to_retire->prev=NULL; \
  UPDATE_RETIRE_INFO(to_retire, retired_tail); \
  LIST_PUT_TAIL(retired_head, retired_tail, to_retire); \
  to_retire->status = LOCK_RETIRED; \
  retired_cnt++; }

#define CHECK_ROLL_BACK(en) { \
  if (!fcw && (en->type == LOCK_EX)) { \
    en->access->orig_row->copy(en->access->orig_data); \
    fcw = en; \
  } }

// no need to be too complicated (i.e. call function) as the owner will be empty in the end
#define ABORT_ALL_OWNERS(itr) {\
  while(owners) { \
    itr = owners; \
    owners = owners->next; \
    itr->txn->set_abort(); \
    return_entry(itr); \
  } \
  owners_tail = NULL; \
  owners = NULL; \
  owner_cnt = 0; }

// try_wound(to_wound, wounder), if commited, wound failed, return wounder
#define TRY_WOUND_PT(to_wound, wounder) {\
  if (wounder->txn->wound_txn(to_wound->txn) == COMMITED) {\
    bring_next(NULL); \
    return_entry(wounder); \
    unlock(wounder); \
    return Abort; \
  } }


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
#if LATCH == LH_MCSLOCK
  mcslock::qnode_t * m_node;
  BBLockEntry(): type(LOCK_NONE), status(LOCK_DROPPED), is_cohead(false),
                 delta(true), txn(NULL), next(NULL), prev(NULL), access(NULL), m_node(NULL){};
#else
  BBLockEntry(): type(LOCK_NONE), status(LOCK_DROPPED), is_cohead(false),
  delta(true), txn(NULL), next(NULL), prev(NULL), access(NULL) {};
#endif
};

class Row_bamboo_pt {
 public:
  virtual void init(row_t * row);
  // [DL_DETECT] txnids are the txn_ids that current txn is waiting for.
  virtual RC lock_get(lock_t type, txn_man * txn, Access * access);
  virtual RC lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int
  &txncnt, Access * access);
  virtual RC lock_release(void * en, RC rc);
  virtual RC lock_retire(void * en);

 protected:
#if LATCH == LH_SPINLOCK
  pthread_spinlock_t * latch;
#elif LATCH == LH_MUTEX
  pthread_mutex_t * latch;
#else
  mcslock * latch;
#endif
  bool blatch;

  virtual bool 		        conflict_lock(lock_t l1, lock_t l2);
  virtual bool              conflict_lock_entry(BBLockEntry * l1,BBLockEntry * l2);
  virtual BBLockEntry *     get_entry(Access *);
  virtual void 		        return_entry(BBLockEntry * entry);
  virtual void		        lock(BBLockEntry * en);
  virtual void		        unlock(BBLockEntry * en);
  virtual bool              bring_next(txn_man * txn);
  virtual void              update_entry(BBLockEntry * en);
  virtual BBLockEntry *     rm_from_retired(BBLockEntry * en, bool is_abort);
  virtual BBLockEntry *     remove_descendants(BBLockEntry * en, txn_man * txn);
  row_t * _row;
  UInt32 owner_cnt;
  UInt32 waiter_cnt;
  UInt32 retired_cnt; // no need to keep retied cnt

  // owners is a double linked list
  // waiters is a double linked list
  // [waiters] head is the oldest txn, tail is the youngest txn.
  //   So new txns are inserted into the tail.
  BBLockEntry * owners;
  BBLockEntry * owners_tail;
  BBLockEntry * retired_head;
  BBLockEntry * retired_tail;
  BBLockEntry * waiters_head;
  BBLockEntry * waiters_tail;
  BBLockEntry * fcw;
};

#endif
