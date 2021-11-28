#pragma once

#include "global.h"
#include <cstdint>
#include <cassert>

class row_t;
class table_t;
class Catalog;
class txn_man;
struct TsReqEntry;

#if CC_ALG == SILO_PRIO

#define SILO_PRIO_MAX_REF_CNT 4096

union TID_prio_t {
	uint64_t raw_bits;
	struct {
		uint32_t latch : 1;
		uint32_t prio_ver: 3;
		// currently support 16 levels of priority
		// but we could just use more bits to support more
		uint32_t prio : 4;
		// it's possible to have reference count as a separated value
		// but for now, let's just embed it for simplicity
		uint32_t ref_cnt : 12;
		uint64_t data_ver : 44;
	} tid_prio;

	TID_prio_t() {}

	TID_prio_t(uint64_t data_ver, uint32_t prio_ver) {
		tid_prio = {
			.latch = 0,
			.prio_ver = prio_ver,
			.prio = 0,
			.ref_cnt = 0,
			.data_ver = data_ver
		};
	}

private:
	void inc_ref_cnt() {
		assert(tid_prio.ref_cnt < SILO_PRIO_MAX_REF_CNT);
		++tid_prio.ref_cnt;
	}
	void dec_ref_cnt() {
		assert(tid_prio.ref_cnt > 0);
		--tid_prio.ref_cnt;
	}
	void set_ref_cnt(uint32_t cnt) { tid_prio.ref_cnt = cnt; }
	void set_prio(uint32_t prio) { tid_prio.prio = prio; }
	void inc_prio_ver() { ++tid_prio.prio_ver; }

public:
	// get functions
	bool is_locked() const { return tid_prio.latch; }
	uint32_t get_prio() const { return tid_prio.prio; }
	uint32_t get_ref_cnt() const { return tid_prio.ref_cnt; }
	uint32_t get_prio_ver() const { return tid_prio.prio_ver; }
	uint32_t get_data_ver() const { return tid_prio.data_ver; }
	bool operator==(const union TID_prio_t& rhs) { return this->raw_bits == rhs.raw_bits; }
	bool operator!=(const union TID_prio_t& rhs) { return this->raw_bits != rhs.raw_bits; }

	void lock() { tid_prio.latch = 1; }
	void unlock() { tid_prio.latch = 0; }

	// acquire/release_prio will maintain ref_cnt based on priority
	bool acquire_prio(uint32_t prio) {
		if (tid_prio.prio == prio) {
			inc_ref_cnt();
			return true;
		}
		if (tid_prio.prio > prio) {
			set_prio(prio);
			set_ref_cnt(1);
			return true;
		}
		return false;
	}

	void release_prio(uint32_t prio, uint32_t prio_ver) {
		if (tid_prio.prio != prio || tid_prio.prio_ver != prio_ver) return;
		dec_ref_cnt();
		if (tid_prio.ref_cnt == 0) {
			set_prio(0);
			inc_prio_ver();
		}
	}

	// make a rvalue copy of TID with latch bit set
	TID_prio_t get_locked_copy() {
		TID_prio_t tmp = *this;
		tmp.lock();
		return tmp;
	}
};

static_assert(sizeof(TID_prio_t) == 8, "TID_prio_t must be of size 64 bits");

class Row_silo_prio {
	TID_prio_t _tid_word_prio;
	row_t * 			_row;

public:
	enum class LOCK_STATUS: uint8_t {
		LOCK_DONE,			// lock successfully
		LOCK_ERR_TAKEN,		// fail due to it's taken by other
		LOCK_ERR_PRIO,		// fail due to priority issue
	};

	uint32_t			get_data_ver() { return _tid_word_prio.get_data_ver(); }

	void 				init(row_t * row);
	RC 					access(txn_man * txn, TsType type, row_t * local_row);
	// this write only do copy, but not TID operation
	// TID operation is done in writer_release
	void				write(row_t * data);
	void 				assert_lock() { assert(_tid_word_prio.is_locked()); }

	bool				validate(ts_t old_data_ver, bool in_write_set) {
		TID_prio_t v = _tid_word_prio;
		if (!in_write_set && v.is_locked()) return false;
		return v.get_data_ver() == old_data_ver;
	}

	// if the transaction has lower priority, lock acquisition would fail
	LOCK_STATUS 		lock(uint32_t prio) {
		TID_prio_t v;
	retry:
		v = _tid_word_prio;
		if (v.is_locked()) {
			PAUSE
			goto retry;
		}
		if (v.get_prio() > prio) return LOCK_STATUS::LOCK_ERR_PRIO;
		if (!__sync_bool_compare_and_swap(&_tid_word_prio.raw_bits, v.raw_bits,
			v.get_locked_copy().raw_bits))
			goto retry;
		return LOCK_STATUS::LOCK_DONE;
	}

	LOCK_STATUS			try_lock(uint32_t prio) {
		TID_prio_t v;
	retry:
		v = _tid_word_prio;
		if (v.is_locked()) return LOCK_STATUS::LOCK_ERR_TAKEN;
		if (v.get_prio() > prio) return LOCK_STATUS::LOCK_ERR_PRIO;
		if (!__sync_bool_compare_and_swap(&_tid_word_prio.raw_bits, v.raw_bits,
			v.get_locked_copy().raw_bits))
			goto retry;
		return LOCK_STATUS::LOCK_DONE;
	}

	// temporarily release the lock
	// only happen as a backoff in validation
	void				unlock() {
		_tid_word_prio.unlock();
	}

	// the reader only need to release its priority
	void		reader_release(uint32_t prio, uint32_t prio_ver) {
		TID_prio_t v, v2;
	retry:
		v = _tid_word_prio;
		if (v.is_locked()) return;
		v2 = v;
		v2.release_prio(prio, prio_ver);
		if (!__sync_bool_compare_and_swap(&_tid_word_prio.raw_bits, v.raw_bits, v2.raw_bits))
			goto retry;
	}

	// in the case of abort, the writer just do the same things as reader plus
	// releasing latch
	void		writer_release_abort(uint32_t prio, uint32_t prio_ver) {
		TID_prio_t v, v2;
	retry:
		v = _tid_word_prio;
		assert (v.is_locked());
		v2 = v;
		v2.unlock();
		v2.release_prio(prio, prio_ver);
		if (!__sync_bool_compare_and_swap(&_tid_word_prio.raw_bits, v.raw_bits, v2.raw_bits))
			goto retry;
	}

	// in the case of abort, the writer update the data version and reset
	// prioirty and ref_cnt
	void		writer_release_commit(uint64_t data_ver) {
		TID_prio_t v(data_ver, _tid_word_prio.get_prio_ver() + 1);
		_tid_word_prio = v;
	}
};

#endif
