#pragma once 

class table_t;
class Catalog;
class txn_man;
struct TsReqEntry;

#include <atomic>

#if CC_ALG == ARIA

// we implement Aria's reservation as per-record TID
union TID_aria_t {
	uint64_t raw_bits;
	struct {
		uint64_t batch_id : ARIA_NUM_BITS_BATCH_ID;
		uint32_t prio : ARIA_NUM_BITS_PRIO;
		uint64_t txn_id : ARIA_NUM_BITS_TXN_ID; // resereved by
	} tid_aria;
	TID_aria_t() = default;
	TID_aria_t(uint64_t tid_bits): raw_bits(tid_bits) {}
	TID_aria_t(uint64_t batch_id, uint32_t prio, uint64_t txn_id): \
		tid_aria({batch_id, prio, txn_id}) {}
};

class Row_aria {
	// txns are serialized as the order with these comparsion rules:
	// - if txn A has higher prio than txn B, A is serialized before B
	// - else if txn A has lower txn_id than txn B, A is serialized before B
	static bool is_order_before(uint32_t lhs_prio, uint64_t lhs_txn_id,
		uint32_t rhs_prio, uint64_t rhs_txn_id)
	{
		if (lhs_prio != rhs_prio)
			return lhs_prio > rhs_prio;
		return lhs_txn_id < rhs_txn_id;
	}

public:
	void init(row_t * row);
	RC access(txn_man * txn, TsType type, row_t * local_row);
	void write(row_t * data);

	bool reserve_write(uint64_t batch_id, uint32_t prio, uint64_t txn_id) {
		return reserve(_write_resv, batch_id, prio, txn_id);
	}
	bool validate_write(uint64_t batch_id, uint32_t prio, uint64_t txn_id) const {
		return validate(_write_resv, batch_id, prio, txn_id);
	}

#if ARIA_REORDER
	void reserve_read(uint64_t batch_id, uint32_t prio, uint64_t txn_id) {
		// we don't care return value for read reservation
		reserve(_read_resv, batch_id, prio, txn_id);
	}
	bool validate_read(uint64_t batch_id, uint32_t prio, uint64_t txn_id) const {
		return validate(_read_resv, batch_id, prio, txn_id);
	}
#endif

private:
	bool reserve(std::atomic<TID_aria_t>& resv, uint64_t batch_id,
		uint32_t prio, uint64_t txn_id)
	{
		TID_aria_t new_tid(batch_id, prio, txn_id);
		TID_aria_t v = resv.load(std::memory_order_relaxed);
	retry:
		// if no one ever reserves this record in this batch,
		// OR the one that previously has reserved the record is not ordered
		// before the current one (in which case we preempt)
		if (v.tid_aria.batch_id != batch_id \
			|| !is_order_before(v.tid_aria.prio, v.tid_aria.txn_id, prio, txn_id))
		{
			if (!resv.compare_exchange_strong(v, new_tid,
				std::memory_order_relaxed, std::memory_order_relaxed))
				goto retry;
			return true;
		}
		return false;
	}

	bool validate(const std::atomic<TID_aria_t>& resv, uint64_t batch_id,
		uint32_t prio, uint64_t txn_id) const
	{
		TID_aria_t v = resv.load(std::memory_order_relaxed);
		// compared record's TID with txn's tid:
		// - if reserved by a txn from another batch; no one reserves it in the
		//   current batch; pass
		if (v.tid_aria.batch_id != batch_id) return true;
		// - else for a validation to pass, the txn that reserves the record must
		//   not be serialized before the current txn
		return !is_order_before(v.tid_aria.prio, v.tid_aria.txn_id, prio, txn_id);
	}

private:
	std::atomic<TID_aria_t>	_write_resv;
#if ARIA_REORDER
	std::atomic<TID_aria_t>	_read_resv;
#endif
	row_t * 								_row;
};

#endif
