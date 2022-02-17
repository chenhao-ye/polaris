#include "txn.h"
#include "row.h"
#include "row_silo_prio.h"

#if CC_ALG == SILO_PRIO

RC
txn_man::validate_silo_prio()
{
	RC rc = RCOK;
	// lock write tuples in the primary key order.
	int cur_wr_idx = 0;
	int cur_rd_idx = 0;
	int write_set[wr_cnt];
	int read_set[row_cnt - wr_cnt];
	for (int rid = 0; rid < row_cnt; rid ++) {
		if (accesses[rid]->type == WR)
			write_set[cur_wr_idx ++] = rid;
		else 
			read_set[cur_rd_idx ++] = rid;
	}

	// bubble sort the write set, in primary key order
	for (int i = wr_cnt - 1; i >= 1; i--) {
		for (int j = 0; j < i; j++) {
			if (accesses[ write_set[j] ]->orig_row->get_primary_key() >
				accesses[ write_set[j + 1] ]->orig_row->get_primary_key())
			{
				int tmp = write_set[j];
				write_set[j] = write_set[j+1];
				write_set[j+1] = tmp;
			}
		}
	}

	int num_locks = 0;
	ts_t max_data_ver = 0;
	bool done = false;
	if (_pre_abort) {
		for (int i = 0; i < wr_cnt; i++) {
			row_t * row = accesses[ write_set[i] ]->orig_row;
			if (row->manager->get_data_ver() != accesses[write_set[i]]->data_ver) {
				rc = Abort;
				goto final;
			}
		}	
		for (int i = 0; i < row_cnt - wr_cnt; i ++) {
			Access * access = accesses[ read_set[i] ];
			if (access->orig_row->manager->get_data_ver() != accesses[read_set[i]]->data_ver) {
				rc = Abort;
				goto final;
			}
		}
	}

	// lock all rows in the write set.
	if (_validation_no_wait) {
		while (!done) {
			num_locks = 0;
			for (int i = 0; i < wr_cnt; i++) {
				row_t * row = accesses[ write_set[i] ]->orig_row;
				if (row->manager->try_lock(prio) != Row_silo_prio::LOCK_STATUS::LOCK_DONE)
					break;
				row->manager->assert_lock();
				num_locks ++;
				if (row->manager->get_data_ver() != accesses[write_set[i]]->data_ver)
				{
					rc = Abort;
					goto final;
				}
			}
			if (num_locks == wr_cnt)
				done = true;
			else {
				for (int i = 0; i < num_locks; i++)
					accesses[ write_set[i] ]->orig_row->manager->unlock();
				if (_pre_abort) {
					num_locks = 0;
					for (int i = 0; i < wr_cnt; i++) {
						row_t * row = accesses[ write_set[i] ]->orig_row;
						if (row->manager->get_data_ver() != accesses[write_set[i]]->data_ver) {
							rc = Abort;
							goto final;
						}	
					}	
					for (int i = 0; i < row_cnt - wr_cnt; i ++) {
						Access * access = accesses[ read_set[i] ];
						if (access->orig_row->manager->get_data_ver() != accesses[read_set[i]]->data_ver) {
							rc = Abort;
							goto final;
						}
					}
				}
                PAUSE
			}
		}
	} else {
		/**
		 * This path does not work, since release the latch requires resetting prio
		 * and prio_ver etc., we here simply disallow this operation.
		 */
		assert(false);
		for (int i = 0; i < wr_cnt; i++) {
			row_t * row = accesses[ write_set[i] ]->orig_row;
			Row_silo_prio::LOCK_STATUS ls = row->manager->lock(prio);
			if (ls == Row_silo_prio::LOCK_STATUS::LOCK_ERR_PRIO) {
				rc = Abort;
				goto final;
			}
			num_locks++;
			if (row->manager->get_data_ver() != accesses[write_set[i]]->data_ver) {
				rc = Abort;
				goto final;
			}
		}
	}

	// validate rows in the read set
	// for repeatable_read, no need to validate the read set.
	for (int i = 0; i < row_cnt - wr_cnt; i ++) {
		Access * access = accesses[ read_set[i] ];
		bool success = access->orig_row->manager->validate(access->data_ver, false);
		if (!success) {
			rc = Abort;
			goto final;
		}
		if (access->data_ver > max_data_ver)
			max_data_ver = access->data_ver;
	}
	// validate rows in the write set
	for (int i = 0; i < wr_cnt; i++) {
		Access * access = accesses[ write_set[i] ];
		bool success = access->orig_row->manager->validate(access->data_ver, true);
		if (!success) {
			rc = Abort;
			goto final;
		}
		if (access->data_ver > max_data_ver)
			max_data_ver = access->data_ver;
	}
	if (max_data_ver > _cur_data_ver)
		_cur_data_ver = max_data_ver + 1;
	else 
		_cur_data_ver ++;
final:
	// we release the priority and ref_cnt togther with the latch
	// for those rows with latch acquired (all read-only row and some write rows)
	// we release them in cleanup()
	if (rc == Abort) {
		for (int i = 0; i < num_locks; i++) {
			Access * access = accesses[ write_set[i] ];
			access->orig_row->manager->writer_release_abort(prio, access->prio_ver);
			assert(access->is_reserved);
			access->is_reserved = false;
		}
		cleanup(rc);
	} else {
		for (int i = 0; i < wr_cnt; i++) {
			Access * access = accesses[ write_set[i] ];
			access->orig_row->manager->write(access->data);
			access->orig_row->manager->writer_release_commit(_cur_data_ver);
			assert(access->is_reserved);
			access->is_reserved = false;
		}
		cleanup(rc);
	}
	return rc;
}
#endif
