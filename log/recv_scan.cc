/****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place, Suite 330, Boston, MA 02111-1307 USA

*****************************************************************************/

/** @file log/recv_scan.cc 
Recovery scanning operations

*******************************************************/

#include "log0recv.h"
#include "srv0srv.h"

static ulint recv_scan_print_counter;

static bool is_log_block_checksum_ok(const byte *block) noexcept {
  return Log::block_calc_checksum(block) == Log::block_get_checksum(block);
}

static bool recv_sys_add_to_parsing_buf(const byte *log_block, lsn_t scanned_lsn) noexcept {
  ulint more_len;

  ut_ad(scanned_lsn >= recv_sys->m_scanned_lsn);

  if (!recv_sys->m_parse_start_lsn) {
    return false;
  }

  auto data_len = Log::block_get_data_len(log_block);

  if (recv_sys->m_parse_start_lsn >= scanned_lsn) {
    return false;
  } else if (recv_sys->m_scanned_lsn >= scanned_lsn) {
    return false;
  } else if (recv_sys->m_parse_start_lsn > recv_sys->m_scanned_lsn) {
    more_len = (ulint)(scanned_lsn - recv_sys->m_parse_start_lsn);
  } else {
    more_len = (ulint)(scanned_lsn - recv_sys->m_scanned_lsn);
  }

  if (more_len == 0) {
    return false;
  }

  ut_ad(data_len >= more_len);

  auto start_offset = data_len - more_len;

  if (start_offset < LOG_BLOCK_HDR_SIZE) {
    start_offset = LOG_BLOCK_HDR_SIZE;
  }

  auto end_offset = data_len;

  if (end_offset > IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
    end_offset = IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE;
  }

  ut_ad(start_offset <= end_offset);

  if (start_offset < end_offset) {
    memcpy(recv_sys->m_buf + recv_sys->m_len, log_block + start_offset, end_offset - start_offset);

    recv_sys->m_len += end_offset - start_offset;

    ut_a(recv_sys->m_len <= RECV_PARSING_BUF_SIZE);
  }

  return true;
}

static void recv_sys_justify_left_parsing_buf() noexcept {
  memmove(recv_sys->m_buf, recv_sys->m_buf + recv_sys->m_recovered_offset, recv_sys->m_len - recv_sys->m_recovered_offset);

  recv_sys->m_len -= recv_sys->m_recovered_offset;
  recv_sys->m_recovered_offset = 0;
}

static bool recv_scan_log_recs(
  DBLWR *dblwr, ib_recovery_t recovery, ulint available_memory, bool store_to_hash, const byte *buf, ulint len, lsn_t start_lsn,
  lsn_t *contiguous_lsn, lsn_t *group_scanned_lsn
) noexcept {

  ut_ad(len >= IB_FILE_BLOCK_SIZE);
  ut_ad(len % IB_FILE_BLOCK_SIZE == 0);
  ut_ad(start_lsn % IB_FILE_BLOCK_SIZE == 0);

  auto log_block = buf;
  bool finished = false;
  auto more_data = false;
  auto scanned_lsn = start_lsn;

  do {
    auto no = Log::block_get_hdr_no(log_block);
    const auto checksum_ok{is_log_block_checksum_ok(log_block)};
    const auto block_no{Log::block_convert_lsn_to_no(scanned_lsn)};

    if (no != block_no || !checksum_ok) {
      if (no == block_no && !checksum_ok) {
        log_warn(std::format(
          "Log block no {} at lsn {} has ok header, but checksum field contains {}, should be {}",
          no,
          scanned_lsn,
          Log::block_get_checksum(log_block),
          Log::block_calc_checksum(log_block)
        ));
      }
      finished = true;
      break;
    }
    if (Log::block_get_flush_bit(log_block)) {
      if (scanned_lsn > *contiguous_lsn) {
        *contiguous_lsn = scanned_lsn;
      }
    }

    auto data_len = Log::block_get_data_len(log_block);

    if ((store_to_hash || data_len == IB_FILE_BLOCK_SIZE) && scanned_lsn + data_len > recv_sys->m_scanned_lsn &&
        recv_sys->m_scanned_checkpoint_no > 0 && Log::block_get_checkpoint_no(log_block) < recv_sys->m_scanned_checkpoint_no &&
        recv_sys->m_scanned_checkpoint_no - Log::block_get_checkpoint_no(log_block) > 0x80000000UL) {

      finished = true;
      break;
    }

    if (recv_sys->m_parse_start_lsn == 0 && Log::block_get_first_rec_group(log_block) > 0) {

      const auto parse_start_lsn = scanned_lsn + Log::block_get_first_rec_group(log_block);
      recv_sys->m_parse_start_lsn = parse_start_lsn;
      recv_sys->m_scanned_lsn = parse_start_lsn;
      recv_sys->m_recovered_lsn = parse_start_lsn;
    }

    scanned_lsn += data_len;

    if (scanned_lsn > recv_sys->m_scanned_lsn) {

      if (!recv_needed_recovery) {
        log_info("Log scan progressed past the checkpoint lsn ", recv_sys->m_scanned_lsn);
        recv_start_crash_recovery(dblwr, recovery);
      }

      if (recv_sys->m_len + 4 * IB_FILE_BLOCK_SIZE >= RECV_PARSING_BUF_SIZE) {
        log_err("Log parsing buffer overflow. Recovery may have failed!");

        recv_sys->m_found_corrupt_log = true;

        if (!srv_config.m_force_recovery) {
          log_fatal("Set innodb_force_recovery to ignore this error.");
        }

      } else if (!recv_sys->m_found_corrupt_log) {
        more_data = recv_sys_add_to_parsing_buf(log_block, scanned_lsn);
      }

      recv_sys->m_scanned_lsn = scanned_lsn;
      recv_sys->m_scanned_checkpoint_no = Log::block_get_checkpoint_no(log_block);
    }

    if (data_len < IB_FILE_BLOCK_SIZE) {
      finished = true;
      break;
    } else {
      log_block += IB_FILE_BLOCK_SIZE;
    }
  } while (log_block < buf + len && !finished);

  *group_scanned_lsn = scanned_lsn;

  if (recv_needed_recovery) {
    recv_scan_print_counter++;

    if (finished || (recv_scan_print_counter % 80 == 0)) {
      log_info("Doing recovery: scanned up to log sequence number ", *group_scanned_lsn);
    }
  }

  if (more_data && !recv_sys->m_found_corrupt_log) {
    recv_parse_log_recs(store_to_hash);

    if (store_to_hash && mem_heap_get_size(recv_sys->m_heap) > available_memory) {
      recv_apply_log_recs(dblwr, true);
    }

    if (recv_sys->m_recovered_offset > RECV_PARSING_BUF_SIZE / 4) {
      recv_sys_justify_left_parsing_buf();
    }
  }

  return finished;
}

[[maybe_unused]] static void recv_group_scan_log_recs(
  DBLWR *dblwr, ib_recovery_t recovery, log_group_t *group, lsn_t *contiguous_lsn, lsn_t *group_scanned_lsn
) noexcept {

  bool finished = false;
  auto start_lsn = *contiguous_lsn;

  while (!finished) {
    auto end_lsn = start_lsn + RECV_SCAN_SIZE;

    log_sys->group_read_log_seg(LOG_RECOVER, log_sys->m_buf, group, start_lsn, end_lsn);

    finished = recv_scan_log_recs(
      dblwr,
      recovery,
      (srv_buf_pool->m_curr_size - recv_n_pool_free_frames) * UNIV_PAGE_SIZE,
      true,
      log_sys->m_buf,
      RECV_SCAN_SIZE,
      start_lsn,
      contiguous_lsn,
      group_scanned_lsn
    );
    start_lsn = end_lsn;
  }
}
