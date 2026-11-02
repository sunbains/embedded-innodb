/** Copyright (c) 2024 Sunny Bains. All rights reserved. */

#include <stdio.h>
#include <stdlib.h>

#include <vector>

#include "innodb0types.h"

#include "lock0lock.h"
#include "srv0srv.h"
#include "srv0sys.h"
#include "trx0trx.h"

constexpr int N_TRXS = 8;
constexpr int N_ROW_LOCKS = 1;
constexpr int REC_BITMAP_SIZE = 104;

namespace test {

/** Creates and initializes a transaction instance
@return	own: the transaction */
Trx *trx_create(System &sys) {
  return sys.m_trx_sys->create_user_trx(nullptr);
}

/** Free the transaction object.
@param[in,own] trx              Free the transaction. */
void trx_free(System &sys, Trx *&trx) {
  sys.m_trx_sys->destroy_user_trx(trx);
  ut_a(trx == nullptr);
}

/** Setup the test transaction for the simulation.
@param[in,out] trx              Transaction to setup.
@param[in] n_row_locks          Number of row locks to create. */
void trx_setup(System &sys, Trx *trx, int n_row_locks) {
  for (int i = 0; i < n_row_locks; ++i) {
    auto mode = LOCK_S;
    space_id_t space = random() % 100;
    page_no_t page_no = random() % 1000;
    auto heap_no = random() % REC_BITMAP_SIZE;

    if (!(i % 50)) {
      mode = LOCK_X;
    }
    ut_a(trx->m_trx_sys == sys.m_trx_sys.get());

    mutex_enter(&sys.m_trx_sys->m_mutex);
    std::cout << "REC LOCK CREATE: " << i << "\n";

    /* Pass nullptr index handle. */
    (void)sys.m_lock_sys->rec_create_low({space, page_no}, mode, heap_no, REC_BITMAP_SIZE, nullptr, trx);

    mutex_exit(&sys.m_trx_sys->m_mutex);
  }
}

/** Create N_TRXS transactions and create N_ROW_LOCKS rec locks on
random space/page_no/heap_no. Set the wait bit for locks that
clash. Select a random transaction and check if there are any
other transactions that are waiting on its locks. */
void run_1(System &sys) {

  std::cout << "Creating " << N_TRXS << " trxs with " << N_ROW_LOCKS << " random row locks\n";

  auto start = time(nullptr);
  auto trxs = std::vector<Trx *>{};

  trxs.resize(N_TRXS);

  for (auto &trx : trxs) {
    trx = trx_create(sys);
    trx_setup(sys, trx, N_ROW_LOCKS);
  }

  size_t no_waiters{};
  auto end = time(nullptr);

  std::cout << N_TRXS << " Transactions created in " << int(end - start) << " secs\n";

  start = time(nullptr);

  for (auto &trx : trxs) {
    if (sys.m_lock_sys->trx_has_no_waiters(trx)) {
      ++no_waiters;
      std::cout << "Trx " << trx->m_id << " has no waiters\n";
    }
  }

  end = time(nullptr);

  for (auto &trx : trxs) {

    mutex_enter(&sys.m_trx_sys->m_mutex);

    sys.m_lock_sys->release_off_trx_sys_mutex(trx);

    mutex_exit(&sys.m_trx_sys->m_mutex);

    trx_free(sys, trx);
  }

  std::cout << no_waiters << " trx had no waiters. Total time to check: " << int(end - start) << "secs avg"
            << (int((end - start) * 1000) / N_TRXS) << "\n";
}

}  // namespace test

int main() {
  srandom(time(nullptr));

  Config config;

  config.m_srv.m_max_n_threads = N_TRXS;
  config.m_buf_pool.m_size = 64 * 1024 * 1024;
  config.m_lock.m_table_size = 1024 * 1024;

  /* init() creates the sub-systems; nothing touches the disk until startup(),
  which this test doesn't need. */
  System sys;

  auto err = sys.init(config);
  ut_a(err == DB_SUCCESS);

  test::run_1(sys);

  exit(EXIT_SUCCESS);
}
