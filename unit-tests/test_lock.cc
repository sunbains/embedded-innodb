/** Copyright (c) 2024 Sunny Bains. All rights reserved. */

#include <stdio.h>
#include <stdlib.h>

#include <vector>

#include "innodb0types.h"

#include "lock0lock.h"
#include "trx0trx.h"
#include "srv0srv.h"

constexpr int N_TRXS = 8;
constexpr int N_ROW_LOCKS = 1;
constexpr int REC_BITMAP_SIZE = 104;

#define kernel_mutex_enter() \
  do { \
    mutex_enter(kernel_mutex_temp); \
  } while(false)

#define kernel_mutex_exit() \
  do { \
    mutex_exit(kernel_mutex_temp); \
  } while(false)

namespace test {

/** Creates and initializes a transaction instance
@return	own: the transaction */
trx_t* trx_create() {
#if 0
  auto trx = reinterpret_cast<trx_t*>(::malloc(sizeof(trx_t)));

  trx_init(trx);
#else
  auto trx = trx_allocate_for_client(nullptr);
#endif

  return trx;
}

/** Free the transaction object.
@param[in,own] trx              Free the transaction. */
void trx_free(trx_t*& trx) {
  trx_free_for_client(trx);
  ut_a(trx == nullptr);
}

/** Setup the test transaction for the simulation.
@param[in,out] trx              Transaction to setup.
@param[in] n_row_locks          Number of row locks to create. */
void trx_setup(trx_t* trx, int n_row_locks) {
  for (int i = 0; i < n_row_locks; ++i) {
    auto mode = LOCK_S;
    space_id_t space = random() % 100;
    page_no_t page_no = random() % 1000;
    auto heap_no = random() % REC_BITMAP_SIZE;

    if (!(i % 50)) {
      mode = LOCK_X;
    }

    kernel_mutex_enter();

    std::cout << "REC LOCK CREATE: " << i << "\n";

    /* Pass nullptr index handle. */
    (void) srv_lock_sys->rec_create_low({space, page_no}, mode, heap_no, REC_BITMAP_SIZE, nullptr, trx);

    kernel_mutex_exit();
  }
}

/** Create N_TRXS transactions and create N_ROW_LOCKS rec locks on
random space/page_no/heap_no. Set the wait bit for locks that
clash. Select a random transaction and check if there are any
other transactions that are waiting on its locks. */
void run_1() {

  std::cout << "Creating " << N_TRXS << " trxs with " << N_ROW_LOCKS << " random row locks\n";

  auto start = time(nullptr);
  auto trxs = std::vector<trx_t*>{};
  
  trxs.resize(N_TRXS);

  for (auto &trx : trxs) {
    trx = trx_create();
    trx_setup(trx, N_ROW_LOCKS);
  }

  size_t no_waiters{};
  auto end = time(nullptr);

  std::cout << N_TRXS << " Transactions created in " << int(end - start) << " secs\n";

  start = time(nullptr);

  for (auto &trx : trxs) {
    if (srv_lock_sys->trx_has_no_waiters(trx)) {
      ++no_waiters;
      std::cout << "Trx " << trx->m_id << " has no waiters\n";
    }
  }

  end = time(nullptr);

  for (auto &trx : trxs) {

    kernel_mutex_enter();

    srv_lock_sys->release_off_kernel(trx);

    kernel_mutex_exit();

    trx_free(trx);
  }

  std::cout
    << no_waiters << " trx had no waiters. Total time to check: " << int(end - start)
    << "secs avg" << (int((end - start) * 1000) / N_TRXS) << "\n";

}

} // namespace test

int main() {
  srandom(time(nullptr));

  /* Note: The order of initializing and close of the sub-systems is very important. */

  // Startup
  ut_mem_init();

  os_sync_init();

  srv_max_n_threads = N_TRXS;

  sync_init();

  kernel_mutex_temp = static_cast<mutex_t *>(mem_alloc(sizeof(mutex_t)));

  mutex_create(&kernel_mutex, IF_DEBUG("kernel_mutex",) IF_SYNC_DEBUG(SYNC_KERNEL,) Source_location{});

  {
    srv_buf_pool_size = 64 * 1024 * 1024;

    srv_buf_pool = new (std::nothrow) Buf_pool();
    ut_a(srv_buf_pool != nullptr);

    auto success = srv_buf_pool->open(srv_buf_pool_size);
    ut_a(success);
  }


  srv_lock_timeout_thread_event = os_event_create(nullptr);

  srv_trx_sys = Trx_sys::create(srv_fsp); 

  srv_lock_sys = Lock_sys::create(srv_trx_sys, 1024 * 1024);

  kernel_mutex_enter();

  UT_LIST_INIT(srv_trx_sys->m_client_trx_list);

  trx_dummy_sess = sess_open();

  kernel_mutex_exit();

  // Run the test
  test::run_1();

  // Shutdown
  Lock_sys::destroy(srv_lock_sys);

  Trx_sys::destroy(srv_trx_sys);

  mutex_free(&kernel_mutex);

  mem_free(kernel_mutex_temp);
  kernel_mutex_temp = nullptr;

  os_event_free(srv_lock_timeout_thread_event);
  srv_lock_timeout_thread_event = nullptr;

  srv_buf_pool->close();

  sync_close();

  os_sync_free();

  delete srv_buf_pool;

  ut_delete_all_mem();

  exit(EXIT_SUCCESS);
}
