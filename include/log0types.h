/** Copyright (c) 2024 Sunny Bains. All rights reserved. */

#pragma once

#include "innodb0types.h"
#include "log0config.h"
#include "sync0rw.h"
#include "sync0sync.h"

#ifdef UNIV_DEBUG
/** Flag: enable debug output when writing to the log? */
extern bool log_debug_writes;
#endif /* UNIV_DEBUG */


// Forward declarations
struct Log;
struct Checkpoint;
struct LogBuffer;

struct LogFile {
  LogFile() = default;

  /** Constructor
   * @param path Path to the log file
   * @param max_size Maximum size of the log file in bytes
  */
  LogFile(const std::string &path, off_t max_size) noexcept;

  /** Move constructor */
  LogFile(LogFile &&other) noexcept;

  /** Move assignment */
  LogFile &operator=(LogFile &&other) noexcept;

  /** Copy constructor - deleted */
  LogFile(const LogFile &) = delete;

  /** Copy assignment - deleted */
  LogFile &operator=(const LogFile &) = delete;

  ~LogFile() noexcept;

  /** Convert the log file to a string.
   *
   * @return The log file as a string.
  */
  [[nodiscard]] std::string to_string() const noexcept;

  /** File descriptor */
  int m_fd{-1};

  /** Size in bytes, including the log file header */
  off_t m_max_size{};

  /** Path to the log file */
  std::string m_path{};

  /** Number of pending operations */
  std::atomic<uint32_t> m_pending_ops{0};

  /** Every file has a header. */
  alignas(LOG_METADATA_PAGE_SIZE) LogPhyFileHeader m_header{};
};

/**
 * Log virtual file consists of a number of physical files. The idea is to have
 * multiple files, currently all the same size. It hides the details of the
 * virtual offset of the file from the user's perspective and provides a unified
 * interface for read, write and fsync.
 *
 * It maps the virtual offset and LSN to a physical file and offset for both
 * read and write.
 */
struct LogVfile {
  enum class State : ulint { OK = 301, CORRUPTED = 302 };

  /* Forbidden copy and move constructors and operators. */
  LogVfile(LogVfile &&) = delete;
  LogVfile(const LogVfile &) = delete;
  LogVfile &operator=(LogVfile &&) = delete;
  LogVfile &operator=(const LogVfile &) = delete;

  LogVfile() = default;

  /** Constructor
   * @param directory Directory to store the log files
   * @param n_files Number of files that make up the vfile.
   * @param max_file_size Maximum size of the log file in bytes
   *
   * @throws std::invalid_argument if the arguments are invalid.
  */
  LogVfile(std::string directory, size_t n_files, off_t max_file_size);

  ~LogVfile() noexcept;

  /** Convert to a string.
   *
   * @return The vfile as a string.
  */
  [[nodiscard]] std::string to_string() const noexcept;

  /**
   * Opens the files.
   *
   * @return True if the files are opened successfully, false otherwise.
   */
  [[nodiscard]] bool open() noexcept;

  /**
   * Closes the files.
   *
   * @return True if the files are closed successfully, false otherwise.
   */
  [[nodiscard]] bool close() noexcept;

  /**
   * Prepares the checkpoint buffer for a write..
   *
   * @param[in] checkpoint_no The checkpoint number.
   * @param[in] checkpoint_lsn The checkpoint lsn.
   */
  void prepare_checkpoint(ulint checkpoint_no, lsn_t checkpoint_lsn) noexcept;

  /**
   * Sync reads from the files.
   *
   * @param[out] buf The buffer to read into.
   * @param[in] len The length of the data to read.
   * @param[in] off The offset to read from.
   *
   * @return True if the data is read successfully, false otherwise. */
  [[nodiscard]] bool sync_read(void *buf, size_t len, off_t off) const noexcept;

  /**
   * Sync writes to the files.
   *
   * @param[in] buf The buffer to write from.
   * @param[in] len The length of the data to write.
   * @param[in] off The offset to write to.
   *
   * @return True if the data is written successfully, false otherwise. */
  [[nodiscard]] bool sync_write(const void *buf, size_t len, off_t off) noexcept;

  /**
   * Asynchronously writes to the files.
   *
   * @param[in] buf The buffer to write from.
   * @param[in] len The length of the data to write.
   * @param[in] off The offset to write to.
   *
   * @return True if the data is written successfully, false otherwise.
   */
  [[nodiscard]] bool async_write(const void *buf, size_t len, off_t off) noexcept;

  /**
   * Force a sync of the files.
   *
   * @return True if the files are synced successfully, false otherwise.
   */
  [[nodiscard]] bool sync() noexcept;

  /**
   * Gets the checkpoint information.
   *
   * @return The checkpoint information.
   */
  [[nodiscard]] std::pair<lsn_t, uint64_t> checkpoint_get() const noexcept;

  /**
   * Writes the checkpoint information to the files.
   *
   * @param[in] checkpoint The checkpoint information.
   * @param[in] off The offset to write the checkpoint information to.
   *
   * @return True if the checkpoint information is written successfully, false otherwise. */
  [[nodiscard]] bool write_checkpoint(LogPhyCheckpoint *checkpoint, off_t off) noexcept;

  /**
   * Reads the checkpoint information from the files.
   *
   * @param[out] phy_checkpoint The checkpoint information.
   * @param[in] off The offset to read the checkpoint information from.
   *
   * @return True if the checkpoint information is read successfully, false otherwise. */
  [[nodiscard]] bool read(LogPhyCheckpoint &phy_checkpoint, off_t off) const noexcept;

  /**
   * Reads the checkpoint information from the files.
   *
   * @param[out] lsn The lsn of the checkpoint.
   * @param[out] no The number of the checkpoint.
   *
   * @return True if the checkpoint information is read successfully, false otherwise. */
  [[nodiscard]] bool read_checkpoint(lsn_t &lsn, uint64_t &no) noexcept;

  /**
   * Reads the log buffer from the files.
   *
   * @param[out] buffer The log buffer to read into.
   * @param[in] start_lsn The start lsn of the log buffer to read.
   * @param[in] end_lsn The end lsn of the log buffer to read.
   *
   * @return True if the log buffer is read successfully, false otherwise. */
  [[nodiscard]] bool read(LogBuffer &buffer, lsn_t start_lsn, lsn_t end_lsn) noexcept;

  /**
   * Writes the log buffer to the files.
   *
   * @param[in] buffer The log buffer to write from.
   * @param[in] write_lsn The lsn to write the log buffer to.
   *
   * @return True if the log buffer is written successfully, false otherwise.
   */
  [[nodiscard]] bool write(LogBuffer &buffer, lsn_t write_lsn) noexcept;

  /**
   * Flushes the header of the files.
   *
   * @param[in] header The header to flush.
   * @param[in] offset The offset to flush the header to.
   * @param[in] start_lsn The start lsn of the log buffer to flush.
   *
   * @return True if the header is flushed successfully, false otherwise. */
  [[nodiscard]] bool flush_header(byte *header, off_t offset, lsn_t start_lsn) noexcept;

  /**
   * Sets the checkpoint information.
   *
   * @param[in] file_no The file number.
   * @param[in] offset The offset to set the checkpoint information to.
   *
   * @return True if the checkpoint information is set successfully, false otherwise. */
  void checkpoint_set(ulint file_no, off_t offset) noexcept;

  /**
   * Sets the fields of the files.
   *
   * @param[in] lsn The lsn to set the fields to.
   */
  void set_fields(lsn_t lsn) noexcept;

  /**
   * Gets the capacity of the files.
   *
   * @return The capacity of the files.
   */
  [[nodiscard]] off_t get_capacity() const noexcept;

  /** Calculate the real offset of a given offset in the vfile.
   *
   * @param[in] offset The offset to calculate the real offset for.
   *
   * @return The real offset of the offset in the log vfile.
   */
  [[nodiscard]] off_t calc_real_offset(off_t offset) noexcept;

  /** Calculate the offset of a given lsn in the vfile.
   *
   * @param[in]  lsn The lsn to calculate the offset for.
   *
   * @return The offset of the lsn in the vfile.
   */
  [[nodiscard]] off_t calc_lsn_offset(lsn_t lsn) noexcept;

  /**
   * Gets the file number and offset to write to for a given lsn.
   *
   * @param[in] first_header_lsn The lsn of the first header in the vfile.
   * @param[in] lsn The lsn to get the write coordinates for.
   *
   * @return <file number, offset to write to within file number>.
   */
  [[nodiscard]] std::pair<const LogFile *, off_t> get_io_coordinates(lsn_t first_header_lsn, lsn_t lsn) const noexcept;

  /**
   * Converts a lsn to a log block number.
   *
   * @param[in]  lsn The lsn of a byte within the block.
   * @return The log block number, it is > 0 and <= 1G.
   */
  [[nodiscard]] static ulint convert_lsn_to_block_no(lsn_t lsn) noexcept {
    return ulint((lsn / LOG_DATA_PAGE_SIZE) & 0x3FFFFFFFUL) + 1;
  }

  /**
   * Calculates the size offset of a given offset in the vfile.
   *
   * @param[in] offset The offset to calculate the size offset for.
   *
   * @return The size offset of the offset in the vfile.
   */
  [[nodiscard]] off_t calc_size_offset(off_t offset) const noexcept;

  /**
   * Calculates the offset of a given lsn in the vfile.
   *
   * @param[in] lsn The lsn to calculate the offset for.
   *
   * @return The offset of the lsn in the vfile.
   */
  [[nodiscard]] off_t calc_lsn_offset(lsn_t lsn) const noexcept;

  /* FIXME: The following fields are protected by log_sys->mutex */

  /** Directory path for the vfile */
  std::string m_directory{};

  /** Number of files in the vfile */
  std::vector<LogFile> m_files{};

  /** LOG_VFILE_OK or LOG_VFILE_CORRUPTED */
  State m_state{State::OK};

  /** LSN used to fix coordinates within the vfile */
  lsn_t m_lsn{LOG_START_LSN};

  /** The offset of the above lsn */
  ulint m_lsn_offset{LogPhyFileHeader::HEADER_SIZE};

  /** Number of currently pending flush writes for this vfile */
  ulint m_n_pending_writes{};

  /** Used only in recovery: recovery scan succeeded up to
  this lsn in this vfile */
  lsn_t m_recovered_upto_lsn{};

  /** Maximum file size in bytes. */
  off_t m_max_file_size{};

  /** Whether the directory was created on startup. */
  bool m_created_directory{false};
};

struct LogCheckpoint {

  LogCheckpoint() = default;

  /* Forbidden copy and move constructors and operators. */
  LogCheckpoint(LogCheckpoint &&) = delete;
  LogCheckpoint(const LogCheckpoint &) = delete;
  LogCheckpoint &operator=(LogCheckpoint &&) = delete;
  LogCheckpoint &operator=(const LogCheckpoint &) = delete;

  ~LogCheckpoint() noexcept;

  /** Prepare the checkpoint buffer.
   *
   * @param log The log instance.
   *
   * @return LOG_CHECKPOINT_1 or LOG_CHECKPOINT_2
  */
  [[nodiscard]] ulint prepare(Log &log) noexcept;

  /** Persist the checkpoint buffer to the vfile.
   *
   * @param vfile The vfile.
   * @param write_offset The write offset (LOG_CHECKPOINT_1 or LOG_CHECKPOINT_2)
   *
   * @return True if the checkpoint buffer is persisted successfully, false otherwise.
  */
  [[nodiscard]] bool persist(LogVfile &vfile, off_t write_offset) noexcept;

  /** Check if the checkpoint is consistent.
   *
   * @return True if the checkpoint is consistent, false otherwise.
  */
  [[nodiscard]] bool is_consistent() const noexcept;

  /** Read the checkpoint from the log file.
   *
   * @param offset The offset of the checkpoint in the log file.
   *
   * @return True if the checkpoint is read successfully, false otherwise.
  */
  [[nodiscard]] bool read(off_t offset) noexcept;

  /**
   * Completes a checkpoint.
   */
  void complete() noexcept;

  /**
   * Completes an asynchronous checkpoint info write i/o to a log file.
   */
  void io_complete() noexcept;

  /** Convert the checkpoint to a string.
   *
   * @return The checkpoint as a string.
  */
  [[nodiscard]] std::string to_string() const noexcept;

  /** When this recommended value for lsn - srv_buf_pool->get_oldest_modification()
  s exceeded, we start an asynchronous preflush of pool pages */
  ulint m_max_modified_age_async{};

  /** When this recommended value for lsn - srv_buf_pool->get_oldest_modification()
  is exceeded, we start a synchronous preflush of pool pages */
  ulint m_max_modified_age_sync{};

  /** Administrator-specified checkpoint interval in terms of log growth in
  bytes; the interval actually used by the database can be smaller */
  ulint m_adm_interval{};

  /** When this checkpoint age is exceeded we start an asynchronous writing
  of a new checkpoint */
  ulint m_max_age_async{};

  /** This is the maximum allowed value for lsn - last_checkpoint_lsn when a new query step is started */
  ulint m_max_age{};

  /** Next checkpoint number */
  lsn_t m_next_no{};

  /** Latest checkpoint lsn */
  lsn_t m_last_lsn{};

  /** Next checkpoint lsn */
  lsn_t m_next_lsn{};

  /** Number of currently pending checkpoint writes */
  ulint m_n_pending_writes{};

  /** This latch is x-locked when a checkpoint write is running; a thread should wait for this without owning the log mutex */
  RwLock m_lock{};

  /** Checkpoint data. */
  alignas(LOG_METADATA_PAGE_SIZE) LogPhyCheckpoint m_phy_checkpoint{};
};
