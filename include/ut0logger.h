/** Copyright (c) 2024 Sunny Bains. All rights reserved. */

#pragma once

#include <cstdio>

#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <syncstream>
#include <thread>

#include <libgen.h>
#include <cassert>
#include <cstring>

namespace logger {

enum Level { Debug, Info, Warn, Error, Fatal };

/** Name of the program, printed in the message header. Defined by the application. */
extern const char *Progname;

/** Messages below this level are suppressed. Defined by the application. */
extern int level;

inline std::string now() noexcept {
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  std::tm local_time{};

  localtime_r(&time_t, &local_time);

  char buffer[64];
  std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &local_time);

  return std::string{buffer};
}

inline std::string func_name(const char *filename, int line) noexcept {
  auto len = strlen(filename);
  assert(len > 0);

  auto ptr = filename + (len - 1);

  while (ptr != filename && *ptr != '/') {
    --ptr;
  }

  std::string location{*ptr == '/' ? ptr + 1 : ptr};
  location.push_back(':');
  location.append(std::format("{}", line));

  return location;
}

template <typename... Args>
std::ostream &write(std::ostream &out, Args &&...args) noexcept {
  ((out << std::forward<Args>(args)), ...);
  return out;
}

template <typename Now = std::string, typename Func = std::string, typename... Args>
std::ostream &write(std::string &&now, std::string &&func, const char *level, bool hdr, bool eol, Args &&...args) noexcept {
  std::stringstream ss{};

  if (hdr) {
    ss << now << " " << level << " " << Progname << " " << func << " " << std::this_thread::get_id() << " ";
  }

  (write(ss, std::forward<Args>(args)), ...);

  if (eol) {
    ss << std::endl;
  }

  std::osyncstream out{std::cerr};

  out << ss.str();
  out.flush();

  return std::cerr;
}

}  // namespace logger

#define NOW logger::now()
#define FUNC logger::func_name(__FILE__, __LINE__)

#define log_dbg_hdr(...)                                                    \
  do {                                                                      \
    if (logger::level <= (int)logger::Debug) {                              \
      logger::write(std::move(NOW), FUNC, "dbg", true, false, __VA_ARGS__); \
    }                                                                       \
  } while (false)

#define log_dbg(...)                                                       \
  do {                                                                     \
    if (logger::level <= (int)logger::Debug) {                             \
      logger::write(std::move(NOW), FUNC, "dbg", true, true, __VA_ARGS__); \
    }                                                                      \
  } while (false)

#define log_dbg_msg(...)                                                     \
  do {                                                                       \
    if (logger::level <= (int)logger::Debug) {                               \
      logger::write(std::move(NOW), FUNC, "dbg", false, false, __VA_ARGS__); \
    }                                                                        \
  } while (false)

#define log_info_hdr(...)                                                              \
  do {                                                                                 \
    if (logger::level <= (int)logger::Info) {                                          \
      logger::write(std::move(NOW), std::move(FUNC), "inf", true, false, __VA_ARGS__); \
    }                                                                                  \
  } while (false)

#define log_info(...)                                                                 \
  do {                                                                                \
    if (logger::level <= (int)logger::Info) {                                         \
      logger::write(std::move(NOW), std::move(FUNC), "inf", true, true, __VA_ARGS__); \
    }                                                                                 \
  } while (false)

#define log_info_msg(...)                                                               \
  do {                                                                                  \
    if (logger::level <= (int)logger::Info) {                                           \
      logger::write(std::move(NOW), std::move(FUNC), "inf", false, false, __VA_ARGS__); \
    }                                                                                   \
  } while (false)

#define log_warn_hdr(...)                                                              \
  do {                                                                                 \
    if (logger::level <= (int)logger::Warn) {                                          \
      logger::write(std::move(NOW), std::move(FUNC), "wrn", true, false, __VA_ARGS__); \
    }                                                                                  \
  } while (false)

#define log_warn(...)                                                                 \
  do {                                                                                \
    if (logger::level <= (int)logger::Warn) {                                         \
      logger::write(std::move(NOW), std::move(FUNC), "wrn", true, true, __VA_ARGS__); \
    }                                                                                 \
  } while (false)

#define log_warn_msg(...)                                                               \
  do {                                                                                  \
    if (logger::level <= (int)logger::Warn) {                                           \
      logger::write(std::move(NOW), std::move(FUNC), "wrn", false, false, __VA_ARGS__); \
    }                                                                                   \
  } while (false)

#define log_err_hdr(...)                                                               \
  do {                                                                                 \
    if (logger::level <= (int)logger::Error) {                                         \
      logger::write(std::move(NOW), std::move(FUNC), "err", true, false, __VA_ARGS__); \
    }                                                                                  \
  } while (false)

#define log_err(...)                                                                  \
  do {                                                                                \
    if (logger::level <= (int)logger::Error) {                                        \
      logger::write(std::move(NOW), std::move(FUNC), "err", true, true, __VA_ARGS__); \
    }                                                                                 \
  } while (false)

#define log_err_msg(...)                                                                \
  do {                                                                                  \
    if (logger::level <= (int)logger::Error) {                                          \
      logger::write(std::move(NOW), std::move(FUNC), "err", false, false, __VA_ARGS__); \
    }                                                                                   \
  } while (false)

/* A fatal error always terminates the process, the level only suppresses the message. */
#define log_fatal(...)                                                                  \
  do {                                                                                  \
    if (logger::level <= (int)logger::Fatal) {                                          \
      logger::write(std::move(NOW), std::move(FUNC), "fatal", true, true, __VA_ARGS__); \
    }                                                                                   \
    ::abort();                                                                          \
  } while (false)
