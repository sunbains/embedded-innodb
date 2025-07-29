/** Copyright (c) 2024 Sunny Bains. All rights reserved. */

#pragma once

#include "innodb.h"

#include <iostream>
#include <sstream>
#include <string>
#include <syncstream>
#include <thread>
#include <chrono>
#include <iomanip>

#include <libgen.h>
#include <cstring>
#include <cassert>

namespace logger {

inline std::string now() noexcept {
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  std::tm local_time = *std::localtime(&time_t);

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

  assert(ptr != filename && *ptr == '/');

  std::string location{ptr + 1};
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

#define log_dbg_hdr(args...)                       \
  do { \
    if (logger::level <= (int)logger::Debug) { \
      logger::write(std::move(NOW), FUNC, "dbg", true, false, args); \
    } \
  } while (false)

#define log_dbg(args...)                           \
  do { \
    if (logger::level <= (int)logger::Debug) { \
      logger::write(std::move(NOW), FUNC, "dbg", true, true, args); \
    } \
  } while (false)

#define log_dbg_msg(args...)                       \
  do { \
    if (logger::level <= (int)logger::Debug) { \
      logger::write(std::move(NOW), FUNC, "dbg", false, false, args); \
    } \
  } while (false)

#define log_info_hdr(args...)                      \
  do { \
    if (logger::level <= (int)logger::Info) { \
      logger::write(std::move(NOW), std::move(FUNC), "inf", true, false, args); \
    } \
  } while (false)

#define log_info(args...)                          \
  do { \
    if (logger::level <= (int)logger::Info) { \
      logger::write(std::move(NOW), std::move(FUNC), "inf", true, true, args); \
    } \
  } while (false)

#define log_info_msg(args...)                      \
  do { \
    if (logger::level <= (int)logger::Info) { \
      logger::write(std::move(NOW), std::move(FUNC), "inf", false, false, args); \
    } \
  } while (false)

#define log_warn_hdr(args...)                      \
  do { \
    if (logger::level <= (int)logger::Warn) { \
      logger::write(std::move(NOW), std::move(FUNC), "wrn", true, false, args); \
    }\
  } while (false)

#define log_warn(args...)                          \
  do { \
    if (logger::level <= (int)logger::Warn) { \
      logger::write(std::move(NOW), std::move(FUNC), "wrn", true, true, args); \
    } \
  } while (false)

#define log_warn_msg(args...)                      \
  do { \
    if (logger::level <= (int)logger::Warn) { \
      logger::write(std::move(NOW), std::move(FUNC), "wrn", false, false, args); \
    } \
  } while (false)

#define log_err_hdr(args...)                       \
  do { \
    if (logger::level <= (int)logger::Error) { \
      logger::write(std::move(NOW), std::move(FUNC), "err", true, false, args); \
    } \
  } while (false)

#define log_err(args...)                           \
  do { \
    if (logger::level <= (int)logger::Error) { \
      logger::write(std::move(NOW), std::move(FUNC), "err", false, true, args); \
    } \
  } while (false)

#define log_err_msg(args...)                       \
  do { \
    if (logger::level <= (int)logger::Error) { \
      logger::write(std::move(NOW), std::move(FUNC), "err", false, false, args); \
    } \
  } while (false)

#define log_fatal(args...)                         \
  do { \
    if (logger::level <= (int)logger::Fatal) { \
      logger::write(std::move(NOW), std::move(FUNC), "fatal", true, true, args); \
      ::abort(); \
    } \
  } while (false)
