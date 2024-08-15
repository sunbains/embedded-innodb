/** Copyright (c) 2024 Sunny Bains. All rights reserved. */

#pragma once

#include "innodb.h"

#include <iostream>
#include <sstream>
#include <string>
#include <syncstream>
#include <thread>

#include <libgen.h>
#include <cstring>

namespace logger {

inline std::string func_name(const char *filename, int line) noexcept {
  std::string location{};
  auto fname{::strdup(filename)};
  auto ptr{::basename(fname)};

  location.append(ptr);
  location.push_back(':');

  ::free(fname);

  {
    char buf[6];
    std::snprintf(buf, sizeof(buf), "%d", line);
    location.append(buf);
  }

  return location;
}

template <typename... Args>
std::ostream &write(std::ostream &out, Args &&...args) noexcept {
  ((out << std::forward<Args>(args)), ...);
  return out;
}

template <typename Func, typename... Args>
std::ostream &write(const char *level, bool hdr, bool eol, Func &&func, Args &&...args) noexcept {
  std::stringstream ss{};

  if (hdr) {
    ss << level << " " << Progname << " " << func << " " << std::this_thread::get_id() << " ";
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

#define FUNC logger::func_name(__FILE__, __LINE__)

#define log_dbg_hdr(args...)                       \
  if (logger::level <= (int)logger::Level::Debug) {\
    logger::write("dbg", true, false, FUNC, args); \
  }

#define log_dbg(args...)                           \
  if (logger::level <= (int)logger::Level::Debug) {\
    logger::write("dbg", true, true, FUNC, args);  \
  }

#define log_dbg_msg(args...)                       \
  if (logger::level <= (int)logger::Level::Debug) {\
    logger::write("dbg", false, false, FUNC, args);\
  }

#define log_info_hdr(args...)                      \
  if (logger::level <= (int)logger::Level::Info) { \
    logger::write("inf", true, false, FUNC, args); \
  }

#define log_info(args...)                          \
  if (logger::level <= (int)logger::Level::Info) { \
    logger::write("inf", true, true, FUNC, args);  \
  }

#define log_info_msg(args...)                      \
  if (logger::level <= (int)logger::Level::Info) { \
    logger::write("inf", false, false, FUNC, args);\
  }

#define log_warn_hdr(args...)                      \
  if (logger::level <= (int)logger::Level::Warn) { \
    logger::write("wrn", true, false, FUNC, args); \
  }

#define log_warn(args...)                          \
  if (logger::level <= (int)logger::Level::Warn) { \
    logger::write("wrn", true, true, FUNC, args);  \
  }

#define log_warn_msg(args...)                      \
  if (logger::level <= (int)logger::Level::Warn) { \
    logger::write("wrn", false, false, FUNC, args);\
  }

#define log_err_hdr(args...)                       \
  if (logger::level <= (int)logger::Level::Error) {\
    logger::write("err", true, false, FUNC, args); \
  }

#define log_err(args...)                           \
  if (logger::level <= (int)logger::Level::Error) {\
    logger::write("err", false, true, FUNC, args); \
  }

#define log_err_msg(args...)                       \
  if (logger::level <= (int)logger::Level::Error) {\
    logger::write("err", false, false, FUNC, args);\
  }

#define log_fatal(args...)                         \
  do {                                             \
    logger::write("fatal", true, true, FUNC, args);\
    ::abort();                                     \
  } while (false)
