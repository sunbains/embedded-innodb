#pragma once

#include "innodb.h"

#include<iostream>
#include<sstream>
#include<string>
#include<syncstream>
#include<thread>

#include <cstring>
#include <libgen.h>

namespace logger {

inline std::string func_name(const char* filename, int line) noexcept {
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
std::ostream& write(std::ostream& out, Args&&... args) noexcept {
  ((out << std::forward<Args>(args)), ...);
  return out;
}

template <typename Func, typename... Args>
std::ostream& write(const char* level, Func&& func, Args&&... args) noexcept {
  std::stringstream ss{};
  ss << level << " " << Progname << " " << func << " " << std::this_thread::get_id() << " ";
  (write(ss, std::forward<Args>(args)), ...);
  ss << std::endl;

  std::osyncstream out{std::cerr};

  out << ss.str();
  out.flush();

  return std::cerr;
}

} // namespace logger

#define FUNC logger::func_name(__FILE__,__LINE__)

#define log_dbg(args...)      if (logger::level <= (int) logger::Level::Debug) { \
                                logger::write("dbg", FUNC, args); \
                              }

#define log_info(args...)     if (logger::level <= (int) logger::Level::Info) { \
                                logger::write("inf", FUNC, args); \
                              }

#define log_warn(args...)     if (logger::level <= (int) logger::Level::Warn) { \
                                logger::write("wrn", FUNC, args); \
                              }

#define log_err(args...)      if (logger::level <= (int) logger::Level::Error) { \
                                logger::write("err", FUNC, args); \
                              }

#define log_fatal(args...)    do { \
                                logger::write("fatal", FUNC, args); \
                                ::abort(); \
                              } while (false)
