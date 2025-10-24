#include "innodb0types.h"

#include <libgen.h>
#include <format>

#include "ut0dbg.h"
#include "ut0logger.h"

void ut_dbg_assertion_failed(const char *expr, const char *file, ulint line) {
  log_err(std::format(" Assertion failure in file {}  line {}", ::basename((char *)file), line));

  if (expr != nullptr) {
    log_err(" see: ", expr);
  }
}
