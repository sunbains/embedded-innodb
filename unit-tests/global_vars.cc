#include "ut0logger.h"
#include "os0thread.h"

std::atomic_int os_thread_count{};

namespace logger {
int level = static_cast<int>(Debug);
const char *Progname = "log-vfile-test";
}  // namespace logger
