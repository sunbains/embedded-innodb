#include <new>
#include "innodb0types.h"

void *ut_new_func(ulint n, Source_location location) {
  return new (std::nothrow) std::byte[n]();
}

void ut_delete_func(void *p, Source_location location) {
  delete [] static_cast<std::byte*>(p);
}
