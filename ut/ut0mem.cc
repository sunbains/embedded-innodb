/****************************************************************************
Copyright (c) 1994, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2024 Sunny Bains. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place, Suite 330, Boston, MA 02111-1307 USA

*****************************************************************************/

/** @file ut/ut0mem.c
Memory primitives

Created 5/11/1994 Heikki Tuuri
*************************************************************************/

#include "ut0mem.h"

#include "ut0logger.h"

#include <cstdlib>
#include <cstring>

void *ut_new_func(ulint n, SourceLocation location) {
  auto ptr = std::calloc(1, n);

  if (ptr == nullptr) {
    log_fatal(std::format("Cannot allocate {} bytes of memory at {}: {}", n, location.to_string(), std::strerror(errno)));
  }

  return ptr;
}

void ut_delete_func(void *ptr, SourceLocation) {
  std::free(ptr);
}

void *ut_realloc_func(void *ptr, ulint n, SourceLocation location) {
  if (n == 0) {
    std::free(ptr);
    return nullptr;
  }

  auto new_ptr = std::realloc(ptr, n);

  if (new_ptr == nullptr) {
    log_fatal(std::format("Cannot reallocate {} bytes of memory at {}: {}", n, location.to_string(), std::strerror(errno)));
  }

  return new_ptr;
}

ulint ut_strlcpy(char *dst, const char *src, ulint size) noexcept {
  ulint src_size = strlen(src);

  if (size != 0) {
    ulint n = std::min<ulint>(src_size, size - 1);

    memcpy(dst, src, n);
    dst[n] = '\0';
  }

  return src_size;
}

ulint ut_strlcpy_rev(char *dst, const char *src, ulint size) {
  ulint src_size = strlen(src);

  if (size != 0) {
    ulint n = std::min<ulint>(src_size, size - 1);

    memcpy(dst, src + src_size - n, n + 1);
  }

  return src_size;
}

