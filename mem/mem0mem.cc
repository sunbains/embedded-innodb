/*****************************************************************************
Copyright (c) 1994, 2010, Innobase Oy. All Rights Reserved.

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

/** @file mem/mem0mem.c
The memory management

Created 6/9/1994 Heikki Tuuri
*************************************************************************/

#include <stdarg.h>

#include "buf0buf.h"
#include "srv0srv.h"

/*
                        THE MEMORY MANAGEMENT
                        =====================

The basic element of the memory management is called a memory
heap. A memory heap is conceptually a
stack from which memory can be allocated. The stack may grow infinitely.
The top element of the stack may be freed, or
the whole stack can be freed at one time. The advantage of the
memory heap concept is that we can avoid using the malloc and free
functions of C which are quite expensive, for example, on the Solaris + GCC
system (50 MHz Sparc, 1993) the pair takes 3 microseconds,
on Win NT + 100MHz Pentium, 2.5 microseconds.
When we use a memory heap,
we can allocate larger blocks of memory at a time and thus
reduce overhead. Slightly more efficient the method is when we
allocate the memory from the index page buffer pool, as we can
claim a new page fast. This is called buffer allocation.
When we allocate the memory from the dynamic memory of the
C environment, that is called dynamic allocation.

The default way of operation of the memory heap is the following.
First, when the heap is created, an initial block of memory is
allocated. In dynamic allocation this may be about 50 bytes.
If more space is needed, additional blocks are allocated
and they are put into a linked list.
After the initial block, each allocated block is twice the size of the
previous, until a threshold is attained, after which the sizes
of the blocks stay the same. An exception is, of course, the case
where the caller requests a memory buffer whose size is
bigger than the threshold. In that case a block big enough must
be allocated.

The heap is physically arranged so that if the current block
becomes full, a new block is allocated and always inserted in the
chain of blocks as the last block.

In the debug version of the memory management, all the allocated
heaps are kept in a list (which is implemented as a hash table).
Thus we can notice if the caller tries to free an already freed
heap. In addition, each buffer given to the caller contains
start field at the start and a trailer field at the end of the buffer.

The start field has the following content:
A. sizeof(ulint) bytes of field length (in the standard byte order)
B. sizeof(ulint) bytes of check field (a random number)

The trailer field contains:
A. sizeof(ulint) bytes of check field (the same random number as at the start)

Thus we can notice if something has been copied over the
borders of the buffer, which is illegal.
The memory in the buffers is initialized to a random byte sequence.
After freeing, all the blocks in the heap are set to random bytes
to help us discover errors which result from the use of
buffers in an already freed heap. */

char *mem_heap_strdup(mem_heap_t *heap, const char *str) {
  return static_cast<char *>(mem_heap_dup(heap, str, strlen(str) + 1));
}

void *mem_heap_dup(mem_heap_t *heap, const void *data, ulint len) {
  return (memcpy(mem_heap_alloc(heap, len), data, len));
}

char *mem_heap_strcat(mem_heap_t *heap, const char *s1, const char *s2) {
  auto s1_len = strlen(s1);
  auto s2_len = strlen(s2);

  auto s = reinterpret_cast<char *>(mem_heap_alloc(heap, s1_len + s2_len + 1));

  memcpy(s, s1, s1_len);
  memcpy(s + s1_len, s2, s2_len);

  s[s1_len + s2_len] = '\0';

  return s;
}

/** Helper function for mem_heap_printf.
@return	length of formatted string, including terminating NUL */
static ulint mem_heap_printf_low(
  char *buf,          /*!< in/out: buffer to store formatted string
                               in, or nullptr to just calculate length */
  const char *format, /*!< in: format string */
  va_list ap
) /*!< in: arguments */
{
  ulint len = 0;

  while (*format) {

    /* Does this format specifier have the 'l' length modifier. */
    bool is_long = false;

    /* Length of one parameter. */
    size_t plen;

    if (*format++ != '%') {
      /* Non-format character. */

      len++;

      if (buf) {
        *buf++ = *(format - 1);
      }

      continue;
    }

    if (*format == 'l') {
      is_long = true;
      format++;
    }

    switch (*format++) {
      case 's':
        /* string */
        {
          char *s = va_arg(ap, char *);

          /* "%ls" is a non-sensical format specifier. */
          ut_a(!is_long);

          plen = strlen(s);
          len += plen;

          if (buf) {
            memcpy(buf, s, plen);
            buf += plen;
          }
        }

        break;

      case 'u':
        /* unsigned int */
        {
          char tmp[32];
          unsigned long val;

          /* We only support 'long' values for now. */
          ut_a(is_long);

          val = va_arg(ap, unsigned long);

          plen = sprintf(tmp, "%lu", val);
          len += plen;

          if (buf) {
            memcpy(buf, tmp, plen);
            buf += plen;
          }
        }

        break;

      case '%':

        /* "%l%" is a non-sensical format specifier. */
        ut_a(!is_long);

        len++;

        if (buf) {
          *buf++ = '%';
        }

        break;

      default:
        ut_error;
    }
  }

  /* For the NUL character. */
  len++;

  if (buf) {
    *buf = '\0';
  }

  return (len);
}

char *mem_heap_printf(mem_heap_t *heap, const char *format, ...) {
  va_list ap;
  ulint len;

  /* Calculate length of string */
  len = 0;
  va_start(ap, format);
  len = mem_heap_printf_low(nullptr, format, ap);
  va_end(ap);

  /* Now create it for real. */
  auto str = reinterpret_cast<char *>(mem_heap_alloc(heap, len));
  va_start(ap, format);
  mem_heap_printf_low(str, format, ap);
  va_end(ap);

  return (str);
}

mem_block_t *mem_heap_create_block(mem_heap_t *heap, ulint n, ulint type, const char *file_name, ulint line) {
  buf_block_t *buf_block = nullptr;
  mem_block_t *block;
  ulint len;

  ut_ad((type == MEM_HEAP_DYNAMIC) || (type == MEM_HEAP_BUFFER) || (type == MEM_HEAP_BUFFER + MEM_HEAP_BTR_SEARCH));

  if (heap && heap->magic_n != MEM_BLOCK_MAGIC_N) {
    ut_error;
  }

  /* In dynamic allocation, calculate the size: block header + data. */

  len = mem_block_header_size() + MEM_SPACE_NEEDED(n);

  if (type == MEM_HEAP_DYNAMIC || len < UNIV_PAGE_SIZE / 2) {

    ut_a(type == MEM_HEAP_DYNAMIC || n <= MEM_MAX_ALLOC_IN_BUF);

    block = (mem_block_t *)malloc(len);
  } else {

    len = UNIV_PAGE_SIZE;

    if ((type & MEM_HEAP_BTR_SEARCH) && heap) {
      /* We cannot allocate the block from the
      buffer pool, but must get the free block from
      the heap header free block field */

      buf_block = reinterpret_cast<buf_block_t *>(heap->free_block);
      heap->free_block = nullptr;

      if (unlikely(buf_block == nullptr)) {

        return nullptr;
      }
    } else {
      buf_block = buf_pool->block_alloc();
    }

    block = (mem_block_t *)buf_block->m_frame;
  }

  ut_ad(block);
  block->buf_block = buf_block;
  block->free_block = nullptr;

  block->magic_n = MEM_BLOCK_MAGIC_N;
  ut_strlcpy_rev(block->file_name, file_name, sizeof(block->file_name));
  block->line = line;

  mem_block_set_len(block, len);
  mem_block_set_type(block, type);
  mem_block_set_free(block, mem_block_header_size());
  mem_block_set_start(block, mem_block_header_size());

  if (unlikely(heap == nullptr)) {
    /* This is the first block of the heap. The field
    total_size should be initialized here */
    block->total_size = len;
  } else {
    /* Not the first allocation for the heap. This block's
    total_length field should be set to undefined. */
    ut_d(block->total_size = ULINT_UNDEFINED);
    UNIV_MEM_INVALID(&block->total_size, sizeof block->total_size);

    heap->total_size += len;
  }

  ut_ad((ulint)mem_block_header_size() < len);

  return block;
}

mem_block_t *mem_heap_add_block(mem_heap_t *heap, ulint n) {
  mem_block_t *block;
  mem_block_t *new_block;
  ulint new_size;

  ut_ad(mem_heap_check(heap));

  block = UT_LIST_GET_LAST(heap->base);

  /* We have to allocate a new block. The size is always at least
  doubled until the standard size is reached. After that the size
  stays the same, except in cases where the caller needs more space. */

  new_size = 2 * mem_block_get_len(block);

  if (heap->type != MEM_HEAP_DYNAMIC) {
    /* From the buffer pool we allocate buffer frames */
    ut_a(n <= MEM_MAX_ALLOC_IN_BUF);

    if (new_size > MEM_MAX_ALLOC_IN_BUF) {
      new_size = MEM_MAX_ALLOC_IN_BUF;
    }
  } else if (new_size > mem_block_standard_size()) {

    new_size = mem_block_standard_size();
  }

  if (new_size < n) {
    new_size = n;
  }

  new_block = mem_heap_create_block(heap, new_size, heap->type, heap->file_name, heap->line);
  if (new_block == nullptr) {

    return (nullptr);
  }

  /* Add the new block as the last block */

  UT_LIST_INSERT_AFTER(heap->base, block, new_block);

  return (new_block);
}

void mem_heap_block_free(mem_heap_t *heap, mem_block_t *block) {
  auto buf_block = reinterpret_cast<buf_block_t *>(block->buf_block);

  if (block->magic_n != MEM_BLOCK_MAGIC_N) {
    ut_error;
  }

  UT_LIST_REMOVE(heap->base, block);

  ut_ad(heap->total_size >= block->len);
  heap->total_size -= block->len;

  auto type = heap->type;
  auto len = block->len;
  block->magic_n = MEM_FREED_BLOCK_MAGIC_N;

  if (!srv_use_sys_malloc) {
    UNIV_MEM_ASSERT_AND_FREE(block, len);
  }
  if (type == MEM_HEAP_DYNAMIC || len < UNIV_PAGE_SIZE / 2) {

    free(block);
  } else {
    buf_pool->block_free(buf_block);
  }
}

void mem_heap_free_block_free(mem_heap_t *heap) {
  if (likely_null(heap->free_block)) {
    buf_pool->block_free(static_cast<buf_block_t *>(heap->free_block));
    heap->free_block = nullptr;
  }
}

#ifdef UNIV_DEBUG
bool mem_heap_check(mem_heap_t *heap) {
  ut_a(heap->magic_n == MEM_BLOCK_MAGIC_N);

  return (true);
}

void mem_heap_validate_or_print(
  mem_heap_t *heap, byte *top __attribute__((unused)), bool print, bool *error, ulint *us_size, ulint *ph_size, ulint *n_blocks
) {
  mem_block_t *block;
  ulint total_len = 0;
  ulint block_count = 0;
  ulint phys_len = 0;

  /* Pessimistically, we set the parameters to error values */
  if (us_size != nullptr) {
    *us_size = 0;
  }
  if (ph_size != nullptr) {
    *ph_size = 0;
  }
  if (n_blocks != nullptr) {
    *n_blocks = 0;
  }
  *error = true;

  block = heap;

  if (block->magic_n != MEM_BLOCK_MAGIC_N) {
    return;
  }

  if (print) {
    ib_logger(ib_stream, "Memory heap:");
  }

  while (block != nullptr) {
    phys_len += mem_block_get_len(block);

    if ((block->type == MEM_HEAP_BUFFER) && (mem_block_get_len(block) > UNIV_PAGE_SIZE)) {

      ib_logger(
        ib_stream,
        "Error: mem block %p"
        " length %lu > UNIV_PAGE_SIZE\n",
        (void *)block,
        (ulong)mem_block_get_len(block)
      );
      /* error */

      return;
    }

    block = UT_LIST_GET_NEXT(list, block);
    block_count++;
  }
  if (us_size != nullptr) {
    *us_size = total_len;
  }
  if (ph_size != nullptr) {
    *ph_size = phys_len;
  }
  if (n_blocks != nullptr) {
    *n_blocks = block_count;
  }
  *error = false;
}

/** Prints the contents of a memory heap. */
static void mem_heap_print(mem_heap_t *heap) /*!< in: memory heap */
{
  bool error;
  ulint us_size;
  ulint phys_size;
  ulint n_blocks;

  ut_ad(mem_heap_check(heap));

  mem_heap_validate_or_print(heap, nullptr, true, &error, &us_size, &phys_size, &n_blocks);
  ib_logger(
    ib_stream,
    "\nheap type: %lu; size: user size %lu;"
    " physical size %lu; blocks %lu.\n",
    (ulong)heap->type,
    (ulong)us_size,
    (ulong)phys_size,
    (ulong)n_blocks
  );
  ut_a(!error);
}

bool mem_heap_validate(mem_heap_t *heap) {
  bool error;
  ulint us_size;
  ulint phys_size;
  ulint n_blocks;

  ut_ad(mem_heap_check(heap));

  mem_heap_validate_or_print(heap, nullptr, false, &error, &us_size, &phys_size, &n_blocks);
  if (error) {
    mem_heap_print(heap);
  }

  ut_a(!error);

  return (true);
}
#endif /* UNIV_DEBUG */

#ifdef UNIV_DEBUG
void mem_heap_verify(const mem_heap_t *heap) {
  auto block = UT_LIST_GET_FIRST(heap->base);

  while (block != nullptr) {
    ut_a(block->magic_n == MEM_BLOCK_MAGIC_N);
    block = UT_LIST_GET_NEXT(list, block);
  }
}
#endif
