/** 
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

#include "mem0mem.h"
#ifdef UNIV_NONINL
#include "mem0mem.ic"
#endif

#include "buf0buf.h"
#include "srv0srv.h"
#include <stdarg.h>

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

/** Duplicates a NUL-terminated string, allocated from a memory heap.
@return	own: a copy of the string */
UNIV_INTERN
char*
mem_heap_strdup(
	mem_heap_t*	heap,	/*!< in: memory heap where string is allocated */
	const char*	str)	/*!< in: string to be copied */
{
	return(mem_heap_dup(heap, str, strlen(str) + 1));
}

/** Duplicate a block of data, allocated from a memory heap.
@return	own: a copy of the data */
UNIV_INTERN
void*
mem_heap_dup(
	mem_heap_t*	heap,	/*!< in: memory heap where copy is allocated */
	const void*	data,	/*!< in: data to be copied */
	ulint		len)	/*!< in: length of data, in bytes */
{
	return(memcpy(mem_heap_alloc(heap, len), data, len));
}

/** Concatenate two strings and return the result, using a memory heap.
@return	own: the result */
UNIV_INTERN
char*
mem_heap_strcat(
	mem_heap_t*	heap,	/*!< in: memory heap where string is allocated */
	const char*	s1,	/*!< in: string 1 */
	const char*	s2)	/*!< in: string 2 */
{
	char*	s;
	ulint	s1_len = strlen(s1);
	ulint	s2_len = strlen(s2);

	s = mem_heap_alloc(heap, s1_len + s2_len + 1);

	memcpy(s, s1, s1_len);
	memcpy(s + s1_len, s2, s2_len);

	s[s1_len + s2_len] = '\0';

	return(s);
}


/** Helper function for mem_heap_printf.
@return	length of formatted string, including terminating NUL */
static
ulint
mem_heap_printf_low(
	char*		buf,	/*!< in/out: buffer to store formatted string
				in, or NULL to just calculate length */
	const char*	format,	/*!< in: format string */
	va_list		ap)	/*!< in: arguments */
{
	ulint 		len = 0;

	while (*format) {

		/* Does this format specifier have the 'l' length modifier. */
		ibool	is_long = FALSE;

		/* Length of one parameter. */
		size_t	plen;

		if (*format++ != '%') {
			/* Non-format character. */

			len++;

			if (buf) {
				*buf++ = *(format - 1);
			}

			continue;
		}

		if (*format == 'l') {
			is_long = TRUE;
			format++;
		}

		switch (*format++) {
		case 's':
			/* string */
			{
				char*	s = va_arg(ap, char*);

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
				char		tmp[32];
				unsigned long	val;

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

	return(len);
}

/** A simple (s)printf replacement that dynamically allocates the space for the
formatted string from the given heap. This supports a very limited set of
the printf syntax: types 's' and 'u' and length modifier 'l' (which is
required for the 'u' type).
@return	heap-allocated formatted string */
UNIV_INTERN
char*
mem_heap_printf(
	mem_heap_t*	heap,	/*!< in: memory heap */
	const char*	format,	/*!< in: format string */
	...)
{
	va_list		ap;
	char*		str;
	ulint 		len;

	/* Calculate length of string */
	len = 0;
	va_start(ap, format);
	len = mem_heap_printf_low(NULL, format, ap);
	va_end(ap);

	/* Now create it for real. */
	str = mem_heap_alloc(heap, len);
	va_start(ap, format);
	mem_heap_printf_low(str, format, ap);
	va_end(ap);

	return(str);
}

/** Creates a memory heap block where data can be allocated.
@return own: memory heap block, NULL if did not succeed (only possible
for MEM_HEAP_BTR_SEARCH type heaps) */
UNIV_INTERN
mem_block_t*
mem_heap_create_block(
	mem_heap_t*	heap,	/*!< in: memory heap or NULL if first block
				should be created */
	ulint		n,	/*!< in: number of bytes needed for user data */
	ulint		type,	/*!< in: type of heap: MEM_HEAP_DYNAMIC or
				MEM_HEAP_BUFFER */
	const char*	file_name,/*!< in: file name where created */
	ulint		line)	/*!< in: line where created */
{
#ifndef UNIV_HOTBACKUP
	buf_block_t*	buf_block = NULL;
#endif /* !UNIV_HOTBACKUP */
	mem_block_t*	block;
	ulint		len;

	ut_ad((type == MEM_HEAP_DYNAMIC) || (type == MEM_HEAP_BUFFER)
	      || (type == MEM_HEAP_BUFFER + MEM_HEAP_BTR_SEARCH));

	if (heap && heap->magic_n != MEM_BLOCK_MAGIC_N) {
		ut_error;
	}

	/* In dynamic allocation, calculate the size: block header + data. */

	len = MEM_BLOCK_HEADER_SIZE + MEM_SPACE_NEEDED(n);

#ifndef UNIV_HOTBACKUP
	if (type == MEM_HEAP_DYNAMIC || len < UNIV_PAGE_SIZE / 2) {

		ut_a(type == MEM_HEAP_DYNAMIC || n <= MEM_MAX_ALLOC_IN_BUF);

		block = (mem_block_t*) malloc(len);
	} else {

		len = UNIV_PAGE_SIZE;

		if ((type & MEM_HEAP_BTR_SEARCH) && heap) {
			/* We cannot allocate the block from the
			buffer pool, but must get the free block from
			the heap header free block field */

			buf_block = heap->free_block;
			heap->free_block = NULL;

			if (UNIV_UNLIKELY(buf_block == NULL)) {

				return(NULL);
			}
		} else {
			buf_block = buf_block_alloc(0);
		}

		block = (mem_block_t*) buf_block->frame;
	}

	ut_ad(block);
	block->buf_block = buf_block;
	block->free_block = NULL;
#else /* !UNIV_HOTBACKUP */
	len = MEM_BLOCK_HEADER_SIZE + MEM_SPACE_NEEDED(n);
	block = ut_malloc(len);
	ut_ad(block);
#endif /* !UNIV_HOTBACKUP */

	block->magic_n = MEM_BLOCK_MAGIC_N;
	ut_strlcpy_rev(block->file_name, file_name, sizeof(block->file_name));
	block->line = line;

	mem_block_set_len(block, len);
	mem_block_set_type(block, type);
	mem_block_set_free(block, MEM_BLOCK_HEADER_SIZE);
	mem_block_set_start(block, MEM_BLOCK_HEADER_SIZE);

	if (UNIV_UNLIKELY(heap == NULL)) {
		/* This is the first block of the heap. The field
		total_size should be initialized here */
		block->total_size = len;
	} else {
		/* Not the first allocation for the heap. This block's
		total_length field should be set to undefined. */
		ut_d(block->total_size = ULINT_UNDEFINED);
		UNIV_MEM_INVALID(&block->total_size,
				 sizeof block->total_size);

		heap->total_size += len;
	}

	ut_ad((ulint)MEM_BLOCK_HEADER_SIZE < len);

	return(block);
}

/** Adds a new block to a memory heap.
@return created block, NULL if did not succeed (only possible for
MEM_HEAP_BTR_SEARCH type heaps) */
UNIV_INTERN
mem_block_t*
mem_heap_add_block(
	mem_heap_t*	heap,	/*!< in: memory heap */
	ulint		n)	/*!< in: number of bytes user needs */
{
	mem_block_t*	block;
	mem_block_t*	new_block;
	ulint		new_size;

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
	} else if (new_size > MEM_BLOCK_STANDARD_SIZE) {

		new_size = MEM_BLOCK_STANDARD_SIZE;
	}

	if (new_size < n) {
		new_size = n;
	}

	new_block = mem_heap_create_block(heap, new_size, heap->type,
					  heap->file_name, heap->line);
	if (new_block == NULL) {

		return(NULL);
	}

	/* Add the new block as the last block */

	UT_LIST_INSERT_AFTER(list, heap->base, block, new_block);

	return(new_block);
}

/** Frees a block from a memory heap. */
UNIV_INTERN
void
mem_heap_block_free(
	mem_heap_t*	heap,	/*!< in: heap */
	mem_block_t*	block)	/*!< in: block to free */
{
	ulint		type;
	ulint		len;
#ifndef UNIV_HOTBACKUP
	buf_block_t*	buf_block	= block->buf_block;
#endif /* !UNIV_HOTBACKUP */

	if (block->magic_n != MEM_BLOCK_MAGIC_N) {
		ut_error;
	}

	UT_LIST_REMOVE(list, heap->base, block);


	ut_ad(heap->total_size >= block->len);
	heap->total_size -= block->len;

	type = heap->type;
	len = block->len;
	block->magic_n = MEM_FREED_BLOCK_MAGIC_N;

#ifndef UNIV_HOTBACKUP
	if (!srv_use_sys_malloc) {
#ifdef UNIV_MEM_DEBUG
		/* In the debug version we set the memory to a random
		combination of hex 0xDE and 0xAD. */

		mem_erase_buf((byte*)block, len);
#else /* UNIV_MEM_DEBUG */
		UNIV_MEM_ASSERT_AND_FREE(block, len);
#endif /* UNIV_MEM_DEBUG */

	}
	if (type == MEM_HEAP_DYNAMIC || len < UNIV_PAGE_SIZE / 2) {

		free(block);
	} else {
		buf_block_free(buf_block);
	}
#else /* !UNIV_HOTBACKUP */
#ifdef UNIV_MEM_DEBUG
	/* In the debug version we set the memory to a random
	combination of hex 0xDE and 0xAD. */

	mem_erase_buf((byte*)block, len);
#else /* UNIV_MEM_DEBUG */
	UNIV_MEM_ASSERT_AND_FREE(block, len);
#endif /* UNIV_MEM_DEBUG */
	ut_free(block);
#endif /* !UNIV_HOTBACKUP */
}

#ifndef UNIV_HOTBACKUP
/** Frees the free_block field from a memory heap. */
UNIV_INTERN
void
mem_heap_free_block_free(
	mem_heap_t*	heap)	/*!< in: heap */
{
	if (UNIV_LIKELY_NULL(heap->free_block)) {

		buf_block_free(heap->free_block);

		heap->free_block = NULL;
	}
}
#endif /* !UNIV_HOTBACKUP */

#ifdef UNIV_DEBUG
/** Goes through the list of all allocated mem blocks, checks their magic
numbers, and reports possible corruption. */
UNIV_INTERN
ibool
mem_heap_check(
	mem_heap_t*	heap)	/*!< in: memory heap */
{
	ut_a(heap->magic_n == MEM_BLOCK_MAGIC_N);

	return(TRUE);
}
#endif /* UNIV_DEBUG */

#if defined UNIV_MEM_DEBUG || defined UNIV_DEBUG
/** Checks a memory heap for consistency and prints the contents if requested.
Outputs the sum of sizes of buffers given to the user (only in
the debug version), the physical size of the heap and the number of
blocks in the heap. In case of error returns 0 as sizes and number
of blocks. */

void
mem_heap_validate_or_print(
	mem_heap_t*	heap,	/*!< in: memory heap */
	byte*		top __attribute__((unused)),
				/*!< in: calculate and validate only until
				this top pointer in the heap is reached,
				if this pointer is NULL, ignored */
	ibool		print,	/*!< in: if TRUE, prints the contents
				of the heap; works only in
				the debug version */
	ibool*		error,	/*!< out: TRUE if error */
	ulint*		us_size,/*!< out: allocated memory
				(for the user) in the heap,
				if a NULL pointer is passed as this
				argument, it is ignored; in the
				non-debug version this is always -1 */
	ulint*		ph_size,/*!< out: physical size of the heap,
				if a NULL pointer is passed as this
				argument, it is ignored */
	ulint*		n_blocks) /*!< out: number of blocks in the heap,
				if a NULL pointer is passed as this
				argument, it is ignored */
{
	mem_block_t*	block;
	ulint		total_len	= 0;
	ulint		block_count	= 0;
	ulint		phys_len	= 0;
#ifdef UNIV_MEM_DEBUG
	ulint		len;
	byte*		field;
	byte*		user_field;
	ulint		check_field;
#endif

	/* Pessimistically, we set the parameters to error values */
	if (us_size != NULL) {
		*us_size = 0;
	}
	if (ph_size != NULL) {
		*ph_size = 0;
	}
	if (n_blocks != NULL) {
		*n_blocks = 0;
	}
	*error = TRUE;

	block = heap;

	if (block->magic_n != MEM_BLOCK_MAGIC_N) {
		return;
	}

	if (print) {
		ib_logger(ib_stream, "Memory heap:");
	}

	while (block != NULL) {
		phys_len += mem_block_get_len(block);

		if ((block->type == MEM_HEAP_BUFFER)
		    && (mem_block_get_len(block) > UNIV_PAGE_SIZE)) {

			ib_logger(ib_stream,
				"InnoDB: Error: mem block %p"
				" length %lu > UNIV_PAGE_SIZE\n",
				(void*) block,
				(ulong) mem_block_get_len(block));
			/* error */

			return;
		}

#ifdef UNIV_MEM_DEBUG
		/* We can trace the fields of the block only in the debug
		version */
		if (print) {
			ib_logger(ib_stream, " Block %ld:", block_count);
		}

		field = (byte*)block + mem_block_get_start(block);

		if (top && (field == top)) {

			goto completed;
		}

		while (field < (byte*)block + mem_block_get_free(block)) {

			/* Calculate the pointer to the storage
			which was given to the user */

			user_field = field + MEM_FIELD_HEADER_SIZE;

			len = mem_field_header_get_len(user_field);

			if (print) {
				ut_print_buf(ib_stream, user_field, len);
			}

			total_len += len;
			check_field = mem_field_header_get_check(user_field);

			if (check_field
			    != mem_field_trailer_get_check(user_field)) {
				/* error */

				ib_logger(ib_stream,
					"InnoDB: Error: block %lx mem"
					" field %lx len %lu\n"
					"InnoDB: header check field is"
					" %lx but trailer %lx\n",
					(ulint)block,
					(ulint)field, len, check_field,
					mem_field_trailer_get_check(
						user_field));

				return;
			}

			/* Move to next field */
			field = field + MEM_SPACE_NEEDED(len);

			if (top && (field == top)) {

				goto completed;
			}

		}

		/* At the end check that we have arrived to the first free
		position */

		if (field != (byte*)block + mem_block_get_free(block)) {
			/* error */

			ib_logger(ib_stream,
				"InnoDB: Error: block %lx end of"
				" mem fields %lx\n"
				"InnoDB: but block free at %lx\n",
				(ulint)block, (ulint)field,
				(ulint)((byte*)block
					+ mem_block_get_free(block)));

			return;
		}

#endif

		block = UT_LIST_GET_NEXT(list, block);
		block_count++;
	}
#ifdef UNIV_MEM_DEBUG
completed:
#endif
	if (us_size != NULL) {
		*us_size = total_len;
	}
	if (ph_size != NULL) {
		*ph_size = phys_len;
	}
	if (n_blocks != NULL) {
		*n_blocks = block_count;
	}
	*error = FALSE;
}

/** Prints the contents of a memory heap. */
static
void
mem_heap_print(
	mem_heap_t*	heap)	/*!< in: memory heap */
{
	ibool	error;
	ulint	us_size;
	ulint	phys_size;
	ulint	n_blocks;

	ut_ad(mem_heap_check(heap));

	mem_heap_validate_or_print(heap, NULL, TRUE, &error,
				   &us_size, &phys_size, &n_blocks);
	ib_logger(ib_stream,
		"\nheap type: %lu; size: user size %lu;"
		" physical size %lu; blocks %lu.\n",
		(ulong) heap->type, (ulong) us_size,
		(ulong) phys_size, (ulong) n_blocks);
	ut_a(!error);
}

/** Validates the contents of a memory heap.
@return	TRUE if ok */

ibool
mem_heap_validate(
	mem_heap_t*	heap)	/*!< in: memory heap */
{
	ibool	error;
	ulint	us_size;
	ulint	phys_size;
	ulint	n_blocks;

	ut_ad(mem_heap_check(heap));

	mem_heap_validate_or_print(heap, NULL, FALSE, &error, &us_size,
				   &phys_size, &n_blocks);
	if (error) {
		mem_heap_print(heap);
	}

	ut_a(!error);

	return(TRUE);
}
#endif /* UNIV_MEM_DEBUG || UNIV_DEBUG */

#ifdef UNIV_DEBUG
/** Verify that the heap is not corrupt. */
UNIV_INTERN
void
mem_heap_verify(
	const mem_heap_t* heap)	/*!< in: heap to verify */
{
	mem_block_t*	block;

	block = UT_LIST_GET_FIRST(heap->base);

	while (block != NULL) {
		ut_a(block->magic_n == MEM_BLOCK_MAGIC_N);
		block = UT_LIST_GET_NEXT(list, block);
	}
}
#endif
