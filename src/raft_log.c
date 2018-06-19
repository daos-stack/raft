/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief ADT for managing Raft log entries (aka entries)
 * @author Willem Thiart himself@willemthiart.com
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include "raft.h"
#include "raft_private.h"
#include "raft_log.h"

#define INITIAL_CAPACITY 10

typedef struct
{
    /* size of array */
    int size;

    /* the amount of elements in the array */
    int count;

    /* position of the queue */
    int front, back;

    /* we compact the log, and thus need to increment the Base Log Index */
    int base;

    raft_entry_t* entries;

    /* callbacks */
    raft_cbs_t *cb;
    void* raft;
} log_private_t;

static int mod(int a, int b)
{
    int r = a % b;
    return r < 0 ? r + b : r;
}

static int __ensurecapacity(log_private_t * me, int n)
{
    int newsize;
    raft_entry_t *temp;

    if ((me->count + n) <= me->size)
        return 0;

    for (newsize = me->size; newsize < (me->count + n); newsize *= 2)
        ;
    temp = (raft_entry_t*) calloc(1, sizeof(raft_entry_t) * newsize);
    if (!temp)
        return RAFT_ERR_NOMEM;


    if (me->front < me->back)
    {
        memcpy(&temp[0], &me->entries[me->front],
               sizeof(raft_entry_t) * (me->back - me->front));
    }
    else if (me->count > 0)
    {
        int i = me->size - me->front;
        memcpy(&temp[0], &me->entries[me->front], i * sizeof(raft_entry_t));
        memcpy(&temp[i], &me->entries[0], me->back * sizeof(raft_entry_t));
    }

    /* clean up old entries */
    free(me->entries);

    me->size = newsize;
    me->entries = temp;
    me->front = 0;
    me->back = me->count;
    return 0;
}

int log_load_from_snapshot(log_t *me_, int idx, int term)
{
    log_private_t* me = (log_private_t*)me_;

    log_clear(me_);

    raft_entry_t ety;
    ety.data.len = 0;
    ety.id = 1;
    ety.term = term;
    ety.type = RAFT_LOGTYPE_SNAPSHOT;

    int k = 1;
    int e = log_append(me_, &ety, &k);
    if (e != 0)
    {
        assert(0);
        return e;
    }
    assert(k == 1);
    me->base = idx - 1;

    return 0;
}

log_t* log_alloc(int initial_size)
{
    log_private_t* me = (log_private_t*)calloc(1, sizeof(log_private_t));
    if (!me)
        return NULL;
    me->size = initial_size;
    log_clear((log_t*)me);
    me->entries = (raft_entry_t*)calloc(1, sizeof(raft_entry_t) * me->size);
    if (!me->entries) {
        free(me);
        return NULL;
    }
    return (log_t*)me;
}

log_t* log_new()
{
    return log_alloc(INITIAL_CAPACITY);
}

void log_set_callbacks(log_t* me_, raft_cbs_t* funcs, void* raft)
{
    log_private_t* me = (log_private_t*)me_;

    me->raft = raft;
    me->cb = funcs;
}

void log_clear(log_t* me_)
{
    log_private_t* me = (log_private_t*)me_;
    me->count = 0;
    me->back = 0;
    me->front = 0;
    me->base = 0;
}

static int has_idx(log_private_t* me, int idx)
{
    return me->base < idx && idx <= me->base + me->count;
}

/* Return the me->entries[] subscript for idx. */
static int subscript(log_private_t* me, int idx)
{
    return (me->front + (idx - (me->base + 1))) % me->size;
}

/* Return the maximal number of entries contiguous in me->entries[] from idx up
 * to at most idx + n - 1. */
static int batch_up(log_private_t* me, int idx, int n)
{
    assert(n > 0);
    int low = subscript(me, idx);
    int high = subscript(me, idx + n - 1);
    if (low <= high)
        return high - low + 1;
    else
        return me->size - low;
}

raft_entry_t* log_get_from_idx(log_t* me_, int idx, int *n_etys)
{
    log_private_t* me = (log_private_t*)me_;

    if (!has_idx(me, idx))
    {
        *n_etys = 0;
        return NULL;
    }

    *n_etys = batch_up(me, idx, (me->base + me->count) - idx + 1);
    return &me->entries[subscript(me, idx)];
}

raft_entry_t* log_get_at_idx(log_t* me_, int idx)
{
    int n;
    return log_get_from_idx(me_, idx, &n);
}

int log_count(log_t* me_)
{
    return ((log_private_t*)me_)->count;
}

int log_append(log_t* me_, raft_entry_t* ety, int *n)
{
    log_private_t* me = (log_private_t*)me_;
    int i, e = 0;

    e = __ensurecapacity(me, *n);
    if (e != 0)
        return e;

    for (i = 0; i < *n; )
    {
        raft_entry_t *ptr = &me->entries[me->back];
        int idx = me->base + me->count + 1;
        int k = batch_up(me, idx, *n - i);
        int batch_size = k;
        memcpy(ptr, &ety[i], k * sizeof(raft_entry_t));
        if (me->cb && me->cb->log_offer)
            e = me->cb->log_offer(me->raft, raft_get_udata(me->raft),
                                  ptr, idx, &k);
        if (k > 0)
        {
            me->count += k;
            me->back += k;
            me->back = me->back % me->size;
            i += k;
            raft_offer_log(me->raft, ptr, k, idx);
        }
        if (0 != e) {
            *n = i;
            return e;
        }
        assert(batch_size == k);
    }
    return 0;
}

int log_delete(log_t* me_, int idx)
{
    log_private_t* me = (log_private_t*)me_;

    if (!has_idx(me, idx))
        return -1;

    for (; idx <= me->base + me->count && me->count;)
    {
        int idx_tmp = me->base + me->count;
        int back = mod(me->back - 1, me->size);

        if (me->cb && me->cb->log_pop)
        {
            int e = me->cb->log_pop(me->raft, raft_get_udata(me->raft),
                                    &me->entries[back], idx_tmp);
            if (0 != e)
                return e;
        }
        raft_pop_log(me->raft, &me->entries[back], idx_tmp);
        me->back = back;
        me->count--;
    }
    return 0;
}

int log_poll(log_t* me_, int idx)
{
    log_private_t* me = (log_private_t*)me_;
    int e = 0;

    if (0 == me->count)
        return -1;

    if (!has_idx(me, idx))
        return -1;

    while (me->base + 1 <= idx)
    {
        int n = batch_up(me, me->base + 1, idx - (me->base + 1) + 1);
        int batch_size = n;
        if (me->cb && me->cb->log_poll)
            e = me->cb->log_poll(me->raft, raft_get_udata(me->raft),
                                 &me->entries[me->front], me->base + 1, &n);
        me->front += n;
        me->front = me->front % me->size;
        me->count -= n;
        me->base += n;
        if (0 != e)
            return e;
        assert(n == batch_size);
    }

    return 0;
}

raft_entry_t *log_peektail(log_t * me_)
{
    log_private_t* me = (log_private_t*)me_;

    if (0 == me->count)
        return NULL;

    if (0 == me->back)
        return &me->entries[me->size - 1];
    else
        return &me->entries[me->back - 1];
}

void log_empty(log_t * me_)
{
    log_private_t* me = (log_private_t*)me_;

    me->front = 0;
    me->back = 0;
    me->count = 0;
}

void log_free(log_t * me_)
{
    log_private_t* me = (log_private_t*)me_;

    free(me->entries);
    free(me);
}

int log_get_current_idx(log_t* me_)
{
    log_private_t* me = (log_private_t*)me_;
    return log_count(me_) + me->base;
}

int log_get_base(log_t* me_)
{
    return ((log_private_t*)me_)->base;
}
