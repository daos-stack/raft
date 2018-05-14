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

static int __ensurecapacity(log_private_t * me)
{
    int i, j;
    raft_entry_t *temp;

    if (me->count < me->size)
        return 0;

    temp = (raft_entry_t*)calloc(1, sizeof(raft_entry_t) * me->size * 2);
    if (!temp)
        return RAFT_ERR_NOMEM;

    for (i = 0, j = me->front; i < me->count; i++, j++)
    {
        if (j == me->size)
            j = 0;
        memcpy(&temp[i], &me->entries[j], sizeof(raft_entry_t));
    }

    /* clean up old entries */
    free(me->entries);

    me->size *= 2;
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

    int e = log_append_entry(me_, &ety);
    if (e != 0)
    {
        assert(0);
        return e;
    }

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

/** TODO: rename log_append */
int log_append_entry(log_t* me_, raft_entry_t* ety)
{
    log_private_t* me = (log_private_t*)me_;
    int idx = me->base + me->count + 1;
    int e;

    e = __ensurecapacity(me);
    if (e != 0)
        return e;

    memcpy(&me->entries[me->back], ety, sizeof(raft_entry_t));

    if (me->cb && me->cb->log_offer)
    {
        void* ud = raft_get_udata(me->raft);
        e = me->cb->log_offer(me->raft, ud, &me->entries[me->back], idx);
        if (0 != e)
            return e;
        raft_offer_log(me->raft, &me->entries[me->back], idx);
    }

    me->count++;
    me->back++;
    me->back = me->back % me->size;

    return 0;
}

raft_entry_t* log_get_from_idx(log_t* me_, int idx, int *n_etys)
{
    log_private_t* me = (log_private_t*)me_;
    int i;

    assert(0 <= idx - 1);

    if (me->base + me->count < idx || idx < me->base)
    {
        *n_etys = 0;
        return NULL;
    }

    /* idx starts at 1 */
    idx -= 1;

    i = (me->front + idx - me->base) % me->size;

    int logs_till_end_of_log;

    if (i < me->back)
        logs_till_end_of_log = me->back - i;
    else
        logs_till_end_of_log = me->size - i;

    *n_etys = logs_till_end_of_log;
    return &me->entries[i];
}

raft_entry_t* log_get_at_idx(log_t* me_, int idx)
{
    log_private_t* me = (log_private_t*)me_;
    int i;

    if (idx == 0)
        return NULL;

    if (idx <= me->base)
        return NULL;

    if (me->base + me->count < idx)
        return NULL;

    /* idx starts at 1 */
    idx -= 1;

    i = (me->front + idx - me->base) % me->size;
    return &me->entries[i];
}

int log_count(log_t* me_)
{
    return ((log_private_t*)me_)->count;
}

int log_delete(log_t* me_, int idx)
{
    log_private_t* me = (log_private_t*)me_;

    if (0 == idx)
        return -1;

    if (idx < me->base)
        idx = me->base;

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

int log_poll(log_t * me_, void** etyp)
{
    log_private_t* me = (log_private_t*)me_;
    int idx = me->base + 1;

    if (0 == me->count)
        return -1;

    const void *elem = &me->entries[me->front];
    if (me->cb && me->cb->log_poll)
    {
        int e = me->cb->log_poll(me->raft, raft_get_udata(me->raft),
                                 &me->entries[me->front], idx);
        if (0 != e)
            return e;
    }
    me->front++;
    me->front = me->front % me->size;
    me->count--;
    me->base++;

    *etyp = (void*)elem;
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
