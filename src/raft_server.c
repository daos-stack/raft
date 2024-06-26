/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief Implementation of a Raft server
 * @author Willem Thiart himself@willemthiart.com
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

/* for varags */
#include <stdarg.h>

#include "raft.h"
#include "raft_log.h"
#include "raft_private.h"

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

#ifndef max
#define max(a, b) ((a) < (b) ? (b) : (a))
#endif

static void __log(raft_server_t *me_, raft_node_t* node, raft_loglevel_e level, char *fmt, ...)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    if (me->cb.log == NULL) return;
    char buf[1024];
    va_list args;

    va_start(args, fmt);
    vsprintf(buf, fmt, args);

    me->cb.log(me_, node, me->udata, level, buf);

    va_end(args);
}

raft_loglevel_e raft_loglevel = RAFT_LOG_INFO;

#define _log(server, node, level, fmt, ...) do \
{ \
    if (level <= raft_loglevel) \
        __log(server, node, level, fmt, ##__VA_ARGS__); \
} while (0)

#define log_error(server, node, fmt, ...) _log(server, node, RAFT_LOG_ERROR, fmt, ##__VA_ARGS__)
#define log_info(server, node, fmt, ...) _log(server, node, RAFT_LOG_INFO, fmt, ##__VA_ARGS__)
#define log_debug(server, node, fmt, ...) _log(server, node, RAFT_LOG_DEBUG, fmt, ##__VA_ARGS__)

void raft_set_log_level(raft_loglevel_e level)
{
    raft_loglevel = level;
}

static double get_rand(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    if (me->cb.get_rand == NULL) {
        int r = rand();
        if (r == RAND_MAX)
            r = 0;
        return (double)r / RAND_MAX;
    }
    return me->cb.get_rand(me_, me->udata);
}

void raft_randomize_election_timeout(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    /* [election_timeout, 2 * election_timeout) */
    me->election_timeout_rand = me->election_timeout * (1 + get_rand(me_));
    log_info(me_, NULL, "randomize election timeout to %d", me->election_timeout_rand);
}

raft_server_t* raft_new()
{
    raft_server_private_t* me =
        (raft_server_private_t*)calloc(1, sizeof(raft_server_private_t));
    if (!me)
        return NULL;
    me->current_term = 0;
    me->voted_for = -1;
    me->election_timer = 0;
    me->request_timeout = 200;
    me->election_timeout = 1000;
    raft_randomize_election_timeout((raft_server_t*)me);
    me->log = log_new();
    if (!me->log) {
        free(me);
        return NULL;
    }
    me->voting_cfg_change_log_idx = -1;
    raft_set_state((raft_server_t*)me, RAFT_STATE_FOLLOWER);
    me->leader_id = -1;
    me->node_id = -1;

    me->snapshot_in_progress = 0;
    raft_set_snapshot_metadata((raft_server_t*)me, 0, 0);

    me->start_time = 0;
    me->lease_maintenance_grace = 0;
    me->first_start = 0;

    return (raft_server_t*)me;
}

void raft_set_callbacks(raft_server_t* me_, raft_cbs_t* funcs, void* udata)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    memcpy(&me->cb, funcs, sizeof(raft_cbs_t));
    me->udata = udata;
    log_set_callbacks(me->log, &me->cb, me_);

    /* We couldn't initialize the time fields without the callback. */
    raft_time_t now = me->cb.get_time(me_, me->udata);
    me->election_timer = now;
    me->start_time = now;
}

void raft_free(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    log_free(me->log);
    if (me->nodes)
        free(me->nodes);
    free(me_);
}

void raft_clear(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    me->current_term = 0;
    me->voted_for = -1;
    me->election_timer = 0;
    raft_randomize_election_timeout(me_);
    me->voting_cfg_change_log_idx = -1;
    raft_set_state((raft_server_t*)me, RAFT_STATE_FOLLOWER);
    me->leader_id = -1;
    me->commit_idx = 0;
    me->last_applied_idx = 0;
    me->num_nodes = 0;
    me->node_id = -1;
    log_clear(me->log);
    me->start_time = 0;
    me->lease_maintenance_grace = 0;
    me->first_start = 0;
}

int raft_delete_entry_from_idx(raft_server_t* me_, raft_index_t idx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    assert(raft_get_commit_idx(me_) < idx);

    if (idx <= me->voting_cfg_change_log_idx)
        me->voting_cfg_change_log_idx = -1;

    return log_delete(me->log, idx);
}

int raft_election_start(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    log_info(me_, NULL, "election starting: %d %ld, term: %ld ci: %ld",
             me->election_timeout_rand, me->election_timer, me->current_term,
             raft_get_current_idx(me_));

    return raft_become_candidate(me_);
}

void raft_become_leader(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    log_info(me_, NULL, "becoming leader term:%ld", raft_get_current_term(me_));

    raft_set_state(me_, RAFT_STATE_LEADER);
    raft_time_t now = me->cb.get_time(me_, me->udata);
    me->election_timer = now;
    for (i = 0; i < me->num_nodes; i++)
    {
        raft_node_t* node = me->nodes[i];

        if (raft_is_self(me_, node))
            continue;

        raft_node_set_next_idx(node, raft_get_current_idx(me_) + 1);
        raft_node_set_match_idx(node, 0);
        raft_node_set_effective_time(node, now);
        raft_send_appendentries(me_, node);
    }
}

/* Might we have granted a lease that hasn't expired to someone other than
 * except_id? */
static int raft_lease_granted(raft_server_t* me_, raft_node_id_t except_id, raft_time_t now)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    /* If we have just started, unless for the first time in our life, we might
     * have granted a lease to someone before that hasn't expired yet. */
    if (!me->first_start && now - me->start_time < me->election_timeout)
        return 1;

    if (me->leader_id != -1 && me->leader_id != except_id &&
        now - me->election_timer < me->election_timeout)
        return 1;

    return 0;
}

int raft_count_votes(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    int votes = raft_get_nvotes_for_me(me_);
    if (raft_votes_is_majority(raft_get_num_voting_nodes(me_), votes))
    {
        if (me->prevote)
        {
            int e = raft_become_prevoted_candidate(me_);
            if (0 != e)
                return e;
        }
        else
            raft_become_leader(me_);
    }
    return 0;
}

int raft_become_candidate(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    raft_time_t now = me->cb.get_time(me_, me->udata);
    int i;

    if (raft_lease_granted(me_, me->node_id, now))
        return RAFT_ERR_MIGHT_VIOLATE_LEASE;

    log_info(me_, NULL, "becoming candidate");

    raft_set_state(me_, RAFT_STATE_CANDIDATE);
    me->prevote = 1;

    for (i = 0; i < me->num_nodes; i++)
        raft_node_vote_for_me(me->nodes[i], 0);
    raft_node_vote_for_me(raft_get_my_node(me_), 1);

    me->leader_id = -1;
    raft_randomize_election_timeout(me_);
    me->election_timer = now;

    for (i = 0; i < me->num_nodes; i++)
    {
        raft_node_t* node = me->nodes[i];

        if (!raft_is_self(me_, node) &&
            raft_node_is_voting(node))
        {
            raft_send_requestvote(me_, node);
        }
    }

    /* We've already got at least one prevote from ourself, which is enough if
     * we are the only voting node. */
    return raft_count_votes(me_);
}

int raft_become_prevoted_candidate(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    log_info(me_, NULL, "becoming prevoted candidate");

    int e = raft_set_current_term(me_, raft_get_current_term(me_) + 1);
    if (0 != e)
        return e;
    for (i = 0; i < me->num_nodes; i++)
        raft_node_vote_for_me(me->nodes[i], 0);
    e = raft_vote_for_nodeid(me_, me->node_id);
    if (0 != e)
        return e;
    raft_node_vote_for_me(raft_get_my_node(me_), 1);
    me->prevote = 0;

    for (i = 0; i < me->num_nodes; i++)
    {
        raft_node_t* node = me->nodes[i];

        if (!raft_is_self(me_, node) &&
            raft_node_is_voting(node))
        {
            raft_send_requestvote(me_, node);
        }
    }

    /* We've already got at least one vote from ourself, which is enough if we
     * are the only voting node. */
    return raft_count_votes(me_);
}

void raft_become_follower(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    log_info(me_, NULL, "becoming follower");
    raft_set_state(me_, RAFT_STATE_FOLLOWER);
    raft_randomize_election_timeout(me_);
    me->election_timer = me->cb.get_time(me_, me->udata);
}

static int has_lease(raft_server_t* me_, raft_node_t* node, raft_time_t now, int with_grace)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (raft_is_self(me_, node))
        return 1;

    if (with_grace)
    {
        if (now < raft_node_get_lease(node) + me->lease_maintenance_grace)
            return 1;
        /* Since a leader has no lease from any other node at the beginning of
         * its term, or from any new node the leader adds thereafter, we give
         * it some time to acquire the initial lease. */
        if (now - raft_node_get_effective_time(node) < me->election_timeout + me->lease_maintenance_grace)
            return 1;
    }
    else
    {
        if (now < raft_node_get_lease(node))
            return 1;
    }

    return 0;
}

static int has_majority_leases(raft_server_t* me_, raft_time_t now, int with_grace)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    assert(me->state == RAFT_STATE_LEADER);

    int n = 0;
    int n_voting = 0;

    int i;
    for (i = 0; i < me->num_nodes; i++)
    {
        if (raft_node_is_voting(me->nodes[i]))
        {
            n_voting++;
            if (has_lease(me_, me->nodes[i], now, with_grace))
                n++;
        }
    }

    return n_voting / 2 + 1 <= n;
}

int raft_has_majority_leases(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (me->state != RAFT_STATE_LEADER)
        return 0;

    /* Check without grace, because the caller may be checking leadership for
     * linearizability (§6.4). */
    return has_majority_leases(me_, me->cb.get_time(me_, me->udata), 0 /* with_grace */);
}

int raft_periodic(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    raft_node_t *my_node = raft_get_my_node(me_);
    raft_time_t now = me->cb.get_time(me_, me->udata);

    if (me->state == RAFT_STATE_LEADER)
    {
        if (!has_majority_leases(me_, now, 1 /* with_grace */))
        {
            /* A leader who can't maintain majority leases shall step down. */
            log_error(me_, NULL, "unable to maintain majority leases");
            raft_become_follower(me_);
            me->leader_id = -1;
        }
        else if (me->request_timeout <= now - me->election_timer)
        {
            raft_send_appendentries_all(me_);
        }
    }
    else if (me->election_timeout_rand <= now - me->election_timer &&
        /* Don't become the leader when building snapshots or bad things will
         * happen when we get a client request */
        !raft_snapshot_is_in_progress(me_))
    {
        if (my_node && raft_node_is_voting(my_node))
        {
            int e = raft_election_start(me_);
            if (0 != e)
                return e;
        }
    }

    if (me->last_applied_idx < raft_get_commit_idx(me_) &&
        !raft_snapshot_is_in_progress(me_))
    {
        int e = raft_apply_all(me_);
        if (0 != e)
            return e;
    }

    return 0;
}

raft_entry_t* raft_get_entry_from_idx(raft_server_t* me_, raft_index_t etyidx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    return log_get_at_idx(me->log, etyidx);
}

/* Returns nonzero if we've got the term at idx or zero otherwise. */
int raft_get_entry_term(raft_server_t* me_, raft_index_t idx, raft_term_t* term)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int got = 1;
    raft_entry_t* ety = raft_get_entry_from_idx(me_, idx);
    if (ety)
        *term = ety->term;
    else if (idx == log_get_base(me->log))
        *term = log_get_base_term(me->log);
    else
        got = 0;
    return got;
}

int raft_voting_change_is_in_progress(raft_server_t* me_)
{
    return ((raft_server_private_t*)me_)->voting_cfg_change_log_idx != -1;
}

int raft_recv_appendentries_response(raft_server_t* me_,
                                     raft_node_t* node,
                                     msg_appendentries_response_t* r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    log_debug(me_, node,
              "received appendentries response %s ci:%ld rci:%ld 1stidx:%ld ls=%ld",
              r->success == 1 ? "SUCCESS" : "fail",
              raft_get_current_idx(me_),
              r->current_idx,
              r->first_idx,
              r->lease);

    if (!node)
        return -1;

    if (!raft_is_leader(me_))
        return RAFT_ERR_NOT_LEADER;

    /* If response contains term T > currentTerm: set currentTerm = T
       and convert to follower (§5.3) */
    if (me->current_term < r->term)
    {
        int e = raft_set_current_term(me_, r->term);
        if (0 != e)
            return e;
        raft_become_follower(me_);
        me->leader_id = -1;
        return 0;
    }
    else if (me->current_term != r->term)
        return 0;

    raft_node_set_lease(node, r->lease);

    raft_index_t match_idx = raft_node_get_match_idx(node);

    if (0 == r->success)
    {
        /* If AppendEntries fails because of log inconsistency:
           decrement nextIndex and retry (§5.3) */
        raft_index_t next_idx = raft_node_get_next_idx(node);
        assert(0 < next_idx);
        /* Stale response -- ignore */
        assert(match_idx <= next_idx - 1);
        if (match_idx == next_idx - 1)
            return 0;
        if (r->current_idx < next_idx - 1)
            raft_node_set_next_idx(node, min(r->current_idx + 1, raft_get_current_idx(me_)));
        else
            raft_node_set_next_idx(node, next_idx - 1);

        /* retry */
        raft_send_appendentries(me_, node);
        return 0;
    }


    if (!raft_node_is_voting(node) &&
        !raft_voting_change_is_in_progress(me_) &&
        raft_get_current_idx(me_) <= r->current_idx + 1 &&
        me->cb.node_has_sufficient_logs &&
        0 == raft_node_has_sufficient_logs(node)
        )
    {
        int e = me->cb.node_has_sufficient_logs(me_, me->udata, node);
        if (0 == e)
            raft_node_set_has_sufficient_logs(node);
    }

    if (r->current_idx <= match_idx)
        return 0;

    assert(r->current_idx <= raft_get_current_idx(me_));

    raft_node_set_next_idx(node, r->current_idx + 1);
    raft_node_set_match_idx(node, r->current_idx);

    /* Update commit idx */
    raft_index_t point = r->current_idx;
    if (point && raft_get_commit_idx(me_) < point)
    {
        raft_term_t term;
        int got = raft_get_entry_term(me_, point, &term);
        if (got && term == me->current_term)
        {
            int i, votes = 1;
            for (i = 0; i < me->num_nodes; i++)
            {
                raft_node_t* tmpnode = me->nodes[i];
                if (!raft_is_self(me_, tmpnode) &&
                    raft_node_is_voting(tmpnode) &&
                    point <= raft_node_get_match_idx(tmpnode))
                {
                    votes++;
                }
            }

            if (raft_get_num_voting_nodes(me_) / 2 < votes)
                raft_set_commit_idx(me_, point);
        }
    }

    /* Aggressively send remaining entries */
    if (raft_node_get_next_idx(node) <= raft_get_current_idx(me_))
        raft_send_appendentries(me_, node);

    /* periodic applies committed entries lazily */

    return 0;
}

int raft_recv_appendentries(
    raft_server_t* me_,
    raft_node_t* node,
    msg_appendentries_t* ae,
    msg_appendentries_response_t *r
    )
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int e = 0;

    if (0 < ae->n_entries)
        log_debug(me_, node, "recvd appendentries t:%ld ci:%ld lc:%ld pli:%ld plt:%ld #%d",
                  ae->term,
                  raft_get_current_idx(me_),
                  ae->leader_commit,
                  ae->prev_log_idx,
                  ae->prev_log_term,
                  ae->n_entries);

    r->success = 0;

    if (raft_is_candidate(me_) && me->current_term == ae->term)
    {
        raft_become_follower(me_);
    }
    else if (me->current_term < ae->term)
    {
        e = raft_set_current_term(me_, ae->term);
        if (0 != e)
            goto out;
        raft_become_follower(me_);
    }
    else if (ae->term < me->current_term)
    {
        /* 1. Reply false if term < currentTerm (§5.1) */
        log_debug(me_, node, "AE term %ld is less than current term %ld",
                  ae->term, me->current_term);
        goto out;
    }

    /* update current leader because ae->term is up to date */
    me->leader_id = raft_node_get_id(node);

    me->election_timer = me->cb.get_time(me_, me->udata);
    r->lease = me->election_timer + me->election_timeout;

    /* Not the first appendentries we've received */
    /* NOTE: the log starts at 1 */
    if (0 < ae->prev_log_idx)
    {
        /* 2. Reply false if log doesn't contain an entry at prevLogIndex
           whose term matches prevLogTerm (§5.3) */
        raft_term_t term;
        int got = raft_get_entry_term(me_, ae->prev_log_idx, &term);
        if (!got && raft_get_current_idx(me_) < ae->prev_log_idx)
        {
            log_debug(me_, node, "AE no log at prev_idx %ld", ae->prev_log_idx);
            goto out;
        }
        else if (got && term != ae->prev_log_term)
        {
            log_debug(me_, node, "AE term doesn't match prev_term (ie. %ld vs %ld) ci:%ld comi:%ld lcomi:%ld pli:%ld",
                      term, ae->prev_log_term, raft_get_current_idx(me_),
                      raft_get_commit_idx(me_), ae->leader_commit, ae->prev_log_idx);
            if (ae->prev_log_idx <= raft_get_commit_idx(me_))
            {
                /* Should never happen; something is seriously wrong! */
                log_error(me_, node, "AE prev conflicts with committed entry ci:%ld comi:%ld lcomi:%ld pli:%ld",
                          raft_get_current_idx(me_), raft_get_commit_idx(me_),
                          ae->leader_commit, ae->prev_log_idx);
                e = RAFT_ERR_SHUTDOWN;
                goto out;
            }
            /* Delete all the following log entries because they don't match */
            e = raft_delete_entry_from_idx(me_, ae->prev_log_idx);
            goto out;
        }
    }

    r->success = 1;
    r->current_idx = ae->prev_log_idx;

    /* 3. If an existing entry conflicts with a new one (same index
       but different terms), delete the existing entry and all that
       follow it (§5.3) */
    int i;
    for (i = 0; i < ae->n_entries; i++)
    {
        raft_entry_t* ety = &ae->entries[i];
        raft_index_t ety_index = ae->prev_log_idx + 1 + i;
        raft_term_t term;
        int got = raft_get_entry_term(me_, ety_index, &term);
        if (got && term != ety->term)
        {
            if (ety_index <= raft_get_commit_idx(me_))
            {
                /* Should never happen; something is seriously wrong! */
                log_error(me_, node, "AE entry conflicts with committed entry ci:%ld comi:%ld lcomi:%ld pli:%ld",
                          raft_get_current_idx(me_), raft_get_commit_idx(me_),
                          ae->leader_commit, ae->prev_log_idx);
                e = RAFT_ERR_SHUTDOWN;
                goto out;
            }
            e = raft_delete_entry_from_idx(me_, ety_index);
            if (0 != e)
                goto out;
            break;
        }
        else if (!got && raft_get_current_idx(me_) < ety_index)
            break;
        r->current_idx = ety_index;
    }

    /* 4. Append any new entries not already in the log */
    int k = ae->n_entries - i;
    e = raft_append_entries(me_, &ae->entries[i], &k);
    i += k;
    r->current_idx = ae->prev_log_idx + i;
    if (0 != e)
        goto out;

    /* 5. If leaderCommit > commitIndex, set commitIndex =
        min(leaderCommit, index of last new entry) */
    if (raft_get_commit_idx(me_) < ae->leader_commit)
    {
        raft_index_t new_commit_idx = min(ae->leader_commit, r->current_idx);
        if (raft_get_commit_idx(me_) < new_commit_idx)
            raft_set_commit_idx(me_, new_commit_idx);
    }

out:
    r->term = me->current_term;
    if (0 == r->success)
        r->current_idx = raft_get_current_idx(me_);
    r->first_idx = ae->prev_log_idx + 1;
    return e;
}

static int __should_grant_vote(raft_server_private_t* me, raft_node_t* node, msg_requestvote_t* vr)
{
    /* For a prevote, we could theoretically proceed to the votedFor check
     * below, if vr->term == currentTerm - 1. That, however, would only matter
     * if we had rejected a previous RequestVote from a third server, who must
     * have already won a prevote phase. Hence, we choose not to look into
     * votedFor for simplicity. */
    if (vr->term < raft_get_current_term((void*)me))
    {
        log_info((void*)me, node, "rejected requestvote%s for %d: stale term: %ld < %ld",
                 vr->prevote ? " (prevote)" : "", vr->candidate_id, vr->term,
                 raft_get_current_term((void*)me));
        return 0;
    }

    if (!vr->prevote && me->voted_for != -1 && me->voted_for != vr->candidate_id)
    {
        log_info((void*)me, node, "rejected requestvote%s for %d: voted for %d",
                 vr->prevote ? " (prevote)" : "", vr->candidate_id, me->voted_for);
        return 0;
    }

    /* Below we check if log is more up-to-date... */

    raft_index_t current_idx = raft_get_current_idx((void*)me);

    raft_term_t term;
    int got = raft_get_entry_term((void*)me, current_idx, &term);
    assert(got);
    if (term < vr->last_log_term)
        return 1;

    if (vr->last_log_term == term && current_idx <= vr->last_log_idx)
        return 1;

    log_info((void*)me, node, "rejected requestvote%s for %d: stale log: (%ld, %ld) < (%ld, %ld)",
             vr->prevote ? " (prevote)" : "", vr->candidate_id, vr->last_log_term, vr->last_log_idx,
             term, current_idx);
    return 0;
}

int raft_recv_requestvote(raft_server_t* me_,
                          raft_node_t* node,
                          msg_requestvote_t* vr,
                          msg_requestvote_response_t *r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    raft_time_t now = me->cb.get_time(me_, me->udata);
    int e = 0;

    if (!node)
        node = raft_get_node(me_, vr->candidate_id);

    /* Reject request if we have a leader or if we might have granted a lease
     * that hasn't expired to someone other than the candidate */
    if (me->state == RAFT_STATE_LEADER || raft_lease_granted(me_, vr->candidate_id, now))
    {
        log_info(me_, node, "rejected requestvote%s for %d: might violate lease",
                 vr->prevote ? " (prevote)" : "", vr->candidate_id);
        r->vote_granted = 0;
        goto done;
    }

    if (raft_get_current_term(me_) < vr->term)
    {
        e = raft_set_current_term(me_, vr->term);
        if (0 != e) {
            log_error(me_, node, "rejected requestvote%s for %d: could not update term: %d",
                      vr->prevote ? " (prevote)" : "", vr->candidate_id, e);
            r->vote_granted = 0;
            goto done;
        }
        raft_become_follower(me_);
        me->leader_id = -1;
    }

    if (__should_grant_vote(me, node, vr))
    {
        /* It shouldn't be possible for a leader or prevoted candidate to grant a vote
         * Both states would have voted for themselves
         * A candidate may grant a prevote though */
        assert(!raft_is_leader(me_) && (!raft_is_candidate(me_) || me->prevote || vr->prevote));

        r->vote_granted = 1;
        if (!vr->prevote)
        {
            e = raft_vote_for_nodeid(me_, vr->candidate_id);
            if (0 != e)
            {
                log_error(me_, node, "rejected requestvote%s for %d: could not update vote: %d",
                          vr->prevote ? " (prevote)" : "", vr->candidate_id, e);
                r->vote_granted = 0;
            }

            /* there must be in an election. */
            me->leader_id = -1;
            me->election_timer = now;
        }
    }
    else
    {
        /* It's possible the candidate node has been removed from the cluster but
         * hasn't received the appendentries that confirms the removal. Therefore
         * the node is partitioned and still thinks its part of the cluster. It
         * will eventually send a requestvote. This is error response tells the
         * node that it might be removed. */
        if (!node)
        {
            r->vote_granted = RAFT_REQUESTVOTE_ERR_UNKNOWN_NODE;
            goto done;
        }
        else
            r->vote_granted = 0;
    }

done:
    if (r->vote_granted)
        log_info(me_, node, "granted requestvote%s for %d",
                 vr->prevote ? " (prevote)" : "", vr->candidate_id);

    r->term = raft_get_current_term(me_);
    r->prevote = vr->prevote;
    return e;
}

int raft_votes_is_majority(const int num_nodes, const int nvotes)
{
    if (num_nodes < nvotes)
        return 0;
    int half = num_nodes / 2;
    return half + 1 <= nvotes;
}

int raft_recv_requestvote_response(raft_server_t* me_,
                                   raft_node_t* node,
                                   msg_requestvote_response_t* r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    log_info(me_, node, "node responded to requestvote%s status:%s ct:%ld rt:%ld",
             r->prevote ? " (prevote)" : "",
             r->vote_granted == 1 ? "granted" :
             r->vote_granted == 0 ? "not granted" : "unknown",
             me->current_term,
             r->term);

    if (!raft_is_candidate(me_) || me->prevote != r->prevote)
    {
        return 0;
    }
    else if (raft_get_current_term(me_) < r->term)
    {
        int e = raft_set_current_term(me_, r->term);
        if (0 != e)
            return e;
        raft_become_follower(me_);
        me->leader_id = -1;
        return 0;
    }
    else if (raft_get_current_term(me_) != r->term)
    {
        /* The node who voted for us would have obtained our term.
         * Therefore this is an old message we should ignore.
         * This happens if the network is pretty choppy. */
        return 0;
    }

    switch (r->vote_granted)
    {
        case RAFT_REQUESTVOTE_ERR_GRANTED:
            if (node)
                raft_node_vote_for_me(node, 1);
            return raft_count_votes(me_);

        case RAFT_REQUESTVOTE_ERR_NOT_GRANTED:
            break;

        case RAFT_REQUESTVOTE_ERR_UNKNOWN_NODE:
            if (raft_node_is_voting(raft_get_my_node(me_)) &&
                me->connected == RAFT_NODE_STATUS_DISCONNECTING)
                return RAFT_ERR_SHUTDOWN;
            break;

        default:
            assert(0);
    }

    return 0;
}

int raft_recv_installsnapshot(raft_server_t* me_,
                              raft_node_t* node,
                              msg_installsnapshot_t* is,
                              msg_installsnapshot_response_t* r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int e;

    r->term = me->current_term;
    r->last_idx = is->last_idx;
    r->complete = 0;

    if (is->term < me->current_term)
        return 0;

    if (me->current_term < is->term)
    {
        e = raft_set_current_term(me_, is->term);
        if (0 != e)
            return e;
        r->term = me->current_term;
    }

    if (!raft_is_follower(me_))
        raft_become_follower(me_);

    me->leader_id = raft_node_get_id(node);
    me->election_timer = me->cb.get_time(me_, me->udata);
    r->lease = me->election_timer + me->election_timeout;

    if (is->last_idx <= raft_get_commit_idx(me_))
    {
        /* Committed entries must match the snapshot. */
        r->complete = 1;
        return 0;
    }

    raft_term_t term;
    int got = raft_get_entry_term(me_, is->last_idx, &term);
    if (got && term == is->last_term)
    {
        raft_set_commit_idx(me_, is->last_idx);
        r->complete = 1;
        return 0;
    }

    assert(me->cb.recv_installsnapshot);
    e = me->cb.recv_installsnapshot(me_, me->udata, node, is, r);
    if (e < 0)
        return e;

    if (e == 1)
        r->complete = 1;

    return 0;
}

int raft_recv_installsnapshot_response(raft_server_t* me_,
                                       raft_node_t* node,
                                       msg_installsnapshot_response_t *r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!node)
        return -1;

    if (!raft_is_leader(me_))
        return RAFT_ERR_NOT_LEADER;

    if (me->current_term < r->term)
    {
        int e = raft_set_current_term(me_, r->term);
        if (0 != e)
            return e;
        raft_become_follower(me_);
        me->leader_id = -1;
        return 0;
    }
    else if (me->current_term != r->term)
        return 0;

    raft_node_set_lease(node, r->lease);

    assert(me->cb.recv_installsnapshot_response);
    int e = me->cb.recv_installsnapshot_response(me_, me->udata, node, r);
    if (0 != e)
        return e;

    /* The snapshot installation is complete. Update the node state. */
    if (r->complete && raft_node_get_match_idx(node) < r->last_idx)
    {
        raft_node_set_match_idx(node, r->last_idx);
        raft_node_set_next_idx(node, r->last_idx + 1);
    }

    if (raft_node_get_next_idx(node) <= raft_get_current_idx(me_))
        raft_send_appendentries(me_, node);

    return 0;
}

static int __cfg_change_is_valid(raft_server_private_t* me, msg_entry_t* ety)
{
    /* A membership change to a leader is either nonsense or dangerous
       (e.g., we append the entry locally and count voting nodes below
       without checking if ourself remains a voting node). */
    raft_node_id_t node_id = me->cb.log_get_node_id((void*)me, raft_get_udata((void*)me), ety, 0);
    if (node_id == raft_get_nodeid((void*)me))
        return 0;

    raft_node_t* node = raft_get_node((void*)me, node_id);
    switch (ety->type)
    {
        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
        case RAFT_LOGTYPE_ADD_NODE:
            if (node)
                return 0;
            break;

        case RAFT_LOGTYPE_DEMOTE_NODE:
        case RAFT_LOGTYPE_REMOVE_NODE:
            if (!node || !raft_node_is_voting(node))
                return 0;
            break;

        case RAFT_LOGTYPE_PROMOTE_NODE:
        case RAFT_LOGTYPE_REMOVE_NONVOTING_NODE:
            if (!node || raft_node_is_voting(node))
                return 0;
            break;

        default:
            assert(0);
    }

    return 1;
}

int raft_recv_entry(raft_server_t* me_,
                    msg_entry_t* ety,
                    msg_entry_response_t *r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    if (!raft_is_leader(me_))
        return RAFT_ERR_NOT_LEADER;

    if (raft_entry_is_cfg_change(ety))
    {
        /* Multi-threading: need to fail here because user might be
         * snapshotting membership settings. */
        if (raft_snapshot_is_in_progress(me_))
            return RAFT_ERR_SNAPSHOT_IN_PROGRESS;

        /* Only one voting cfg change at a time */
        if (raft_entry_is_voting_cfg_change(ety) &&
            raft_voting_change_is_in_progress(me_))
                return RAFT_ERR_ONE_VOTING_CHANGE_ONLY;

        if (!__cfg_change_is_valid(me, ety))
            return RAFT_ERR_INVALID_CFG_CHANGE;
    }

    log_debug(me_, NULL, "received entry t:%ld id: %d idx: %ld",
              me->current_term, ety->id, raft_get_current_idx(me_) + 1);

    ety->term = me->current_term;
    int k = 1;
    int e = raft_append_entries(me_, ety, &k);
    if (0 != e)
        return e;
    assert(k == 1);

    for (i = 0; i < me->num_nodes; i++)
    {
        raft_node_t* node = me->nodes[i];

        if (!node || raft_is_self(me_, node) ||
            !raft_node_is_voting(node))
            continue;

        /* Only send new entries.
         * Don't send the entry to peers who are behind, to prevent them from
         * becoming congested. */
        raft_index_t next_idx = raft_node_get_next_idx(node);
        if (next_idx == raft_get_current_idx(me_))
            raft_send_appendentries(me_, node);
    }

    /* if we're the only node, we can consider the entry committed */
    if (1 == raft_get_num_voting_nodes(me_))
        raft_set_commit_idx(me_, raft_get_current_idx(me_));

    r->id = ety->id;
    r->idx = raft_get_current_idx(me_);
    r->term = me->current_term;

    if (raft_entry_is_voting_cfg_change(ety))
        me->voting_cfg_change_log_idx = raft_get_current_idx(me_);

    return 0;
}

int raft_send_requestvote(raft_server_t* me_, raft_node_t* node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    msg_requestvote_t rv;
    int e = 0;

    assert(node);
    assert(!raft_is_self(me_, node));

    log_info(me_, node, "sending requestvote%s to: %d",
             me->prevote ? " (prevote)" : "", raft_node_get_id(node));

    rv.term = me->current_term;
    rv.last_log_idx = raft_get_current_idx(me_);
    rv.last_log_term = raft_get_last_log_term(me_);
    rv.candidate_id = raft_get_nodeid(me_);
    rv.prevote = me->prevote;
    if (me->cb.send_requestvote)
        e = me->cb.send_requestvote(me_, me->udata, node, &rv);
    return e;
}

int raft_append_entries(raft_server_t* me_, raft_entry_t* entries, int *n)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    return log_append(me->log, entries, n);
}

int raft_apply_entry(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (raft_snapshot_is_in_progress(me_))
        return -1;

    /* Don't apply after the commit_idx */
    if (me->last_applied_idx == raft_get_commit_idx(me_))
        return -1;

    raft_index_t log_idx = me->last_applied_idx + 1;

    raft_entry_t* ety = raft_get_entry_from_idx(me_, log_idx);
    if (!ety)
        return -1;

    log_debug(me_, NULL, "applying log: %ld, id: %d size: %u",
              log_idx, ety->id, ety->data.len);

    me->last_applied_idx++;
    if (me->cb.applylog)
    {
        int e = me->cb.applylog(me_, me->udata, ety, me->last_applied_idx);
        if (RAFT_ERR_SHUTDOWN == e)
            return RAFT_ERR_SHUTDOWN;
    }

    /* voting cfg change is now complete */
    if (log_idx == me->voting_cfg_change_log_idx)
        me->voting_cfg_change_log_idx = -1;

    return 0;
}

raft_entry_t* raft_get_entries_from_idx(raft_server_t* me_, raft_index_t idx, int* n_etys)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    return log_get_from_idx(me->log, idx, n_etys);
}

int raft_send_installsnapshot(raft_server_t* me_, raft_node_t* node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    msg_installsnapshot_t is = {};
    is.term = me->current_term;
    is.last_idx = log_get_base(me->log);
    is.last_term = log_get_base_term(me->log);

    log_debug(me_, node, "sending installsnapshot: ci:%ld comi:%ld t:%ld lli:%ld llt:%ld",
              raft_get_current_idx(me_),
              raft_get_commit_idx(me_),
              is.term,
              is.last_idx,
              is.last_term);

    assert(me->cb.send_installsnapshot);
    return me->cb.send_installsnapshot(me_, me->udata, node, &is);
}

int raft_send_appendentries(raft_server_t* me_, raft_node_t* node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    assert(node);
    assert(!raft_is_self(me_, node));

    if (!(me->cb.send_appendentries))
        return -1;

    msg_appendentries_t ae = {};
    ae.term = me->current_term;
    ae.leader_commit = raft_get_commit_idx(me_);

    raft_index_t next_idx = raft_node_get_next_idx(node);

    if (next_idx <= log_get_base(me->log))
        return raft_send_installsnapshot(me_, node);

    ae.entries = raft_get_entries_from_idx(me_, next_idx, &ae.n_entries);
    assert((!ae.entries && 0 == ae.n_entries) ||
           (ae.entries && 0 < ae.n_entries));

    ae.prev_log_idx = next_idx - 1;
    int got = raft_get_entry_term(me_, ae.prev_log_idx, &ae.prev_log_term);
    assert(got);

    log_debug(me_, node, "sending appendentries node: ci:%ld comi:%ld t:%ld lc:%ld pli:%ld plt:%ld",
              raft_get_current_idx(me_),
              raft_get_commit_idx(me_),
              ae.term,
              ae.leader_commit,
              ae.prev_log_idx,
              ae.prev_log_term);

    return me->cb.send_appendentries(me_, me->udata, node, &ae);
}

int raft_send_appendentries_all(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i, e;

    me->election_timer = me->cb.get_time(me_, me->udata);
    for (i = 0; i < me->num_nodes; i++)
    {
        if (raft_is_self(me_, me->nodes[i]))
            continue;

        e = raft_send_appendentries(me_, me->nodes[i]);
        if (0 != e)
            return e;
    }

    return 0;
}

raft_node_t* raft_add_node_internal(raft_server_t* me_, raft_entry_t *ety, void* udata, raft_node_id_t id, int is_self)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    /* we shouldn't add a node twice */
    raft_node_t* node = raft_get_node(me_, id);
    if (node)
        return NULL;

    node = raft_node_new(udata, id);
    if (!node)
        return NULL;
    if (raft_is_leader(me_))
        raft_node_set_effective_time(node, me->cb.get_time(me_, me->udata));
    void* p = realloc(me->nodes, sizeof(void*) * (me->num_nodes + 1));
    if (!p) {
        raft_node_free(node);
        return NULL;
    }
    me->num_nodes++;
    me->nodes = p;
    me->nodes[me->num_nodes - 1] = node;
    if (is_self)
        me->node_id = id;

    node = me->nodes[me->num_nodes - 1];

    if (me->cb.notify_membership_event)
        me->cb.notify_membership_event(me_, raft_get_udata(me_), node, ety, RAFT_MEMBERSHIP_ADD);

    return node;
}

raft_node_t* raft_add_node(raft_server_t* me_, void* udata, raft_node_id_t id, int is_self)
{
    return raft_add_node_internal(me_, NULL, udata, id, is_self);
}

static raft_node_t* raft_add_non_voting_node_internal(raft_server_t* me_, raft_entry_t *ety, void* udata, raft_node_id_t id, int is_self)
{
    raft_node_t* node = raft_add_node_internal(me_, ety, udata, id, is_self);
    if (!node)
        return NULL;

    raft_node_set_voting(node, 0);
    return node;
}

raft_node_t* raft_add_non_voting_node(raft_server_t* me_, void* udata, raft_node_id_t id, int is_self)
{
    return raft_add_non_voting_node_internal(me_, NULL, udata, id, is_self);
}

void raft_remove_node(raft_server_t* me_, raft_node_t* node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    assert(node);

    if (me->cb.notify_membership_event)
        me->cb.notify_membership_event(me_, raft_get_udata(me_), node, NULL, RAFT_MEMBERSHIP_REMOVE);

    int i, found = 0;
    for (i = 0; i < me->num_nodes; i++)
    {
        if (me->nodes[i] == node)
        {
            found = 1;
            break;
        }
    }
    assert(found);
    memmove(&me->nodes[i], &me->nodes[i + 1], sizeof(*me->nodes) * (me->num_nodes - i - 1));
    me->num_nodes--;

    raft_node_free(node);
}

int raft_get_nvotes_for_me(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i, votes;

    for (i = 0, votes = 0; i < me->num_nodes; i++)
    {
        if (raft_node_is_voting(me->nodes[i]) &&
            raft_node_has_vote_for_me(me->nodes[i]))
        {
            votes += 1;
        }
    }

    return votes;
}

int raft_vote(raft_server_t* me_, raft_node_t* node)
{
    return raft_vote_for_nodeid(me_, node ? raft_node_get_id(node) : -1);
}

int raft_vote_for_nodeid(raft_server_t* me_, const raft_node_id_t nodeid)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (me->cb.persist_vote) {
        int e = me->cb.persist_vote(me_, me->udata, nodeid);
        if (0 != e)
            return e;
    }
    me->voted_for = nodeid;
    return 0;
}

int raft_msg_entry_response_committed(raft_server_t* me_,
                                      const msg_entry_response_t* r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    raft_term_t term;
    int got = raft_get_entry_term(me_, r->idx, &term);
    if (!got)
    {
        if (r->idx <= log_get_base(me->log))
        {
            /* The entry has been compacted. */
            if (r->term == me->current_term)
                /* The index is committed in this term, so it must be ours. */
                return 1;
            else
                /* Impossible to know for sure. */
                return -1;
        }
        else
        {
            /* The entry is not stored on this replica yet. */
            return 0;
        }
    }

    /* entry from another leader has invalidated this entry message */
    if (r->term != term)
        return -1;
    return r->idx <= raft_get_commit_idx(me_);
}

int raft_apply_all(raft_server_t* me_)
{
    if (raft_snapshot_is_in_progress(me_))
        return 0;

    while (raft_get_last_applied_idx(me_) < raft_get_commit_idx(me_))
    {
        int e = raft_apply_entry(me_);
        if (0 != e)
            return e;
    }

    return 0;
}

int raft_entry_is_voting_cfg_change(raft_entry_t* ety)
{
    return RAFT_LOGTYPE_ADD_NODE == ety->type ||
           RAFT_LOGTYPE_PROMOTE_NODE == ety->type ||
           RAFT_LOGTYPE_DEMOTE_NODE == ety->type ||
           RAFT_LOGTYPE_REMOVE_NODE == ety->type;
}

int raft_entry_is_cfg_change(raft_entry_t* ety)
{
    return RAFT_LOGTYPE_ADD_NODE == ety->type ||
           RAFT_LOGTYPE_ADD_NONVOTING_NODE == ety->type ||
           RAFT_LOGTYPE_PROMOTE_NODE == ety->type ||
           RAFT_LOGTYPE_DEMOTE_NODE == ety->type ||
           RAFT_LOGTYPE_REMOVE_NONVOTING_NODE == ety->type ||
           RAFT_LOGTYPE_REMOVE_NODE == ety->type;
}

void raft_offer_log(raft_server_t* me_, raft_entry_t* entries,
                    int n_entries, raft_index_t idx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    for (i = 0; i < n_entries; i++)
    {
        raft_entry_t *ety = &entries[i];

        if (!raft_entry_is_cfg_change(ety))
            continue;

        if (raft_entry_is_voting_cfg_change(ety))
            me->voting_cfg_change_log_idx = idx + i;

        raft_node_id_t node_id = me->cb.log_get_node_id(me_, raft_get_udata(me_),
                                                        ety, idx + i);
        raft_node_t* node = raft_get_node(me_, node_id);
        int is_self = node_id == raft_get_nodeid(me_);

        switch (ety->type)
        {
            case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
                assert(!node);
                node = raft_add_non_voting_node_internal(me_, ety, NULL, node_id, is_self);
                assert(node);
                break;

            case RAFT_LOGTYPE_ADD_NODE:
                assert(!node);
                node = raft_add_node_internal(me_, ety, NULL, node_id, is_self);
                assert(node);
                break;

            case RAFT_LOGTYPE_PROMOTE_NODE:
                assert(node && !raft_node_is_voting(node));
                raft_node_set_voting(node, 1);
                break;

            case RAFT_LOGTYPE_DEMOTE_NODE:
                assert(node && raft_node_is_voting(node));
                raft_node_set_voting(node, 0);
                break;

            case RAFT_LOGTYPE_REMOVE_NODE:
                assert(node && raft_node_is_voting(node));
                raft_remove_node(me_, node);
                break;

            case RAFT_LOGTYPE_REMOVE_NONVOTING_NODE:
                assert(node && !raft_node_is_voting(node));
                raft_remove_node(me_, node);
                break;

            default:
                assert(0);
        }
    }
}

void raft_pop_log(raft_server_t* me_, raft_entry_t* entries,
                  int n_entries, raft_index_t idx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    for (i = n_entries - 1; i >= 0; i--)
    {
        raft_entry_t *ety = &entries[i];

        if (!raft_entry_is_cfg_change(ety))
            continue;

        if (idx + i <= me->voting_cfg_change_log_idx)
            me->voting_cfg_change_log_idx = -1;

        raft_node_id_t node_id = me->cb.log_get_node_id(me_, raft_get_udata(me_),
                                                        ety, idx + i);
        raft_node_t* node = raft_get_node(me_, node_id);
        int is_self = node_id == raft_get_nodeid(me_);

        switch (ety->type)
        {
            case RAFT_LOGTYPE_DEMOTE_NODE:
                assert(node && !raft_node_is_voting(node));
                raft_node_set_voting(node, 1);
                break;

            case RAFT_LOGTYPE_REMOVE_NODE:
                assert(!node);
                node = raft_add_node_internal(me_, ety, NULL, node_id, is_self);
                assert(node);
                break;

            case RAFT_LOGTYPE_REMOVE_NONVOTING_NODE:
                assert(!node);
                node = raft_add_non_voting_node_internal(me_, ety, NULL, node_id, is_self);
                assert(node);
                break;

            case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
                assert(node && !raft_node_is_voting(node));
                raft_remove_node(me_, node);
                break;

            case RAFT_LOGTYPE_ADD_NODE:
                assert(node && raft_node_is_voting(node));
                raft_remove_node(me_, node);
                break;

            case RAFT_LOGTYPE_PROMOTE_NODE:
                assert(node && raft_node_is_voting(node));
                raft_node_set_voting(node, 0);
                break;

            default:
                assert(0);
        }
    }
}

raft_index_t raft_get_first_entry_idx(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    assert(0 < raft_get_current_idx(me_));

    return log_get_base(me->log) + 1;
}

raft_index_t raft_get_num_snapshottable_logs(raft_server_t *me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    assert(log_get_base(me->log) <= raft_get_commit_idx(me_));
    return raft_get_commit_idx(me_) - log_get_base(me->log);
}

int raft_begin_snapshot(raft_server_t *me_, raft_index_t idx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (raft_get_commit_idx(me_) < idx)
        return -1;

    raft_entry_t* ety = raft_get_entry_from_idx(me_, idx);
    if (!ety)
        return -1;

    /* we need to get all the way to the commit idx */
    int e = raft_apply_all(me_);
    if (e != 0)
        return e;

    assert(raft_get_commit_idx(me_) == raft_get_last_applied_idx(me_));

    raft_set_snapshot_metadata(me_, ety->term, idx);
    me->snapshot_in_progress = 1;

    log_debug(me_, NULL,
              "begin snapshot sli:%ld slt:%ld slogs:%ld",
              me->snapshot_last_idx,
              me->snapshot_last_term,
              raft_get_num_snapshottable_logs(me_));

    return 0;
}

int raft_end_snapshot(raft_server_t *me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!me->snapshot_in_progress || me->snapshot_last_idx == 0)
        return -1;

    int e = log_poll(me->log, me->snapshot_last_idx);
    if (e != 0)
        return e;

    me->snapshot_in_progress = 0;

    log_debug(me_, NULL,
              "end snapshot base:%ld commit-index:%ld current-index:%ld",
              log_get_base(me->log),
              raft_get_commit_idx(me_),
              raft_get_current_idx(me_));

    return 0;
}

int raft_begin_load_snapshot(
    raft_server_t *me_,
    raft_term_t last_included_term,
    raft_index_t last_included_index)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (last_included_index == -1)
        return -1;

    if (last_included_term == me->snapshot_last_term && last_included_index == me->snapshot_last_idx)
        return RAFT_ERR_SNAPSHOT_ALREADY_LOADED;

    if (last_included_index <= raft_get_commit_idx(me_))
        return -1;

    log_load_from_snapshot(me->log, last_included_index, last_included_term);

    raft_set_commit_idx(me_, last_included_index);

    me->last_applied_idx = last_included_index;
    raft_set_snapshot_metadata(me_, last_included_term, me->last_applied_idx);

    /* remove all nodes */
    while (me->num_nodes > 0)
        raft_remove_node(me_, me->nodes[0]);

    /* this will be realloc'd by a raft_add_node */
    me->num_nodes = 0;

    log_debug(me_, NULL,
              "loaded snapshot sli:%ld slt:%ld slogs:%ld",
              me->snapshot_last_idx,
              me->snapshot_last_term,
              raft_get_num_snapshottable_logs(me_));

    return 0;
}

int raft_end_load_snapshot(raft_server_t *me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    /* Set nodes' voting status as committed */
    for (i = 0; i < me->num_nodes; i++)
    {
        raft_node_t* node = me->nodes[i];
        if (raft_node_is_voting(node))
            raft_node_set_has_sufficient_logs(node);
    }

    return 0;
}
