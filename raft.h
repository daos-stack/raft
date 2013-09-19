
enum {
    RAFT_STATE_FOLLOWER,
    RAFT_STATE_CANDIDATE,
    RAFT_STATE_LEADER
};

/* messages */
enum {
    MSG_RequestVote,
    MSG_RequestVoteResponse,
    MSG_AppendEntries,
    MSG_AppendEntriesResponse
};

typedef struct {
    /* term candidate's term */
    int term;

    /* candidateId candidate requesting vote */
    int candidateID;

    /* index of candidate's last log entry */
    int lastLogIndex;

    /* term of candidate's last log entry */
    int lastLogTerm;
} msg_requestvote_t;

typedef struct {
    unsigned int id;
    unsigned char* data;
    unsigned int len;
} msg_command_t;

typedef struct {
    /* currentTerm, for candidate to update itself */
    int term;

    /* true means candidate received vote */
    int voteGranted;
} msg_requestvote_response_t;

typedef struct {
    int term;
    int leaderID;
    int prevLogIndex;
    int prevLogTerm;
    int n_entries;
    void* entries;
    int leaderCommit;
} msg_appendentries_t;

typedef struct {
    /* currentTerm, for leader to update itself */
    int term;

    /* success true if follower contained entry matching
     * prevLogIndex and prevLogTerm */
    int success;
} msg_appendentries_response_t;

typedef int (
    *func_send_f
)   (
    void *caller,
    void *udata,
    const int peer,
    const unsigned char *send_data,
    const int len
);

#ifndef HAVE_FUNC_LOG
#define HAVE_FUNC_LOG
typedef void (
    *func_log_f
)    (
    void *udata,
    void *src,
//    bt_peer_t * peer,
    const char *buf,
    ...
);
#endif

typedef struct {
    func_send_f send;
    func_log_f log;
} raft_external_functions_t;


typedef struct {
    int pass;
//    recv_appendentries_f recv_appendentries,
//    recv_requestvote_f recv_requestvote,
} raft_functions_t;


typedef struct {
    /*  Persistent state: */

    /*  the server's best guess of what the current term is
     *  starts at zero */
    int currentTerm;

    /* The candidate the server voted for in its current term,
     * or Nil if it hasn't voted for any.  */
    int votedFor;

    /* the log which is replicated */
    void* log;

    /*  Volatile state: */

    /* Index of highest log entry known to be committed */
    int commitIndex;

    /* Index of highest log entry applied to state machine */
    int lastApplied;

    /*  follower/leader/candidate indicator */
    int state;

    raft_functions_t *func;

    /* callbacks */
    raft_external_functions_t *ext_func;
    void* caller;

    int logSize;
} raft_server_t;



void* raft_new();

void raft_set_external_functions(void* r, raft_external_functions_t* funcs, void* caller);

void raft_election_start(void* r);

void raft_become_leader(raft_server_t* me);

void raft_become_candidate(raft_server_t* me);

int raft_receive_append_entries(raft_server_t* me, msg_appendentries_t* ae);

int raft_periodic(void* me_);

int raft_recv_appendentries(void* me_, int peer, msg_appendentries_t* ae);

int raft_recv_requestvote(void* me_, int peer, msg_requestvote_t* vr);

void raft_execute_command(void* me_);

void raft_set_election_timeout(void* me_, int millisec);

int raft_vote(void* me_, int peer);

int raft_add_peer(void* me_, void* peer_udata);

int raft_remove_peer(void* me_, int peer);

int raft_get_num_peers(void* me_);

int raft_recv_command(void* me_, int peer, msg_command_t* cmd);

int raft_get_log_size(void* me_);

void raft_set_current_term(void* me_,int term);

void raft_set_current_index(void* me_,int idx);

int raft_get_current_term(void* me_);

void raft_set_current_index(void* me_,int idx);

int raft_get_current_index(void* me_);

int raft_is_follower(void* me_);

int raft_is_leader(void* me_);

int raft_is_candidate(void* me_);

int raft_send_requestvote(void* me_, int peer);
