package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	// "crypto/rand"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	LEADER = iota
	FOLLOWER
	CANDIDATER
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	current_state int // 0:leader, 1:follower, 2:candidate
	// persistent state on all server
	currentTerm int
	votefor     int
	log         []interface{}
	// volatile state on all server
	commit_index int
	last_applied int
	// volatile state on leaders
	next_index  []int
	match_index []int

	//timer
	electionTimeout    int
	electionTimeCount  int
	electionChan       chan bool
	heartbeatTimeout   int
	heartbeatTimeCount int
	heartbeatChan      chan bool
	mu_timer           sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.current_state == LEADER
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term           int
	Candidate_id   int
	Last_log_index int
	Last_log_term  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	Vote_granted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	term := rf.currentTerm
	state := rf.current_state
	rf.mu.Unlock()
	DPrintf("[%d] requestVote to [%d](%d)", args.Candidate_id, rf.me, state)
	if term > args.Term { // 当前server任期大, 拒绝投票
		reply.Term = term
		reply.Vote_granted = false
	} else if term == args.Term { // 任期相同, 没有投过票可以投票
		rf.mu.Lock()
		if rf.votefor == -1 {
			reply.Term = term
			reply.Vote_granted = true
			rf.votefor = args.Candidate_id
		} else {
			reply.Term = term
			reply.Vote_granted = false
		}
		rf.mu.Unlock()
		rf.resetElectionTimer()
	} else { // 当前server任期小, 转FOLLOWER, 投票
		reply.Term = args.Term
		rf.mu.Lock()
		rf.currentTerm = args.Term
		if state != FOLLOWER {
			rf.current_state = FOLLOWER
			DPrintf("[%d] turn to follower", rf.me)
			// TODO: notify
		}
		rf.votefor = args.Candidate_id
		rf.mu.Unlock()
		rf.resetElectionTimer()
		reply.Term = args.Term
		reply.Vote_granted = true
	}
}

func (rf *Raft) startElection() {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	rf.currentTerm++
	rf.current_state = CANDIDATER
	rf.votefor = rf.me
	term := rf.currentTerm
	me := rf.me
	vote_count := 1
	rf.mu.Unlock()
	DPrintf("[%d] start election, term: %d", me, term)
	args := RequestVoteArgs{
		Term:         term,
		Candidate_id: me,
	}
	rf.resetElectionTimer()
	for i := 0; i < len(rf.peers); i++ {
		if i == me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
				DPrintf("[%d] sendRequestVote to [%d] failed", rf.me, i)
				return
			}
			if reply.Vote_granted {
				rf.mu.Lock()
				vote_count++
				DPrintf("[%d] get vote from [%d], current vote: %d", rf.me, i, vote_count)
				if vote_count > len(rf.peers)/2 {
					rf.current_state = LEADER
					go rf.sendHeartBeat(term)
					DPrintf("[%d] become leader", me)
					rf.resetHeartbeatTimer()
					go rf.heartbeatTimer()
				}
				rf.mu.Unlock()
			} else if reply.Term > term {
				rf.mu.Lock()
				rf.current_state = FOLLOWER
				DPrintf("[%d] turn to follower", rf.me)
				rf.currentTerm = reply.Term
				rf.votefor = -1
				rf.mu.Unlock()
			}
		}(i)
	}
}

type AppendEntryArgs struct {
	Term           int
	Leader_id      int
	Prev_log_index int
	Prev_log_term  int
	Entries        []interface{}
	Leader_commit  int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	state := rf.current_state
	term := rf.currentTerm
	rf.mu.Unlock()
	// DPrintf("[%d] received entries from [%d]", rf.me, args.Leader_id)
	if args.Entries == nil { // heartbeat
		DPrintf("[%d] state(%d) term(%d) received heartbeat from [%d] term(%d)", rf.me, state, term, args.Leader_id, args.Term)
		if term > args.Term {
			DPrintf("[%d]'s term(%d) > leader[%d]'s term(%d)", rf.me, term, args.Leader_id, args.Term)
			reply.Term = term
			reply.Success = false
		} else {
			rf.mu.Lock()
			if rf.currentTerm < args.Term {
				rf.votefor = -1
				rf.currentTerm = args.Term
			}
			if state != FOLLOWER {
				rf.current_state = FOLLOWER
				DPrintf("[%d] turn to follower", rf.me)
				// TODO: stop election
				// rf.electiontimer.Stop()
			}
			// rf.electiontimer.Stop()
			rf.mu.Unlock()
			rf.resetElectionTimer()
			reply.Term = args.Term
			reply.Success = true
		}
	} else {
		// append entries
	}
}

func (rf *Raft) sendHeartBeat(term int) {
	if rf.killed() {
		return
	}
	args := AppendEntryArgs{
		Term:      term,
		Leader_id: rf.me,
		Entries:   nil,
	}
	// DPrintf("[%d] send heartbeat", rf.me)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.resetElectionTimer()
			continue
		}
		go func(i int) {
			reply := AppendEntryReply{}
			ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
			if !ok {
				DPrintf("[%d] sendHeartBeat to [%d] failed", rf.me, i)
				return
			}
			if reply.Term > term {
				DPrintf("[%d] is not the latest term, start new election", rf.me)
				rf.mu.Lock()
				rf.current_state = FOLLOWER
				rf.currentTerm = reply.Term
				rf.mu.Unlock()
			}
		}(i)
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

func (rf *Raft) electionTimer() {
	for {
		time.Sleep(50 * time.Millisecond)
		if rf.killed() {
			return
		}
		rf.mu_timer.Lock()
		rf.electionTimeCount += 50
		if rf.electionTimeCount >= rf.electionTimeout {
			DPrintf("372: [%d] Election Timer Count = %d", rf.me, rf.electionTimeCount)
			rf.electionTimeCount = 0
			select {
			case rf.electionChan <- true:
				DPrintf("376: [%d] push to electionChan", rf.me)
				break
			default:
				DPrintf("[%d] electionTimer(): electionChan has contains", rf.me)
			}
		}
		rf.mu_timer.Unlock()
	}
}

func (rf *Raft) heartbeatTimer() {
	for {
		_, isLeader := rf.GetState()
		if !isLeader {
			return
		}
		time.Sleep(50 * time.Millisecond)
		if rf.killed() {
			return
		}
		rf.mu_timer.Lock()
		rf.heartbeatTimeCount += 50
		if rf.heartbeatTimeCount >= rf.heartbeatTimeout {
			rf.heartbeatTimeCount = 0
			select {
			case rf.heartbeatChan <- true:
				break
			default:
				// DPrintf("[%d] heartbeatTimer(): heartbeatChan has contains", rf.me)
			}
		}
		rf.mu_timer.Unlock()
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.mu_timer.Lock()
	rf.electionTimeCount = 0
	rf.mu_timer.Unlock()
	// DPrintf("[%d] reset election timer, %d", rf.me, rf.electionTimeout)
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.mu_timer.Lock()
	rf.heartbeatTimeCount = 0
	rf.mu_timer.Unlock()
	// DPrintf("[%d] reset heartbeat timer, %d", rf.me, rf.heartbeatTimeout)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionChan:
			DPrintf("[%d] election timeout", rf.me)
			rf.startElection()
		case <-rf.heartbeatChan:
			_, isLeader := rf.GetState()
			// if isLeader {
			// 	DPrintf("[%d] is Leader", rf.me)
			// } else {
			// 	DPrintf("[%d] is not Leader", rf.me)
			// }
			if isLeader {
				// DPrintf("[%d] send heart beat in ticker", rf.me)
				rf.resetHeartbeatTimer()
				rf.mu.Lock()
				term := rf.currentTerm
				rf.mu.Unlock()
				rf.sendHeartBeat(term)
			}
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// DPrintf("Make Raft\n")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.current_state = FOLLOWER
	rf.votefor = -1
	rf.electionTimeout = rand.Intn(500) + 500
	rf.heartbeatTimeout = 150
	DPrintf("electionTimeout = %d, heartbeatTimeout = %d", rf.electionTimeout, rf.heartbeatTimeout)
	rf.electionChan = make(chan bool, 1)
	rf.heartbeatChan = make(chan bool, 1)
	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().UnixNano())

	go rf.electionTimer()

	go rf.ticker()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
