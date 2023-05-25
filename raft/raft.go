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

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"

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

type Entry struct {
	Term    int
	Command interface{}
}

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
	currentState int // 0:leader, 1:follower, 2:candidate
	// persistent state on all server
	currentTerm int
	votefor     int
	log         []Entry
	// volatile state on all server
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	//timer
	electionTimeout    int
	electionTimeCount  int
	electionChan       chan bool
	heartbeatTimeout   int
	heartbeatTimeCount int
	heartbeatChan      chan bool
	heartbeatTerm      int
	muTimer            sync.Mutex

	entryChan chan int
	//apply channel
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.currentState == LEADER
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votefor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)
	var current_term, vote_for int
	var log []Entry
	if decoder.Decode(&current_term) != nil ||
		decoder.Decode(&vote_for) != nil ||
		decoder.Decode(&log) != nil {
		DPrintf("[%d] readPersist(): decode error", rf.me)
	} else {
		DPrintf("[%d] decode result: currentTerm = %d, votefor = %d, log = %d", rf.me, current_term, vote_for, log)
		rf.currentTerm = current_term
		rf.votefor = vote_for
		rf.log = log
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
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
	state := rf.currentState
	oldvote := rf.votefor
	rf.mu.Unlock()
	DPrintf("[%d] requestVote to [%d](%d)", args.CandidateId, rf.me, state)
	if term > args.Term { // 当前server任期大, 拒绝投票
		DPrintf("[%d](%d) > [%d](%d)", rf.me, term, args.CandidateId, args.Term)
		reply.Term = term
		reply.VoteGranted = false
	} else {
		// 判断up to date
		isUpToDate := false
		lastIndex := len(rf.log)
		lastTerm := 0
		if lastIndex != 0 {
			lastTerm = rf.log[lastIndex-1].Term
		}
		DPrintf("[%d]() lastTerm:%d, lastIndex:%d, candidate[%d] lastTerm:%d, lastIndex:%d", rf.me, lastTerm, lastIndex, args.CandidateId, args.LastLogTerm, args.LastLogIndex)
		if lastTerm > args.LastLogTerm {
			isUpToDate = false
		} else if lastTerm == args.LastLogTerm {
			if lastIndex > args.LastLogIndex {
				isUpToDate = false
			} else {
				isUpToDate = true
			}
		} else {
			isUpToDate = true
		}
		if term < args.Term { // 当前server任期小, 转FOLLOWER
			DPrintf("[%d]'s term %d < candidate[%d]'s term %d", rf.me, term, args.CandidateId, args.Term)
			reply.Term = args.Term
			rf.mu.Lock()
			rf.currentTerm = args.Term
			rf.votefor = -1
			if state != FOLLOWER {
				rf.currentState = FOLLOWER
				DPrintf("[%d] in RequestVote(): turn to follower", rf.me)
			}
			rf.mu.Unlock()
		} else {
			reply.Term = term
		}
		DPrintf("[%d] older than candidate[%d] ? %t", rf.me, args.CandidateId, isUpToDate)
		rf.mu.Lock()
		if (rf.votefor == -1 || rf.votefor == args.CandidateId) && isUpToDate {
			rf.votefor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
		DPrintf("[%d] Votefor = %d", rf.me, rf.votefor)
		rf.mu.Unlock()
		if reply.VoteGranted == true {
			rf.resetElectionTimer()
		}
		// update persist state
		rf.mu.Lock()
		if oldvote != rf.votefor || term != rf.currentTerm {
			rf.persist()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	rf.currentTerm++
	rf.votefor = rf.me
	rf.persist()
	rf.currentState = CANDIDATER
	term := rf.currentTerm
	me := rf.me
	vote_count := 1
	lastLogIndex := len(rf.log)
	var lastLogTerm int
	if lastLogIndex == 0 {
		lastLogTerm = 0
	} else {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}
	rf.mu.Unlock()
	DPrintf("[%d] start election, term: %d", me, term)
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
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
			if reply.VoteGranted {
				rf.mu.Lock()
				vote_count++
				DPrintf("[%d] get vote from [%d], current vote: %d", rf.me, i, vote_count)
				// this server win the election
				if vote_count > len(rf.peers)/2 && rf.currentState != LEADER {
					rf.currentState = LEADER
					if len(rf.log) == 0 {
						go rf.sendHeartBeat(term)
					}
					rf.heartbeatTerm = term
					DPrintf("[%d] become leader", me)
					rf.resetHeartbeatTimer()
					go rf.heartbeatTimer()
					// init nextIndex and matchIndex
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.log) + 1
						rf.matchIndex[i] = 0
					}
				}
				rf.mu.Unlock()
			} else if reply.Term > term {
				rf.mu.Lock()
				rf.currentState = FOLLOWER
				DPrintf("[%d] in startElection(): turn to follower", rf.me)
				rf.currentTerm = reply.Term
				rf.votefor = -1
				rf.persist()
				rf.mu.Unlock()
			}
			if !reply.VoteGranted {
				DPrintf("[%d] refuse vote for [%d]", i, rf.me)
			}
		}(i)
	}
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term         int
	ConflictTerm int
	FirstIndex   int
	LastIndex    int
	Success      bool
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	state := rf.currentState
	term := rf.currentTerm
	lastIndex := len(rf.log) // maybe no Lock is fine
	// lastTerm := 0
	// if lastIndex != 0 {
	// 	lastTerm = rf.log[lastIndex-1].Term
	// }
	// rf.mu.Unlock()
	reply.Term = term
	// if term > leader's term, refuse
	if term > args.Term {
		DPrintf("[%d]'s term(%d) > leader[%d]'s term(%d)", rf.me, term, args.LeaderId, args.Term)
		// reply.Term = term
		reply.Success = false
		return
	}
	// DPrintf("[%d] received entries from [%d]", rf.me, args.LeaderId)
	// DPrintf("[%d] state(%d) term(%d) received heartbeat from [%d] term(%d)", rf.me, state, term, args.LeaderId, args.Term)
	// rf.mu.Lock()
	if term < args.Term {
		rf.votefor = args.LeaderId
		rf.currentTerm = args.Term
		rf.persist()
	}
	if state != FOLLOWER {
		rf.currentState = FOLLOWER
		DPrintf("[%d] turn to follower", rf.me)
	}
	// rf.mu.Unlock()
	rf.resetElectionTimer()
	// reply.Term = args.Term
	if args.Entries == nil { // heartbeat
		reply.Success = true
	} else {
		// append entries
		// (1) return false if log doesn't contain and entry at prevLogIndex
		// whose term matches prevLogTerm
		// (2) if an existing entry conflicts with a new one(same index but
		// different terms), delete the existing entry and all that following it
		DPrintf("Append process: [%d]'s lastIndex: %d, [%d]'s prevLogIndex: %d,log = %d, entry = %d", rf.me, lastIndex, args.LeaderId, args.PrevLogIndex, rf.log, args.Entries)
		// follower条目比leader少
		if lastIndex < args.PrevLogIndex {
			DPrintf("[%d]'s index < [%d]'s PrevLogIndex", rf.me, args.LeaderId)
			if lastIndex == 0 {
				reply.ConflictTerm = 0
				reply.LastIndex = 0
				reply.FirstIndex = 0
			} else {
				reply.ConflictTerm = rf.log[lastIndex-1].Term
				first := lastIndex
				for ; first >= 1 && rf.log[first-1].Term == reply.ConflictTerm; first-- {
				}
				reply.LastIndex = lastIndex
				reply.FirstIndex = first + 1
			}
			reply.Success = false
			return
			// log数量相同但Term不同
		} else if lastIndex != 0 && lastIndex == args.PrevLogIndex && rf.log[lastIndex-1].Term != args.PrevLogTerm {
			DPrintf("[%d]'s last Index == [%d]'s PrevLogIndex but different Term", rf.me, args.LeaderId)
			reply.ConflictTerm = rf.log[lastIndex-1].Term
			first := lastIndex
			for ; first >= 1 && rf.log[first-1].Term == reply.ConflictTerm; first-- {
			}
			reply.LastIndex = lastIndex
			reply.FirstIndex = first + 1
			reply.Success = false
			return
		} else if lastIndex == args.PrevLogIndex+1 && rf.log[lastIndex-1].Term == args.Term {
			DPrintf("[%d] command was appended", rf.me)
			reply.Success = true
			return
		}
		shouldAppend := true
		if lastIndex > args.PrevLogIndex && args.PrevLogIndex > 0 {
			matchTerm := rf.log[args.PrevLogIndex].Term
			if matchTerm != args.Term {
				len1 := len(rf.log)
				rf.log = rf.log[:args.PrevLogIndex-1]
				rf.persist()
				len2 := len(rf.log)
				DPrintf("[%d]'s term != [%d]'s Term, delete following entries, len: %d -> %d", rf.me, args.LeaderId, len1, len2)
			}
			// 删除后log数量不匹配
			if len(rf.log) < args.PrevLogIndex {
				DPrintf("[%d]'s index < [%d]'s PrevLogIndex after delete", rf.me, args.LeaderId)
				if len(rf.log) == 0 {
					reply.ConflictTerm = 0
					reply.FirstIndex = 0
					reply.LastIndex = 0
				} else {
					reply.ConflictTerm = rf.log[len(rf.log)-1].Term
					first := len(rf.log)
					for ; first >= 1 && rf.log[first-1].Term == reply.ConflictTerm; first-- {
					}
					reply.FirstIndex = first + 1
					reply.LastIndex = len(rf.log)
				}
				reply.Success = false
				return
			}
		}
		if len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.Term {
			DPrintf("[%d]: entry already appended", rf.me)
			shouldAppend = false
		}
		reply.Success = true
		if shouldAppend {
			applen1 := len(rf.log)
			rf.log = append(rf.log, args.Entries...)
			rf.persist()
			applen2 := len(rf.log)
			DPrintf("[%d] append entry from [%d], len of log: %d -> %d, entry: %d", rf.me, args.LeaderId, applen1, applen2, rf.log)
		}
	}
	// 当前term的entry提交后，前面的entry才能提交
	if args.LeaderCommit > rf.commitIndex {
		shouldCommit := false
		// find a log which's term is currentTerm and is committed by leader
		for i := rf.commitIndex + 1; i <= args.LeaderCommit && i <= len(rf.log); i++ {
			if rf.log[i-1].Term == rf.currentTerm {
				shouldCommit = true
			}
		}
		if shouldCommit {
			DPrintf("[%d] update commit,  commitIndex = %d, leaderCommit = %d", rf.me, rf.commitIndex, args.LeaderCommit)
			lastNewEntry := lastIndex + len(args.Entries) // Index of the last entry
			if args.LeaderCommit < lastNewEntry {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = lastNewEntry
			}
			// apply to state machine
			for rf.commitIndex > rf.lastApplied && rf.lastApplied < len(rf.log) {
				DPrintf("[%d] apply to state machine, lastApplied = %d, commitIndex = %d", rf.me, rf.lastApplied, rf.commitIndex)

				apply := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied].Command,
					CommandIndex: rf.lastApplied + 1,
				}
				rf.applyCh <- apply
				rf.lastApplied++
				DPrintf("[%d] Apply commitIndex: %d, lastApplied: %d, command: %d", rf.me, rf.commitIndex, rf.lastApplied, rf.log[rf.lastApplied-1].Command)
			}
		}
	}
}

func (rf *Raft) sendHeartBeat(term int) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	prevLogIndex := len(rf.log)
	var prevLogTerm int
	if prevLogIndex == 0 {
		prevLogTerm = term
	} else {
		prevLogTerm = rf.log[prevLogIndex-1].Term
	}
	leaderCommit := rf.commitIndex
	isLeader := rf.currentState
	rf.mu.Unlock()
	if isLeader != LEADER {
		return
	}
	args := AppendEntryArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      nil,
		LeaderCommit: leaderCommit,
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
				// DPrintf("[%d] sendHeartBeat to [%d] failed", rf.me, i)
				return
			}
			rf.mu.Lock()
			term = rf.currentTerm
			if reply.Term > term {
				rf.currentState = FOLLOWER
				rf.currentTerm = reply.Term
				rf.votefor = -1
				rf.persist()
				DPrintf("[%d] in sendHeartBeat(): is not the latest term, term(%d -> %d), turn to follower", rf.me, term, reply.Term)
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) sendEntries() {
	for {
		if rf.killed() {
			return
		}
		var sendIndex int
		hasEntry := false
		select {
		case sendIndex = <-rf.entryChan:
			hasEntry = true
		default:
		}
		if !hasEntry {
			continue
		}
		rf.mu.Lock()
		entries := []Entry{rf.log[sendIndex-1]}
		term := rf.currentTerm
		leaderId := rf.me
		prevLogIndex := sendIndex - 1
		var prevLogTerm int
		if prevLogIndex == 0 {
			prevLogTerm = rf.currentTerm
		} else {
			prevLogTerm = rf.log[prevLogIndex-1].Term
		}
		leaderCommit := rf.commitIndex
		DPrintf("[%d] sendEntries: entry = %d, log = %d, send index = %d", rf.me, entries, rf.log, sendIndex)
		rf.mu.Unlock()
		appendCount := 1
		var appendCountMtx sync.Mutex
		for i := 0; i < len(rf.peers); i++ {
			if i == leaderId {
				continue
			}
			go func(i int) {
				args := AppendEntryArgs{
					Term:         term,
					LeaderId:     leaderId,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: leaderCommit,
				}
				for {
					DPrintf("[%d] before Call RPC to [%d], args = %d", rf.me, i, args)
					reply := AppendEntryReply{}
					ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
					for !ok {
						// DPrintf("[%d] sendEntries to [%d] failed through RPC, retry...", leaderId, i)
						if rf.killed() {
							return
						}
						ok = rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
						time.Sleep(10 * time.Millisecond)
					}
					if reply.Term > term {
						DPrintf("[%d] can't sendEntry to [%d], term out of date", rf.me, i)
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentState = FOLLOWER
							rf.currentTerm = reply.Term
							rf.votefor = -1
							rf.persist()
							DPrintf("[%d] in sendEntries(): is not the latest term, term(%d -> %d), turn to follower", rf.me, term, reply.Term)
						}
						rf.mu.Unlock()
						break
					} else if !reply.Success {
						DPrintf("[%d] send entries to [%d] failed, reply's CT: %d, FI: %d, LI: %d", rf.me, i, reply.ConflictTerm, reply.FirstIndex, reply.LastIndex)
						if reply.ConflictTerm == 0 {
							args.PrevLogIndex = 0
							args.PrevLogTerm = 0
							// args.Entries = rf.log[0:sendIndex]
						} else {
							hasTerm := false
							idx := len(rf.log)
							for ; idx >= 1; idx-- {
								if rf.log[idx-1].Term == reply.ConflictTerm {
									hasTerm = true
									break
								}
							}
							// DPrintf("[%d] idx = %d", rf.me, idx)
							if hasTerm {
								args.PrevLogTerm = reply.ConflictTerm
								if reply.LastIndex == idx {
									args.PrevLogIndex = reply.LastIndex
									// args.Entries = rf.log[args.PrevLogIndex:sendIndex]
								} else if reply.LastIndex > idx {
									args.PrevLogIndex = idx
									// args.Entries = rf.log[args.PrevLogIndex:sendIndex]
								} else {
									args.PrevLogIndex = reply.LastIndex
									// args.Entries = rf.log[args.PrevLogIndex:sendIndex]
								}
							} else {
								args.PrevLogIndex = reply.FirstIndex - 1
								if args.PrevLogIndex == 0 {
									args.PrevLogTerm = 0
								} else {
									args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
								}
							}
						}
						DPrintf("[%d] in sendEntry: args.PrevLogIndex = %d", rf.me, args.PrevLogIndex)
						args.Entries = rf.log[args.PrevLogIndex:sendIndex]
						// rf.nextIndex[i] = reply.FirstIndex
						// args.PrevLogIndex = reply.FirstIndex - 1
						// if args.PrevLogIndex == 0 {
						// 	args.PrevLogTerm = 0
						// } else {
						// 	args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
						// }
						// args.Entries = rf.log[reply.FirstIndex:sendIndex]
					} else { // append success
						appendCountMtx.Lock()
						appendCount++
						if appendCount > len(rf.peers)/2 {
							rf.mu.Lock()
							rf.commitIndex = prevLogIndex + 1
							// apply to state machine
							for rf.commitIndex > rf.lastApplied {
								// DPrintf("[%d] apply to state machine, lastApplied = %d, commitIndex = %d", rf.me, rf.lastApplied, rf.commitIndex)
								// DPrintf("DEBUG: [%d] len of log = %d, lastApplied = %d", rf.me, len(rf.log), rf.lastApplied)
								apply := ApplyMsg{
									CommandValid: true,
									Command:      rf.log[rf.lastApplied].Command,
									CommandIndex: rf.lastApplied + 1,
								}
								rf.applyCh <- apply
								DPrintf("[%d] Apply commitIndex: %d, lastApplied: %d, command: %d", rf.me, rf.commitIndex, rf.lastApplied, rf.log[rf.lastApplied].Command)
								rf.lastApplied++
							}
							rf.mu.Unlock()
						}
						appendCountMtx.Unlock()
						break
					}
				}
			}(i)
		}
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
		rf.muTimer.Lock()
		rf.electionTimeCount += 50
		if rf.electionTimeCount >= rf.electionTimeout {
			// DPrintf("372: [%d] Election Timer Count = %d", rf.me, rf.electionTimeCount)
			rf.electionTimeCount = 0
			select {
			case rf.electionChan <- true:
				// DPrintf("376: [%d] push to electionChan", rf.me)
				break
			default:
				// DPrintf("[%d] electionTimer(): electionChan has contains", rf.me)
			}
		}
		rf.muTimer.Unlock()
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
		rf.muTimer.Lock()
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
		rf.muTimer.Unlock()
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.muTimer.Lock()
	rf.electionTimeCount = 0
	rf.muTimer.Unlock()
	// DPrintf("[%d] reset election timer, %d", rf.me, rf.electionTimeout)
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.muTimer.Lock()
	rf.heartbeatTimeCount = 0
	rf.muTimer.Unlock()
	// DPrintf("[%d] reset heartbeat timer, %d", rf.me, rf.heartbeatTimeout)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionChan:
			// DPrintf("[%d] election timeout", rf.me)
			// rf.mu.Lock()
			// state := rf.currentState
			// rf.mu.Unlock()
			// if state != CANDIDATER {
			rf.startElection()
			// } else {
			// DPrintf("[%d] is a candidate, can't start election", rf.me)
			// }
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
				term := rf.heartbeatTerm
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
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}
	entry := Entry{Term: term, Command: command}
	rf.mu.Lock()
	index = rf.nextIndex[rf.me]
	rf.nextIndex[rf.me]++
	rf.log = append(rf.log, entry)
	rf.persist()
	DPrintf("[%d] Start(): log = %d", rf.me, rf.log)
	rf.mu.Unlock()
	rf.entryChan <- index
	// DPrintf("Start(): index = %d, command = %d", index, command)
	// <-waitAppendChan // wait for append success
	DPrintf("Start() return: index = %d, term = %d", index, term)
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
	rf.currentState = FOLLOWER
	rf.currentTerm = 0
	rf.votefor = -1
	rf.electionTimeout = rand.Intn(500) + 500
	rf.heartbeatTimeout = 150
	DPrintf("electionTimeout = %d, heartbeatTimeout = %d", rf.electionTimeout, rf.heartbeatTimeout)
	rf.electionChan = make(chan bool, 1)
	rf.heartbeatChan = make(chan bool, 1)
	rf.heartbeatTerm = 0
	// 2B paramenter init
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.entryChan = make(chan int)
	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().UnixNano())

	go rf.electionTimer()

	go rf.ticker()

	go rf.sendEntries()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
