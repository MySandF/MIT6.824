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
	DPrintf("[%d] requestVote to [%d](%d)", args.CandidateId, rf.me, state)
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
	// 判断是否投票
	if term > args.Term { // 当前server任期大, 拒绝投票
		DPrintf("[%d](%d) > [%d](%d)", rf.me, term, args.CandidateId, args.Term)
		reply.Term = term
		reply.VoteGranted = false
	} else {
		if term < args.Term { // 当前server任期小, 转FOLLOWER, 投票
			DPrintf("[%d]'s term %d < candidate[%d]'s term %d", rf.me, term, args.CandidateId, args.Term)
			reply.Term = args.Term
			rf.currentTerm = args.Term
			rf.votefor = -1
			if state != FOLLOWER {
				rf.currentState = FOLLOWER
				DPrintf("[%d] in RequestVote(): turn to follower", rf.me)
			}
		} else {
			reply.Term = term
		}
		DPrintf("[%d] older than candidate[%d] ? %t", rf.me, args.CandidateId, isUpToDate)
		DPrintf("[%d] Votefor = %d", rf.me, rf.votefor)
		if (rf.votefor == -1 || rf.votefor == args.CandidateId) && isUpToDate {
			rf.votefor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
		rf.resetElectionTimer()
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	rf.currentTerm++
	rf.currentState = CANDIDATER
	rf.votefor = rf.me
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
	DPrintf("[%d] start election, term: %d", me, term)
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()
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
			rf.mu.Lock()
			if reply.VoteGranted {
				vote_count++
				DPrintf("[%d] get vote from [%d], current vote: %d", rf.me, i, vote_count)
				// this server win the election
				if vote_count > len(rf.peers)/2 && rf.currentState != LEADER {
					rf.currentState = LEADER
					rf.heartbeatTerm = term
					DPrintf("[%d] become leader", me)
					rf.resetHeartbeatTimer()
					// init nextIndex and matchIndex
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.log) + 1
						rf.matchIndex[i] = 0
					}
					go rf.sendHeartBeat(term)
					go rf.heartbeatTimer()
				}
			} else if reply.Term > term {
				rf.currentState = FOLLOWER
				DPrintf("[%d] turn to follower", rf.me)
				rf.currentTerm = reply.Term
				rf.votefor = -1
			}
			if !reply.VoteGranted {
				DPrintf("[%d] refuse vote for [%d]", i, rf.me)
			}
			rf.mu.Unlock()
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
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	state := rf.currentState
	term := rf.currentTerm
	index := len(rf.log) // maybe no Lock is fine
	reply.Term = term
	// if term > leader's term, refuse
	if term > args.Term {
		DPrintf("[%d]'s term(%d) > leader[%d]'s term(%d)", rf.me, term, args.LeaderId, args.Term)
		// reply.Term = term
		reply.Success = false
		return
	}
	// DPrintf("[%d] received entries from [%d]", rf.me, args.LeaderId)
	if args.Entries == nil { // heartbeat
		// DPrintf("[%d] state(%d) term(%d) received heartbeat from [%d] term(%d)", rf.me, state, term, args.LeaderId, args.Term)
		if rf.currentTerm < args.Term {
			rf.votefor = -1
			rf.currentTerm = args.Term
		}
		if state != FOLLOWER {
			rf.currentState = FOLLOWER
			DPrintf("[%d] turn to follower", rf.me)
		}
		rf.resetElectionTimer()
		reply.Term = args.Term
		reply.Success = true
	} else {
		// append entries
		// if index and term not found, refuse
		if term == args.Term && index < args.PrevLogIndex {
			DPrintf("[%d]'s index < [%d]'s PrevLogIndex", rf.me, args.LeaderId)
			reply.Success = false
			return
		} else if index == args.PrevLogIndex && term != args.Term {
			DPrintf("[%d]'s term != [%d]'s Term, delete following entries", rf.me, args.LeaderId)
			rf.log = rf.log[:index]
		}
		reply.Success = true
		rf.log = append(rf.log, args.Entries...)
		DPrintf("[%d] append entry from [%d], current len of log: %d, last command: %d", rf.me, args.LeaderId, len(rf.log), args.Entries[len(args.Entries)-1].Command)
		// reply.Term = term
	}
	if args.LeaderCommit > rf.commitIndex {
		DPrintf("[%d] update commit,  commitIndex = %d, leaderCommit = %d", rf.me, rf.commitIndex, args.LeaderCommit)
		lastNewEntry := index + len(args.Entries)
		if args.LeaderCommit < lastNewEntry {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewEntry
		}
		// apply to state machine
		for rf.commitIndex > rf.lastApplied {
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
	rf.mu.Unlock()
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
			if reply.Term > term {
				rf.currentState = FOLLOWER
				rf.currentTerm = reply.Term
				rf.votefor = -1
				DPrintf("[%d] sendHeartBeat(): is not the latest term, term(%d -> %d), turn to follower", rf.me, term, rf.currentTerm)
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) sendEntries(entries []Entry, waitAppendChan chan bool) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	term := rf.currentTerm
	leaderId := rf.me
	prevLogIndex := len(rf.log) - len(entries)
	var prevLogTerm int
	if prevLogIndex == 0 {
		prevLogTerm = rf.currentTerm
	} else {
		prevLogTerm = rf.log[prevLogIndex-1].Term
	}
	leaderCommit := rf.commitIndex
	appendCount := 1
	var appendCountMtx sync.Mutex
	rf.mu.Unlock()
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
				rf.mu.Lock()
				defer rf.mu.Unlock()
				term = rf.currentTerm
				if reply.Term > term {
					rf.currentState = FOLLOWER
					rf.currentTerm = reply.Term
					rf.votefor = -1
					DPrintf("[%d] in sendEntries() is not the latest term, term(%d -> %d), turn to follower", rf.me, term, reply.Term)
					break
				} else if !reply.Success {
					DPrintf("[%d] send entries to [%d] failed", rf.me, i)
					rf.nextIndex[i]--
					args.PrevLogIndex--
					args.Term = rf.currentTerm
					args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
					args.Entries = append([]Entry{rf.log[args.PrevLogIndex]}, args.Entries...)
				} else { // append success
					appendCountMtx.Lock()
					appendCount++
					if appendCount > len(rf.peers)/2 {
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
						// select {
						// case waitAppendChan <- true:
						// 	// DPrintf("check here: push to waitAppendChan")
						// 	break
						// default:
						// 	break
						// }
					}
					appendCountMtx.Unlock()
					break
				}
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
	rf.mu.Unlock()
	entries := make([]Entry, 1)
	entries[0] = entry
	waitAppendChan := make(chan bool)
	go rf.sendEntries(entries, waitAppendChan)
	DPrintf("Start(): index = %d, command = %d", index, command)
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
	// 2B paramenter init
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.lastApplied = 0
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().UnixNano())

	go rf.electionTimer()

	go rf.ticker()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
