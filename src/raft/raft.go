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
	//	"bytes"

	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	HEARTBEAT_INTERVAL = time.Duration(100 * time.Millisecond)
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftState string

const (
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	LEADER    = "leader"
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

	// Server state
	State RaftState

	ApplyCh chan ApplyMsg

	// Persistent state on all servers
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry

	// Volatile state on all servers
	CommitIndex int
	LastApplied int

	// Volatile state on leaders
	NextIndex  []int
	MatchIndex []int

	// Time related variables
	RandomElectionTimeout time.Duration
	LastWakeup            time.Time

	notHandled           int
	notHandledLogEntries []LogEntry
	lastHandle           time.Time

	lastSendAE []time.Time
}

//
// Each item in log[] is a LogEntry
//
type LogEntry struct {
	Term    int
	Command interface{}
}

// AppendEntries RPC uses this struct
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediatelypreceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store(empty for heartbeat; may send more than one for eff)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	NewNextIndex int // optimization, back up nextIndex by more than one entry at a time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.CurrentTerm
	isleader = rf.State == RaftState(LEADER)
	rf.mu.Unlock()

	return term, isleader
}

func (rf *Raft) informOthers() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.State == RaftState(LEADER) {
			lastLogIndex := len(rf.Log) - 1
			for i, e := range rf.NextIndex {
				if i == rf.me {
					continue
				}

				if lastLogIndex >= e {
					go rf.sendAE_one(i, false)
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(HEARTBEAT_INTERVAL)
	}
}

func (rf *Raft) sendAE_one(index int, isHeartBeat bool) {
	rf.mu.Lock()

	if rf.State != RaftState(LEADER) {
		rf.mu.Unlock()
		return
	}

	until := len(rf.Log) - 1

	oldCurrentTerm := rf.CurrentTerm
	oldPrevLogIndex := rf.NextIndex[index] - 1
	oldPrevLogTerm := rf.Log[oldPrevLogIndex].Term
	oldLeaderCommit := rf.CommitIndex
	logEntries := []LogEntry{}
	if !isHeartBeat {
		logEntries = make([]LogEntry, len(rf.Log))
		copy(logEntries, rf.Log)
		logEntries = logEntries[oldPrevLogIndex+1 : until+1]
	}

	args := AppendEntriesArgs{
		Term:         oldCurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: oldPrevLogIndex,
		PrevLogTerm:  oldPrevLogTerm, //seems the first index 1 better
		Entries:      logEntries,
		LeaderCommit: oldLeaderCommit,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	network := rf.sendAppendEntries(index, &args, &reply)
	if !network {
		rf.mu.Lock()
		DPrintf("Server %v ci %v [%v] term %v AE lost %v", rf.me, rf.CommitIndex, rf.State, rf.CurrentTerm, index)
		rf.mu.Unlock()
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > oldCurrentTerm && rf.CurrentTerm == oldCurrentTerm {
		DPrintf("Server %v ci %v [%v -> follower] due to heartbeat reply", rf.me, rf.CommitIndex, rf.State)
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.persist()
		rf.State = RaftState(FOLLOWER)
	} else if rf.CurrentTerm == oldCurrentTerm {
		// remains leader
		if reply.Success {
			rf.MatchIndex[index] = max(rf.MatchIndex[index], until)
			rf.NextIndex[index] = rf.MatchIndex[index] + 1
			// EPrintf("Success! rf.NextIndex[%v] = %v", index, rf.NextIndex[index])
		} else {
			EPrintf("reply.NewNextIndex = %v", reply.NewNextIndex)
			EPrintf("rf.NextIndex[%v] = min(%v, %v)", index, oldPrevLogIndex, reply.NewNextIndex)
			rf.NextIndex[index] = min3(rf.NextIndex[index], oldPrevLogIndex, reply.NewNextIndex)
			// rf.NextIndex[index] = 1
			// rf.NextIndex[index]--
			EPrintf("Fail! rf.NextIndex[%v] = %v", index, rf.NextIndex[index])
			DPrintf("Server %v ci %v [%v] term %v Decri!, nextindex %v at term %v", rf.me, rf.CommitIndex, rf.State, rf.CurrentTerm, rf.NextIndex, rf.CurrentTerm)
		}
	}
}

func (rf *Raft) heartbeat() {
	// On election, send heartbeat immediately
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAE_one(i, true)
	}

	for !rf.killed() {
		rf.mu.Lock()

		if rf.State == RaftState(LEADER) {
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				if time.Since(rf.lastSendAE[i]) > HEARTBEAT_INTERVAL {
					go rf.sendAE_one(i, true)
				}
			}
		}
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Log []LogEntry
	if d.Decode(&CurrentTerm) != nil || d.Decode(&VotedFor) != nil || d.Decode(&Log) != nil {
		log.Fatalf("Having trouble!")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Log = Log
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// RequestVote RPC handler.
//
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Server %v ci %v [%v] term %v handling RV, log = %v", rf.me, rf.CommitIndex, rf.State, rf.CurrentTerm, rf.Log)

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.CurrentTerm {
		DPrintf("Server %v ci %v [%v -> follower] due to late Term in Handle RV", rf.me, rf.CommitIndex, rf.State)
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
		rf.State = RaftState(FOLLOWER)
	}

	// Grant vote when
	// 1. votedFor is null or candidate, and
	// 2. candidate's log is at least as up-to-date as receiver's log
	DPrintf("Server %v ci %v: To vote or not to vote?\nrf.VotedFor = %v, args.CandidateId = %v, args.LastLogTerm = %v, lastTerm = %v, args.LastLogIndex = %v, lastIndex = %v", rf.me, rf.CommitIndex, rf.VotedFor, args.CandidateId, args.LastLogTerm, rf.Log[len(rf.Log)-1].Term, args.LastLogIndex, len(rf.Log)-1)
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.Log[len(rf.Log)-1].Term || (args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex >= len(rf.Log)-1)) {
		DPrintf("Server %v ci %v: Vote", rf.me, rf.CommitIndex)
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		rf.persist()
		rf.wake()
		return
	}
	DPrintf("Server %v ci %v: Not to vote", rf.me, rf.CommitIndex)

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var info string
	if len(args.Entries) == 0 {
		info = "heartbeat"
	} else {
		info = fmt.Sprintf("AE with %v entries", len(args.Entries))
	}

	DPrintf("Server %v ci %v [%v] term %v [%v] handling %v from %v", rf.me, rf.CommitIndex, rf.State, rf.CurrentTerm, rf.Log, info, args.LeaderId)

	// First handle the case when term > currentTerm
	if args.Term > rf.CurrentTerm {
		DPrintf("Server %v ci %v [%v -> follower] due to late Term in Handle AE", rf.me, rf.CommitIndex, rf.State)
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
		rf.State = RaftState(FOLLOWER)
	}
	// 1. Reply false if term < currentTerm
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	rf.wake()

	// 2. Reply false if log doesn't contain an entry
	// at prevLogIndex whose term matches prevLogTerm
	DPrintf("Server %v args.PrevLogIndex = %v args.PrevLogTerm = %v", rf.me, args.PrevLogIndex, args.PrevLogTerm)
	if len(rf.Log)-1 < args.PrevLogIndex ||
		rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		if len(rf.Log)-1 < args.PrevLogIndex {
			reply.NewNextIndex = len(rf.Log)
		} else {
			tmp := args.PrevLogIndex
			for rf.Log[tmp].Term == rf.Log[args.PrevLogIndex].Term {
				tmp--
			}
			reply.NewNextIndex = tmp + 1 // >= 1
		}
		EPrintf("reply.NewNextIndex = %v", reply.NewNextIndex)
		return
	}

	// In cases below, prev is matched
	reply.Success = true
	reply.Term = rf.CurrentTerm

	// 3. If an existing entry conflicts with a new one
	// (same index but different terms), delete the existing
	// entry and all that follow it
	// 4. Append any new entries not already in the log
	newL := args.PrevLogIndex + 1 + len(args.Entries)
	if len(rf.Log) < newL {
		for len(rf.Log) < newL {
			rf.Log = append(rf.Log, LogEntry{})
		}
		for i, entry := range args.Entries {
			rf.Log[args.PrevLogIndex+1+i] = entry
		}
	} else {
		conflict := false
		fromIndex := 0
		for i, entry := range args.Entries {
			if rf.Log[args.PrevLogIndex+1+i] != entry {
				conflict = true
				fromIndex = i
				break
			}
		}
		if conflict {
			rf.Log = rf.Log[:newL]
			for i := fromIndex; i < len(args.Entries); i++ {
				rf.Log[args.PrevLogIndex+1+i] = args.Entries[i]
			}
		}
	}
	rf.persist()

	// 5. If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	DPrintf("Server %v ci %v in handle AE from %v, args.LeaderCommit = %v, rf.CommitIndex = %v", rf.me, rf.CommitIndex, args.LeaderId, args.LeaderCommit, rf.CommitIndex)
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
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
	ok := rf.peers[server].Call("Raft.HandleRequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	rf.lastSendAE[server] = time.Now()
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Server %v ci %v [%v] Start[%v]", rf.me, rf.CommitIndex, rf.State, command)
	isLeader = rf.State == RaftState(LEADER)
	if !isLeader {
		return index, term, isLeader
	}

	// is leader

	index = len(rf.Log) + rf.notHandled
	term = rf.CurrentTerm

	rf.notHandled++
	rf.notHandledLogEntries = append(rf.notHandledLogEntries, LogEntry{
		Term:    term,
		Command: command,
	})

	return index, term, isLeader
}

func (rf *Raft) handleStarts() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.State == RaftState(LEADER) &&
			rf.notHandled > 0 &&
			(rf.notHandled >= 20 || time.Since(rf.lastHandle) > time.Duration(40*time.Millisecond)) {
			rf.Log = append(rf.Log, rf.notHandledLogEntries...)
			rf.persist()
			rf.MatchIndex[rf.me] = len(rf.Log) - 1

			go rf.broadcastAE()
			rf.notHandled = 0
			rf.lastHandle = time.Now()
			rf.notHandledLogEntries = []LogEntry{}
		}
		rf.mu.Unlock()

		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) broadcastAE() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAE_one(i, false)
	}
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		isNotLeader := rf.State != RaftState(LEADER)
		tooLong := time.Since(rf.LastWakeup) > rf.RandomElectionTimeout

		if isNotLeader && tooLong {
			// Election timeout!
			// Start an election
			DPrintf("Server %v ci %v [%v -> candidate] due to election timeout", rf.me, rf.CommitIndex, rf.State)
			rf.State = RaftState(CANDIDATE)

			go rf.StartElection()
		}
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) updateCommit() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.State == RaftState(LEADER) {
			l := len(rf.peers)
			matchIndexes := make([]int, l)
			copy(matchIndexes, rf.MatchIndex)
			// DPrintf("%v", rf.MatchIndex)
			sort.Ints(matchIndexes)
			if rf.Log[matchIndexes[l>>1]].Term == rf.CurrentTerm && rf.CommitIndex < matchIndexes[l>>1] {
				DPrintf("Server %v updated commitIndex from %v to %v", rf.me, rf.CommitIndex, matchIndexes[l>>1])
				rf.CommitIndex = matchIndexes[l>>1]
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) updateApply() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.CommitIndex > rf.LastApplied {
			// entries := append([]LogEntry{}, rf.Log[rf.LastApplied+1:rf.CommitIndex+1]...)
			entries := append([]LogEntry{}, rf.Log...)

			for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      entries[i].Command,
					CommandIndex: i,
				}
				DPrintf("Server %v ci %v [%v] term %v apply %v", rf.me, rf.CommitIndex, rf.State, rf.CurrentTerm, msg)
				rf.ApplyCh <- msg

				rf.LastApplied = i
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	DPrintf("Server %v ci %v [%v] term %v++ started election", rf.me, rf.CommitIndex, rf.State, rf.CurrentTerm)

	rf.CurrentTerm++

	// DPrintf("here")
	// Vote for self
	rf.VotedFor = rf.me
	rf.persist()

	// Reset election timer
	rf.wake()

	// Send RequestVote RPCs to all other servers
	total := len(rf.peers)
	finished := 1
	granted := 1
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	oldCurrentTerm := rf.CurrentTerm
	oldLastLogIndex := len(rf.Log) - 1
	oldLastLogTerm := rf.Log[len(rf.Log)-1].Term
	rf.mu.Unlock()

	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(i int) {
			args := RequestVoteArgs{
				Term:         oldCurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: oldLastLogIndex,
				LastLogTerm:  oldLastLogTerm,
			}
			reply := RequestVoteReply{}
			rf.mu.Lock()
			DPrintf("Server %v ci %v [%v] term %v sending RV to %v", rf.me, rf.CommitIndex, rf.State, rf.CurrentTerm, i)
			rf.mu.Unlock()
			network := rf.sendRequestVote(i, &args, &reply)

			mu.Lock()
			finished++
			if !network {
				rf.mu.Lock()
				DPrintf("Server %v ci %v [%v] term %v lost %v when reqvote", rf.me, rf.CommitIndex, rf.State, rf.CurrentTerm, i)
				rf.mu.Unlock()
				cond.Broadcast()
				mu.Unlock()
				return
			}
			if reply.VoteGranted {
				granted++
			}
			cond.Broadcast()
			mu.Unlock()

			rf.mu.Lock()
			if reply.Term > rf.CurrentTerm {
				DPrintf("Server %v ci %v [%v -> follower] due to RV reply", rf.me, rf.CommitIndex, rf.State)
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1
				rf.persist()
				rf.State = RaftState(FOLLOWER)
			}
			rf.mu.Unlock()

		}(index)
	}

	// Waiting for others to respond to RequestVote
	mu.Lock()
	for granted*2 < total && finished != total {
		cond.Wait()
	}
	rf.mu.Lock()
	DPrintf("Server %v ci %v [%v] term %v election finished [%v/%v]", rf.me, rf.CommitIndex, rf.State, rf.CurrentTerm, granted, finished)
	// rf.mu.Unlock()
	if granted*2 > total && rf.CurrentTerm == oldCurrentTerm {
		// rf.mu.Lock()

		// win the election
		DPrintf("Server %v ci %v [%v -> leader] term %v win the election [granted=%v, total=%v, term=%v]", rf.me, rf.CommitIndex, rf.State, rf.CurrentTerm, granted, total, rf.CurrentTerm)
		rf.State = RaftState(LEADER)
		// Initialize extreme volatile state
		rf.notHandled = 0
		rf.lastHandle = time.Now()
		rf.notHandledLogEntries = []LogEntry{}

		for i := range rf.peers {
			rf.lastSendAE[i] = time.Now().Add(time.Duration(-1) * time.Minute)
		}
		// Initialize volatile state
		for index := range rf.peers {
			if index == rf.me {
				rf.MatchIndex[index] = len(rf.Log) - 1
				continue
			}

			rf.NextIndex[index] = len(rf.Log) // leader last log index + 1
			rf.MatchIndex[index] = 0
		}

		DPrintf("Server %v ci %v [%v] term %v: start hearbeat()", rf.me, rf.CommitIndex, rf.State, rf.CurrentTerm)
		// rf.mu.Unlock()

		// Send initial empty AppendEntries RPCs(heartbeat) to each server
		// and repeat idle periods to prevent election timeouts
		go rf.heartbeat()
	}
	mu.Unlock()
	rf.mu.Unlock()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.ApplyCh = applyCh

	rand.Seed(time.Now().UnixNano())

	rf.State = RaftState(FOLLOWER)
	DPrintf("Server %v ci %v [-> %v]", rf.me, rf.CommitIndex, rf.State)

	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = []LogEntry{
		{
			Term: 0,
		},
	}

	rf.CommitIndex = 0
	rf.LastApplied = 0

	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))

	// if rf.me != 0 {
	rf.wake()
	// }

	rf.notHandledLogEntries = []LogEntry{}
	rf.notHandled = 0
	rf.lastHandle = time.Now()

	rf.lastSendAE = make([]time.Time, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	DPrintf("Make Server %v ci %v [%v] term %v, #peers=%v", rf.me, rf.CommitIndex, rf.State, rf.CurrentTerm, len(peers))
	DPrintf("Server %v ci %v [%v] term %v started", rf.me, rf.CommitIndex, rf.State, rf.CurrentTerm)
	go rf.ticker()
	go rf.handleStarts()
	go rf.updateCommit()
	go rf.updateApply()
	go rf.informOthers()

	return rf
}

func (rf *Raft) wake() {
	rf.LastWakeup = time.Now()
	r := float32(rand.Intn(361) + 180) // timeout [400ms, 800ms]
	rf.RandomElectionTimeout = time.Duration(time.Duration(r) * time.Millisecond)
}
