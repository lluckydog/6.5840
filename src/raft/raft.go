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
	"6.5840/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type STATE int

const (
	LEADER STATE = iota
	CANDIATE
	FOLLOWER
)

const heartBeat = 50 * time.Millisecond

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int
	logs        []Log
	commitIndex int
	lastApplied int
	currenState STATE
	electTime   time.Time

	applyChan  chan ApplyMsg
	nextIndex  []int
	matchIndex []int

	lastIncludedTerm  int
	lastIncludedIndex int
}

type Log struct {
	Index   int
	Term    int
	Command interface{}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A).
	return rf.currentTerm, rf.currenState == LEADER
}

func (rf *Raft) setNewTerm(term int) {
	rf.currentTerm = term
	rf.currenState = FOLLOWER
	rf.voteFor = -1
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) getPersistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	return w.Bytes()
}

func (rf *Raft) persist() {

	rf.persister.Save(rf.getPersistState(), rf.getSnapshot())
}

func (rf *Raft) trimLog(index int, term int) {

	logs := make([]Log, 0)
	logs = append(logs,
		Log{
			Index: index,
			Term:  term,
		})
	base := rf.logs[0].Index

	if base < index && len(rf.logs)+base >= index {
		if rf.logs[index-base].Term == term {
			logs = append(logs, rf.logs[index-base+1:]...)
		}
	}
	rf.logs = logs
	DPrintf("[%v]: trimlog, current logs %v", rf.me, rf.logs)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voted int
	var lastIncludedTerm int
	var lastIncludedIndex int
	var logs []Log

	if d.Decode(&term) != nil || d.Decode(&voted) != nil || d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		fmt.Errorf("decode error")
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.voteFor = voted
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex
		DPrintf("[%v]: readsnapshot update to lastapplied %v", rf.me, rf.lastApplied)
		rf.mu.Unlock()
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v]: start to install snapshot, args %v", rf.me, args)

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		DPrintf("[%v]: refuse install snapshot for term, mine Term %v, his term %v", rf.me, rf.currentTerm, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		reply.Term = rf.currentTerm
	}
	rf.resetElectionTimer()

	if args.LastIncludedIndex > rf.lastIncludedIndex {
		DPrintf("[%v]: start install snapshot", rf.me)
		base := rf.logs[0].Index
		if len(rf.logs)+base <= args.LastIncludedIndex || rf.logs[args.LastIncludedIndex-base].Term != args.LastIncludedTerm {

			rf.logs = []Log{
				Log{
					Term:  args.LastIncludedTerm,
					Index: args.LastIncludedIndex,
				},
			}
			DPrintf("[%v]: installsnapshot in cause 1, logs %v", rf.me, rf.logs)
		} else {
			rf.logs = rf.logs[args.LastIncludedIndex-base:]
			DPrintf("[%v]: installsnapshot in cause 2, logs %v", rf.me, rf.logs)
		}
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		if args.LastIncludedIndex > rf.commitIndex {
			rf.commitIndex = args.LastIncludedIndex
		}

		//rf.persist()
		rf.persister.Save(rf.getPersistState(), args.Data)
		msg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.applyChan <- msg
		if args.LastIncludedIndex > rf.lastApplied {
			rf.lastApplied = args.LastIncludedIndex
			DPrintf("[%v]: update to applied in install snapshot, applied %v", rf.me, rf.lastApplied)
		}
	} else {
		DPrintf("[%v]: refuse install snapshot, my lastIncludedIndex %v, args Index %v", rf.me, rf.lastIncludedIndex, args.LastIncludedIndex)
	}

}

func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs, once *sync.Once) bool {
	if rf.killed() {
		return false
	}
	var reply InstallSnapshotReply
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.setNewTerm(reply.Term)
			return ok
		} else {
			DPrintf("[%v]: update next index & update index for %v", rf.me, server)
			rf.matchIndex[server] = args.LastIncludedIndex
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}

		base := rf.logs[0].Index
		for N := rf.logs[len(rf.logs)-1].Index; N > rf.commitIndex && rf.logs[N-base].Term == rf.currentTerm; N-- {
			cnt := 0
			for j := 0; j < len(rf.peers); j++ {
				if j != rf.me && rf.matchIndex[j] >= N {
					cnt++
				}
			}
			if cnt >= len(rf.peers)/2 {
				once.Do(func() {
					//DPrintf("[%v]: leader start to commit to applychan, log %v", rf.me, rf.logs[rf.commitIndex+1])
					rf.commitIndex = N
					//rf.commitChan <- rf.commitIndex
					go rf.applier()
				})
				break
			}
		}
	}
	return ok
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) getSnapshot() []byte {
	//DPrintf("[%v]: Get Snapshot", rf.me)
	return rf.persister.ReadSnapshot()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	base := rf.logs[0].Index

	if index < base || (base+len(rf.logs) <= index) || index <= rf.lastIncludedIndex || index > rf.commitIndex {
		DPrintf("[%v]: snapshot Error", rf.me)
		return
	}

	stateData := rf.getPersistState()
	rf.persister.Save(stateData, snapshot)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.logs[index-base].Term

	if index > rf.commitIndex {
		rf.commitIndex = index
	}

	rf.logs = append([]Log{
		Log{
			Term:    rf.logs[index-base].Term,
			Index:   index,
			Command: rf.logs[index-base].Command,
		},
	},
		rf.logs[index-base+1:]...)
	//rf.persist()
	rf.persister.Save(rf.getPersistState(), snapshot)

	if index > rf.lastApplied {
		rf.lastApplied = index
		DPrintf("[%v]: snapshot update to lastapplied %v", rf.me, rf.lastApplied)
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v]: receive from %v, Mine Term %v, his Term %v, args %v, my last logs %v", rf.me, args.CandidateID, rf.currentTerm, args.Term, args, rf.logs[len(rf.logs)-1])
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		return
	}

	if rf.currentTerm < args.Term {
		rf.setNewTerm(args.Term)
		reply.Term = rf.currentTerm
		rf.persist()
	}

	if args.LastLogTerm < rf.logs[len(rf.logs)-1].Term ||
		args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < rf.logs[len(rf.logs)-1].Index {
		DPrintf("[%v]: refuse %v for log index", rf.me, args.CandidateID)
		return
	}

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateID) && args.Term == reply.Term {
		rf.voteFor = args.CandidateID
		reply.VoteGranted = true
		DPrintf("[%v]: vote for %v, Term %v", rf.me, args.CandidateID, args.Term)
		rf.resetElectionTimer()
		rf.persist()
	}

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return -1, -1, false
	}
	if rf.currenState != LEADER {
		return -1, -1, false
	}

	DPrintf("[%v]: start command", rf.me)
	index := rf.logs[len(rf.logs)-1].Index + 1
	term := rf.currentTerm
	isLeader := rf.currenState == LEADER

	if isLeader {
		entry := Log{
			Term:    term,
			Index:   index,
			Command: command,
		}

		rf.logs = append(rf.logs, entry)
		rf.persist()
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.electTime = t.Add(electionTimeout)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v]: Term %v appendEntries from %v, args %v, my logs %v", rf.me, rf.currentTerm, args.LeaderID, args, rf.logs)
	reply.Success = false
	reply.Term = rf.currentTerm
	reply.XTerm = -1

	if rf.currentTerm > args.Term {
		DPrintf("[%v]: Term %v appendEntries from %v fail", rf.me, rf.currentTerm, args.LeaderID)

		return
	}

	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		reply.Term = rf.currentTerm
		rf.persist()
		//return
	}

	if rf.currenState == CANDIATE {
		rf.currenState = FOLLOWER
	}
	rf.resetElectionTimer()
	base := rf.logs[0].Index

	if rf.lastIncludedIndex > args.PrevLogIndex {
		reply.XLen = len(rf.logs) + base
		return
	}

	if base+len(rf.logs) <= args.PrevLogIndex {
		DPrintf("[%v]: append Error for reason 1, my lastlogs %v, index %v", rf.me, rf.logs[len(rf.logs)-1], args.PrevLogIndex)

		reply.XLen = len(rf.logs) + base
		return
	}

	if rf.logs[args.PrevLogIndex-base].Term != args.PrevLogTerm {
		reply.XTerm = rf.logs[args.PrevLogIndex-base].Term
		reply.XIndex = args.PrevLogIndex
		for index := args.PrevLogIndex - 1; index >= base; index-- {
			if rf.logs[args.PrevLogIndex-base].Term != rf.logs[index-base].Term {
				reply.XIndex = index
				break
			}
		}
		return
	}

	reply.Success = true
	for i, entry := range args.Entries {
		if entry.Index <= rf.logs[len(rf.logs)-1].Index && rf.logs[entry.Index-base].Term != entry.Term {
			rf.logs = rf.logs[:entry.Index-base]
			rf.persist()
		}

		if len(rf.logs) <= 1 || entry.Index > rf.logs[len(rf.logs)-1].Index {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		//rf.commitChan <- args.LeaderCommit
		go rf.applier()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startAppendEntries() {
	DPrintf("[%v]: start appendEntries in Term %v", rf.me, rf.currentTerm)
	once := &sync.Once{}
	installOnce := &sync.Once{}
	base := rf.logs[0].Index
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			var args AppendEntriesArgs
			var args2 InstallSnapshotArgs
			isInstall := false
			if rf.matchIndex[i] >= rf.lastIncludedIndex {
				DPrintf("[%v]: matchindex %v, base %v, logs %v,", rf.me, rf.matchIndex[i], base, rf.logs)
				args = AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: rf.logs[rf.matchIndex[i]-base].Index,
					PrevLogTerm:  rf.logs[rf.matchIndex[i]-base].Term,
					Entries:      rf.logs[(rf.matchIndex[i] - base + 1):],
					LeaderCommit: rf.commitIndex,
				}
			} else {
				args2 = InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedTerm:  rf.logs[0].Term,
					LastIncludedIndex: rf.logs[0].Index,
					Data:              rf.persister.ReadSnapshot(),
				}
				isInstall = true
			}

			go func(i int, arg AppendEntriesArgs, args2 InstallSnapshotArgs, isInstall bool) {

				if isInstall {
					DPrintf("[%v]: to %v install snapshot, args %v", rf.me, i, arg)
					rf.SendInstallSnapshot(i, &args2, installOnce)
				} else {
					var reply AppendEntriesReply
					ok := rf.sendAppendEntries(i, &arg, &reply)

					if ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()

						if reply.Term > rf.currentTerm {
							rf.setNewTerm(reply.Term)
							rf.persist()
							return
						}

						b := rf.logs[0].Index
						if args.Term == rf.currentTerm && rf.currenState == LEADER {
							if reply.Success {
								//DPrintf("[%v]: appendEntries success from %v", rf.me, i)
								if len(args.Entries) > 0 {
									rf.matchIndex[i] = args.Entries[len(args.Entries)-1].Index
									rf.nextIndex[i] = args.Entries[len(args.Entries)-1].Index + 1
								}
								for N := rf.logs[len(rf.logs)-1].Index; N > rf.commitIndex && rf.logs[N-b].Term == rf.currentTerm; N-- {
									cnt := 0
									for j := 0; j < len(rf.peers); j++ {
										if j != rf.me && rf.matchIndex[j] >= N {
											cnt++
										}
									}
									if cnt >= len(rf.peers)/2 {
										once.Do(func() {
											DPrintf("[%v]: leader start to commit to applychan, log %v, base %v, commit %v", rf.me, rf.logs, base, N)
											rf.commitIndex = N
											go rf.applier()
										})
										break
									}
								}

							} else {
								if reply.XTerm != -1 {

									found := false
									DPrintf("[%v]: conflict 2 %v", rf.me, reply)
									for index := args.PrevLogIndex; index >= b; index-- {
										if rf.logs[index-b].Term == reply.XTerm {
											rf.nextIndex[i] = index
											found = true
											if index > 0 {
												rf.matchIndex[i] = index - 1
											}
											break
										} else if rf.logs[index-b].Term < reply.XTerm {
											break
										}
									}
									if !found {
										rf.nextIndex[i] = reply.XIndex
										if reply.XIndex > 0 {
											rf.matchIndex[i] = reply.XIndex - 1
										}
									}

								} else {
									DPrintf("[%v]: conflict 3 %v", rf.me, reply)

									if reply.XLen > 0 {
										rf.matchIndex[i] = reply.XLen - 1
									}

									rf.nextIndex[i] = reply.XLen
								}
							}
						}
					}

				}

			}(i, args, args2, isInstall)
		}
	}

}

func (rf *Raft) startElect() {
	rf.resetElectionTimer()
	rf.currentTerm++
	rf.currenState = CANDIATE
	rf.voteFor = rf.me
	DPrintf("[%v]: Term %v, start to elect, my last log %v", rf.me, rf.currentTerm, rf.logs[len(rf.logs)-1])
	rf.persist()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.logs[len(rf.logs)-1].Index,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	votes := 0
	once := &sync.Once{}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int, args *RequestVoteArgs, votes *int) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(i, args, &reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					DPrintf("[%v]: Term %v, receive reply from %v, reply %v", rf.me, rf.currentTerm, i, reply)
					if reply.Term == rf.currentTerm && reply.VoteGranted && rf.currenState == CANDIATE {
						DPrintf("[%v]: get Vote from %v, current vote %v", rf.me, i, *votes)
						*votes++
						if *votes >= len(rf.peers)/2 {
							once.Do(func() {
								rf.currenState = LEADER
								lastIndex := rf.logs[len(rf.logs)-1].Index
								for j := 0; j < len(rf.peers); j++ {
									rf.matchIndex[j] = lastIndex
									rf.nextIndex[j] = lastIndex + 1
								}

								DPrintf("[%v]: Term %v become the leader", rf.me, rf.currentTerm)
							})
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("[%v]: get a bigger term", rf.me)
						rf.setNewTerm(reply.Term)
						rf.persist()
					}
				}
			}(i, &args, &votes)
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		if rf.electTime.Before(time.Now()) {
			rf.startElect()
		}

		if rf.currenState == LEADER {
			rf.resetElectionTimer()
			rf.startAppendEntries()
		}
		//rf.applier()
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		time.Sleep(heartBeat)
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	base := rf.logs[0].Index
	DPrintf("[%v]: start to applier to commit %v， last applied %v, commit Index %v", rf.me, rf.logs, rf.lastApplied, rf.commitIndex)

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i = rf.lastApplied + 1 {
		msg := ApplyMsg{
			Command:      rf.logs[i-base].Command,
			CommandValid: true,
			CommandIndex: i,
		}
		rf.mu.Unlock()
		rf.applyChan <- msg
		rf.mu.Lock()
		base = rf.logs[0].Index
		if rf.lastApplied < i {
			rf.lastApplied = i
			DPrintf("[%v]: applier to last applied to %v", rf.me, rf.lastApplied)
		}
	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.currenState = FOLLOWER
	rf.resetElectionTimer()
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.applyChan = applyCh

	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = 1
	}

	rf.logs = append(rf.logs, Log{0, 0, -1})
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	//go rf.applier()

	return rf
}
