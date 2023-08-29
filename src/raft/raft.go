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

	commitChan chan int
	applyChan  chan ApplyMsg
	nextIndex  []int
	matchIndex []int
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

	return w.Bytes()
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	//e.Encode(rf.l)
	//e.Encode(rf.lastIncludedIndex)
	rf.persister.Save(rf.getPersistState(), rf.getSnapshot())
}

// restore previously persisted state.
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voted int
	var logs []Log

	if d.Decode(&term) != nil || d.Decode(&voted) != nil || d.Decode(&logs) != nil {
		fmt.Errorf("decode error")
	} else {
		rf.currentTerm = term
		rf.voteFor = voted
		rf.logs = logs
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) getSnapshot() []byte {
	return nil
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
		//rf.resetElectionTimer()
	}

	if args.LastLogTerm < rf.logs[len(rf.logs)-1].Term ||
		args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < rf.logs[len(rf.logs)-1].Index {
		DPrintf("[%v]: refuse %v for log index", rf.me, args.CandidateID)
		return
	}

	if rf.voteFor == -1 || rf.voteFor == args.CandidateID {
		rf.voteFor = args.CandidateID
		reply.VoteGranted = true
		DPrintf("[%v]: vote for %v, Term %v", rf.me, args.CandidateID, args.Term)
		rf.persist()
		rf.resetElectionTimer()
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
		rf.startAppendEntries()
	}

	return index, term, rf.currenState == LEADER
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
	DPrintf("[%v]: Term %v appendEntries from %v, args %v", rf.me, rf.currentTerm, args.LeaderID, args)
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
	if len(rf.logs) <= args.PrevLogIndex {
		DPrintf("[%v]: append Error for reason 1, my lastlogs %v, index %v", rf.me, rf.logs[len(rf.logs)-1], args.PrevLogIndex)

		reply.XLen = len(rf.logs)
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.logs[args.PrevLogIndex].Term
		reply.XIndex = args.PrevLogIndex
		for index := args.PrevLogIndex - 1; index >= 0; index-- {
			if rf.logs[args.PrevLogIndex].Term != rf.logs[index].Term {
				reply.XIndex = index
				break
			}
		}
		return
	}

	reply.Success = true
	//如果一个outdate消息，可能会导致entries被trune，不是最新的，这边有问题
	//if len(args.Entries) != 0 && ((len(rf.logs) > args.Entries[len(args.Entries)-1].Index && rf.logs[args.Entries[len(args.Entries)-1].Index].Term != args.Entries[len(args.Entries)-1].Term) || (len(rf.logs) <= args.Entries[len(args.Entries)-1].Index)) {
	//	rf.logs = rf.logs[:args.Entries[0].Index]
	//	rf.logs = append(rf.logs, args.Entries...)
	//	rf.persist()
	//}

	//if len(args.Entries) != 0 && !(len(rf.logs) > args.Entries[len(args.Entries)-1].Index && rf.logs[args.Entries[len(args.Entries)-1].Index].Term == args.Entries[len(args.Entries)-1].Term) {
	//	rf.logs = rf.logs[:args.Entries[0].Index]
	//	rf.logs = append(rf.logs, args.Entries...)
	//	rf.persist()
	//}

	for i, entry := range args.Entries {
		if entry.Index <= rf.logs[len(rf.logs)-1].Index && rf.logs[entry.Index].Term != entry.Term {
			rf.logs = rf.logs[:entry.Index]
			rf.persist()
		}

		if len(rf.logs) <= 1 || entry.Index > rf.logs[len(rf.logs)-1].Index {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	//if len(args.Entries) != 0 {
	//	rf.logs = append(rf.logs, args.Entries...)
	//	rf.persist()
	//}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		//for i := rf.commitIndex + 1; i <= args.LeaderCommit; i++ {
		//	DPrintf("[%v]: follower start to appendEntries, Command %v", rf.me, rf.logs[i])
		//	rf.commitIndex = i
		//
		//	msg := ApplyMsg{
		//		Command:      rf.logs[i].Command,
		//		CommandValid: true,
		//		CommandIndex: i,
		//	}
		//	rf.applyChan <- msg
		//	rf.lastApplied = i
		//}
		rf.commitChan <- args.LeaderCommit
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startAppendEntries() {
	DPrintf("[%v]: start appendEntries in Term %v", rf.me, rf.currentTerm)
	once := &sync.Once{}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.logs[rf.matchIndex[i]].Index,
				PrevLogTerm:  rf.logs[rf.matchIndex[i]].Term,
				Entries:      rf.logs[rf.nextIndex[i]:],
				LeaderCommit: rf.commitIndex,
			}

			go func(i int, arg *AppendEntriesArgs) {

				var reply AppendEntriesReply
				ok := rf.sendAppendEntries(i, arg, &reply)

				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						rf.setNewTerm(reply.Term)
						rf.persist()
						return
					}

					if args.Term == rf.currentTerm && rf.currenState == LEADER {
						if reply.Success {
							DPrintf("[%v]: appendEntries success from %v", rf.me, i)
							if len(args.Entries) == 0 {
								rf.matchIndex[i] = args.PrevLogIndex
								rf.nextIndex[i] = args.PrevLogIndex + 1
							} else {
								rf.matchIndex[i] = args.Entries[len(args.Entries)-1].Index
								rf.nextIndex[i] = args.Entries[len(args.Entries)-1].Index + 1
							}
							cnt := 0
							for j := 0; j < len(rf.peers); j++ {
								if rf.matchIndex[j] > rf.commitIndex {
									cnt++
								}
							}

							if cnt >= len(rf.peers)/2 {
								once.Do(func() {

									DPrintf("[%v]: leader start to commit to applychan, log %v", rf.me, rf.logs[rf.commitIndex+1])
									rf.commitIndex++
									rf.commitChan <- rf.commitIndex
									//msg := ApplyMsg{
									//	Command:      rf.logs[rf.commitIndex].Command,
									//	CommandValid: true,
									//	CommandIndex: rf.commitIndex,
									//}
									//rf.applyChan <- msg
									//rf.lastApplied = rf.commitIndex
								})
							}
						} else {
							if reply.XTerm != -1 {

								found := false
								DPrintf("[%v]: conflict 2 %v", rf.me, reply)
								for index := args.PrevLogIndex; index >= 0; index-- {
									if rf.logs[index].Term == reply.XTerm {
										rf.nextIndex[i] = index
										found = true
										if index > 0 {
											rf.matchIndex[i] = index - 1
										}
										break
									} else if rf.logs[index].Term < reply.XTerm {
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

			}(i, &args)
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
								DPrintf("[%v]: Term %v become the leader", rf.me, rf.currentTerm)
							})
						}
					} else if reply.Term > rf.currentTerm {
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

		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		time.Sleep(heartBeat)
	}
}

func (rf *Raft) applier() {
	for commit := range rf.commitChan {
		for i := rf.lastApplied + 1; i <= commit; i++ {
			DPrintf("[%v]: follower start to appendEntries, Command %v", rf.me, rf.logs[i])
			rf.commitIndex = i

			msg := ApplyMsg{
				Command:      rf.logs[i].Command,
				CommandValid: true,
				CommandIndex: i,
			}
			rf.applyChan <- msg
			rf.lastApplied = i
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
	rf.commitChan = make(chan int, 100)
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
	rf.readPersist(persister.ReadRaftState())

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	//rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
