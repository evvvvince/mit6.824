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
	
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	// "fmt"
)

type Status int

const (
	Follower  Status = 0
	Leader    Status = 1
	Candidate Status = 2
)

type VoteStatus int

const (
	Normal  VoteStatus = 0
	Killed  VoteStatus = 1
	Expired VoteStatus = 2
	Voted   VoteStatus = 3
)

type AppendEntrieStatus int

const (
	AppNormal    AppendEntrieStatus = 0 // 追加正常
	AppOutOfDate                        // 追加过时
	AppKilled                           // Raft程序终止
	AppRepeat                           // 追加重复 (2B
	AppCommited                         // 追加的日志已经提交 (2B
	Mismatch                            // 追加不匹配 (2B
)

var HeartBeatTimeout = 120 * time.Millisecond

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
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int //下一个要追加的日志号
	matchIndex []int //已经复制给server的日志号

	status   		Status
	overTime 		time.Duration
	timer   	 	*time.Ticker
	HeartBeatTimer 	*time.Ticker 

	applyChan chan ApplyMsg
}

// AppendEntriesArgs 由leader复制log条目，也可以当做是心跳连接，注释中的rf为leader节点
type AppendEntriesArgs struct {
	Term         int        // leader的任期
	LeaderId     int        // leader自身的ID
	PrevLogIndex int        // 预计要从哪里追加的index，因此每次要比当前的len(logs)多1 args初始化为：rf.nextIndex[i] - 1
	PrevLogTerm  int        // 追加新的日志的任期号(这边传的应该都是当前leader的任期号 args初始化为：rf.currentTerm
	Entries      []LogEntry // 预计存储的日志（为空时就是心跳连接）
	LeaderCommit int        // leader的commit index指的是最后一个被大多数机器都复制的日志Index
}

type AppendEntriesReply struct {
	Term     int                // leader的term可能是过时的，此时收到的Term用于更新他自己
	Success  bool               //	如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
	AppState AppendEntrieStatus // 追加状态
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	//fmt.Println("the peer[", rf.me, "] state is:", rf.state)
	if rf.status == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader

}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //	需要竞选的人的任期
	CandidateId  int // 需要竞选的人的Id
	LastLogIndex int // 竞选人日志条目最后索引
	LastLogTerm  int // 候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int        // 投票方的term，如果竞选者比自己还低就改为这个
	VoteGranted bool       // 是否投票给了该竞选人
	VoteStatus  VoteStatus // 投票状态
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// fmt.Printf("server %v start RequestVote\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		reply.VoteStatus = Killed
		reply.VoteGranted = false
		reply.Term = -1
		return
	}
	
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		reply.VoteStatus = Expired
		return
	}

	currentLogIndex := len(rf.logs) - 1
	currentLogTerm := 0
	if currentLogIndex >= 0 {
		currentLogTerm = rf.logs[currentLogIndex].Term
	}
		
		//安全性检查
	if args.LastLogIndex < currentLogIndex || args.LastLogTerm < currentLogTerm {
		reply.VoteStatus = Expired
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	//出现了新的选举且该节点不是candidate 则重置为follower 更新term和vote
	if args.Term > rf.currentTerm {
		rf.status = Follower
		rf.voteFor = -1
		rf.currentTerm = args.Term
		rf.timer.Reset(rf.overTime)
	}
	
	//如果还没投票
	if rf.voteFor == -1 {
		
		
		rf.voteFor = args.CandidateId
		
		reply.VoteStatus = Normal
		reply.Term = rf.currentTerm //为什么？
		reply.VoteGranted = true
		// fmt.Printf("server %v mid RequestVote\n", rf.me)
		
		
		//fmt.Printf("[	    func-RequestVote-rf(%v)		] : voted rf[%v]\n", rf.me, rf.voteFor)
	} else { // 只剩下任期相同，但是票已经给了，此时存在两种情况

		reply.VoteStatus = Voted
		reply.VoteGranted = false

		// 1、当前的节点是来自同一轮，不同竞选者的，但是票数已经给了(又或者它本身自己就是竞选者）
		if rf.voteFor != args.CandidateId {
			// 告诉reply票已经没了返回false
			return
		} else { // 2. 当前的节点票已经给了同一个人了，但是由于sleep等网络原因，又发送了一次请求
			// 重置自身状态
			//fmt.Println("重置自身状态")
			rf.status = Follower 
		}


	}
	// fmt.Printf("server %v end RequestVote\n", rf.me)
	return

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNums *int) bool {
	// fmt.Printf("server %v start sendRequestVote\n", rf.me)
	if rf.killed() {
		// fmt.Printf("rf.me(%v) is killed\n", rf.me)
		return false
	}
	
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		// fmt.Printf("restart")
		rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	
	// 过期请求 可能是在发送rpc途中currentTerm更新
	
	rf.mu.Lock()
	// fmt.Printf("start sendRequestVote-lock\n")
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		return false
	}

	//检查reply的状态
	switch reply.VoteStatus {
	// 1. 当前节点的Term小于被发送节点的Term
	// 2. 安全性检查 即当前的日志序号不是最大
	case Expired:
		rf.status = Follower
		rf.timer.Reset(rf.overTime)
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.voteFor = -1 //为什么？ 因为进入了新的任期，所以要重置
		}

	case Normal, Voted:
		if reply.VoteGranted && rf.currentTerm == reply.Term && *voteNums < (len(rf.peers)/2) + 1  {
			*voteNums++
		}
		if *voteNums >= (len(rf.peers)/2) + 1 {
			
			*voteNums = 0
			if rf.status == Leader {
				return ok
			}
			rf.status = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs) + 1
			}
			
			//fmt.Printf("[	sendRequestVote-func-rf(%v)		] be a leader\n", rf.me)
		}

	case Killed:
		return false
	}
	return ok

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		
		
		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			switch rf.status {
			case Follower:
				rf.status = Candidate
				fallthrough
			case Candidate:
				rf.currentTerm += 1
				rf.voteFor = rf.me
				voteNums := 1
				rf.overTime = time.Duration(150+rand.Intn(200)) * time.Millisecond
				rf.timer.Reset(rf.overTime)
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					voteArgs := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.logs) - 1,
						LastLogTerm:  0,
					}
					if len(rf.logs) > 0 {
						voteArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
					}
					voteReply := RequestVoteReply{}
					
					go rf.sendRequestVote(i, &voteArgs, &voteReply, &voteNums)

				}

			}
			rf.mu.Unlock()
			
			case <-rf.HeartBeatTimer.C:
				if rf.killed() {	
					return
				}
				rf.mu.Lock()
				if rf.status == Leader {
					rf.HeartBeatTimer.Reset(HeartBeatTimeout)
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						appendEntriesArgs := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: 0,
							PrevLogTerm:  0,
							Entries:      nil,
							LeaderCommit: rf.commitIndex,
						}
						appendEntriedReply := AppendEntriesReply{}
						//fmt.Printf("[	ticker(%v) ] : send a appendEntry to %v\n", rf.me, i)
						go rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriedReply)
					}
				}
				rf.mu.Unlock()

		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
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

	// Your initialization code here (2A, 2B, 2C).

	//初始化
	rf.applyChan = applyCh
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.logs = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))

	rf.status = Follower
	rf.overTime = time.Duration(150+rand.Intn(200)) * time.Millisecond //随机150-350ms
	rf.timer = time.NewTicker(rf.overTime)
	rf.HeartBeatTimer = time.NewTicker(HeartBeatTimeout)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	// start ticker goroutine to start elections
	//fmt.Printf("[ 	Make-func-rf(%v) 	]:  %v\n", rf.me, rf.overTime)
	go rf.ticker()
	return rf
}
