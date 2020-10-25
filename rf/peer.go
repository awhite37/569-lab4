package rf

import "math/rand"
import "time"
import "fmt"
import "sync"

//time to wait before counting votes
const Z = 50 * time.Millisecond

//time to wait before sending heartbeat
const X = 50 * time.Millisecond

//time to wait before checking if Commands can be applied
const Y = 50 * time.Millisecond

type Worker struct {
	id        int
	ApplyCh   chan ApplyMsg
	entriesCh chan EntryMsg
	peers     []*Worker

	term        int
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	IsLeader       bool
	isCandidate    bool
	votedFor       int
	voteinput      chan VoteRequest
	votes          chan Response
	appendResponse chan Response
	numSuccess     chan int
	timeout        chan int
	Mux            sync.RWMutex

	persister *Persister

	Log []*LogEntry
}

type VoteRequest struct {
	from      *Worker
	term      int
	lastTerm  int
	lastIndex int
}

type Response struct {
	term    int
	granted bool
}

type EntryMsg struct {
	term         int
	leaderID     int
	prevIndex    int
	prevTerm     int
	entries      []LogEntry
	leaderCommit int
}

type ApplyMsg struct {
}

type LogEntry struct {
	Command interface{}
	term    int
	index   int
}

func (worker *Worker) Run() {
	go worker.HB()
	go worker.respondToVotes()
	go worker.electionTimeout()
	go worker.handleMsg()
	go worker.applyEntries()
	for {
		//let worker run
	}
}

func (worker *Worker) requestVotes(term int) {
	votes := 1
	lastTerm := -1
	worker.Mux.RLock()
	index := worker.commitIndex
	worker.Mux.RUnlock()
	if index >= 0 {
		worker.Mux.RLock()
		lastTerm = worker.Log[index].term
		worker.Mux.RUnlock()
	}
	for _, peer := range worker.peers {
		peer.voteinput <- VoteRequest{
			from:      worker,
			term:      term,
			lastIndex: index,
			lastTerm:  lastTerm,
		}
	}
	time.Sleep(Z)
	//quit if accepted other leader
	worker.Mux.RLock()
	isCandidate := worker.isCandidate
	newTerm := worker.term
	id := worker.id
	worker.Mux.RUnlock()
	if !isCandidate || newTerm != term {
		return
	}
	for len(worker.votes) > 0 {
		vote := <-worker.votes
		if vote.term > term {
			worker.revert(vote.term)
			return
		}
		if vote.term == term && vote.granted {
			votes += 1
		}
	}
	//check for majority
	worker.Mux.RLock()
	term2 := worker.term
	worker.Mux.RUnlock()
	fmt.Printf("node %d got %d votes for term %d\n", id, votes, term2)
	if votes > (len(worker.peers)+1)/2 {
		fmt.Printf("node %d is leader for term %d\n", id, term2)
		worker.InitLeader()
		worker.Mux.Lock()
		worker.IsLeader = true
		worker.isCandidate = false
		worker.term = term
		worker.Mux.Unlock()
		worker.saveTerm()
		//reset election timeout
		worker.Mux.RLock()
		term2 := worker.term
		worker.Mux.RUnlock()
		for _, peer := range worker.peers {
			peer.entriesCh <- EntryMsg{entries: nil, term: term2, leaderID: id}
		}
	}
}

func (worker *Worker) electionTimeout() {
	rand.Seed(time.Now().UnixNano())
	for {
		worker.Mux.RLock()
		IsLeader := worker.IsLeader
		worker.Mux.RUnlock()
		if !IsLeader {
			//wait for leader, then become candidate
			select {
			case <-worker.timeout:
				break
			//random timeout between 150-300ms
			case <-time.After(time.Duration(rand.Intn(1500)+1501) * time.Millisecond):
				worker.Mux.RLock()
				id := worker.id
				term := worker.term
				worker.Mux.RUnlock()
				worker.Mux.Lock()
				worker.votedFor = id
				worker.isCandidate = true
				worker.term += 1
				worker.Mux.Unlock()
				worker.saveTerm()
				worker.saveVotedFor()
				fmt.Printf("node %d becoming candidate for term %d\n", id, term+1)
				worker.requestVotes(term + 1)
				break
			}
		}
	}
}

func (worker *Worker) handleMsg() {
	for {
		msg := <-worker.entriesCh
		worker.Mux.RLock()
		term := worker.term
		commitIndex := worker.commitIndex
		worker.Mux.RUnlock()
		if msg.term >= term {
			//message from leader, reset election timeout
			worker.timeout <- 1
			worker.Mux.Lock()
			worker.isCandidate = false
			worker.IsLeader = false
			worker.term = msg.term
			worker.Mux.Unlock()
			worker.saveTerm()
			//handle message
			if msg.leaderCommit > commitIndex {
				worker.Mux.Lock()
				worker.commitIndex = Min(msg.leaderCommit, len(worker.Log)-1)
				worker.Mux.Unlock()
			}
			if msg.entries != nil {
				if msg.prevIndex > -1 && len(worker.Log) <= msg.prevIndex {
						worker.appendResponse <- Response{term: term, granted: false}
						continue
					}
				if msg.prevIndex != -1 {
					worker.Mux.RLock()
					LogTerm := worker.Log[msg.prevIndex].term
					worker.Mux.RUnlock()
					if LogTerm != msg.prevTerm {
						worker.appendResponse <- Response{term: term, granted: false}
						continue
					}
					
				}
				worker.restoreLog(msg.leaderID, msg.leaderCommit, msg.prevIndex)
				worker.Mux.Lock()
				worker.Log = append(worker.Log, &msg.entries[0])
				worker.Mux.Unlock()
				worker.saveLog(&msg.entries[0])
				
				worker.appendResponse <- Response{term: term, granted: true}
			}
		} else if msg.entries != nil {
			worker.appendResponse <- Response{term: term, granted: false}
		}
	}
}

func (worker *Worker) restoreLog(leaderID int, leaderCommit int, prevIndex int) {
	var leader *Worker
	for _, peer := range(worker.peers) {
		if peer.id == leaderID {
			leader = peer
			break
		}
	}
	worker.Mux.Lock()
	worker.Log = worker.Log[:prevIndex+1]
	worker.persister.Log = worker.persister.Log[:prevIndex+1]
	worker.Mux.Unlock()
	for i := prevIndex+1; i <= leaderCommit; i++ {
		leader.Mux.RLock()
		entry := leader.Log[i]
		leader.Mux.RUnlock()  
		entryCpy := LogEntry{index:entry.index, term:entry.term, Command:entry.Command}
		worker.Mux.Lock()
		worker.Log = append(worker.Log, &entryCpy)
		worker.Mux.Unlock()
		worker.saveLog(&entryCpy)
	}
}

func (worker *Worker) respondToVotes() {
	highestVoted := 0
	for {
		vote := <-worker.voteinput
		worker.Mux.RLock()
		term := worker.term
		worker.Mux.RUnlock()
		if vote.term > term {
			worker.revert(vote.term)
		}
		worker.Mux.RLock()
		isCandidate := worker.isCandidate
		IsLeader := worker.IsLeader
		worker.Mux.RUnlock()
		if !IsLeader && !isCandidate {
			if vote.term > highestVoted {
				highestVoted = vote.term
				worker.Mux.Lock()
				worker.votedFor = -1
				worker.Mux.Unlock()
				worker.saveVotedFor()
			}
			worker.Mux.RLock()
			votedFor := worker.votedFor
			term := worker.term
			id := worker.id
			commitIndex := worker.commitIndex
			lastTerm := -1
			if len(worker.Log) < commitIndex {
				worker.Mux.RLock()
				lastTerm = worker.Log[commitIndex].term
				worker.Mux.RUnlock()
			}
			worker.Mux.RUnlock()
			if vote.term > term {
				worker.Mux.Lock()
				worker.term = vote.term
				worker.Mux.Unlock()
				worker.saveTerm()
			}
			if vote.term >= term &&
				(votedFor == -1 || votedFor == vote.from.id) &&
				vote.lastIndex >= commitIndex && vote.lastTerm >= lastTerm {
				//restart election timer
				worker.timeout <- 1
				//grant vote
				worker.Mux.Lock()
				worker.term = vote.term
				worker.votedFor = vote.from.id
				worker.Mux.Unlock()
				worker.saveTerm()
				worker.saveVotedFor()
				fmt.Printf("node %d voting for node %d on term %d\n", id, vote.from.id, vote.term)
				(vote.from).votes <- Response{term: vote.term, granted: true}
			} else {
				(vote.from).votes <- Response{term: term, granted: false}
			}
		}
	}
}

func (worker *Worker) HB() {
	for {
		time.Sleep(X)
		worker.Mux.RLock()
		IsLeader := worker.IsLeader
		worker.Mux.RUnlock()
		if IsLeader {
			worker.Mux.RLock()
			id := worker.id
			term := worker.term
			leaderCommit := worker.commitIndex
			worker.Mux.RUnlock()
			for _, peer := range worker.peers {
				peer.entriesCh <- EntryMsg{entries: nil, term: term, leaderID: id, leaderCommit: leaderCommit}
			}
		}
	}
}

func (worker *Worker) revert(term int) {
	worker.Mux.Lock()
	worker.isCandidate = false
	worker.term = term
	worker.Mux.Unlock()
	worker.saveTerm()
}

func (worker *Worker) applyEntries() {
	for {
		time.Sleep(Y)
		worker.Mux.RLock()
		commitIndex := worker.commitIndex
		lastApplied := worker.lastApplied
		worker.Mux.RUnlock()
		if commitIndex > lastApplied {
			worker.Mux.Lock()
			worker.lastApplied += 1
			worker.ApplyCh <- ApplyMsg{}
			worker.Mux.Unlock()
		}
	}

}

func (worker *Worker) InitLeader() {
	nextIndex := make([]int, len(worker.peers))
	matchIndex := make([]int, len(worker.peers))
	worker.Mux.RLock()
	index := worker.commitIndex
	worker.Mux.RUnlock()
	for i, _ := range worker.peers {
		nextIndex[i] = index + 1
		matchIndex[i] = 0
	}
	worker.Mux.Lock()
	worker.nextIndex = nextIndex
	worker.matchIndex = matchIndex
	worker.Mux.Unlock()
}

func (worker *Worker) leaderAppend(Command interface{}) {
	worker.Mux.RLock()
	currTerm := worker.term
	prevIndex := worker.commitIndex+1
	id := worker.id
	worker.Mux.RUnlock()
	entry := LogEntry{Command: Command, term: currTerm, index: prevIndex+1}
	worker.Mux.Lock()
	worker.Log = worker.Log[:prevIndex]
	worker.persister.Log = worker.persister.Log[:prevIndex]
	worker.Log = append(worker.Log, &entry)
	worker.Mux.Unlock()
	worker.saveLog(&entry)

	numSuccess := 0
	for i, peer := range worker.peers {
		worker.Mux.RLock()
		nextIndex := worker.nextIndex[i]
		worker.Mux.RUnlock()
		if (prevIndex) >= nextIndex {
			go worker.sendAppends(peer, i, currTerm, &entry, prevIndex-1)
		}
	}
	worker.Mux.RLock()
	term := (worker.Log[len(worker.Log)-1]).term
	IsLeader := worker.IsLeader
	worker.Mux.RUnlock()
	for IsLeader && term == currTerm{
		success := <-worker.numSuccess
		if success == currTerm {
			numSuccess += 1
		}
		worker.Mux.RLock()
		IsLeader = worker.IsLeader
		worker.Mux.RUnlock()
		if numSuccess > len(worker.peers)/2 {
			if IsLeader {
				worker.Mux.Lock()
				worker.commitIndex += 1
				leaderCommit := worker.commitIndex
				worker.Mux.Unlock()
				for _, peer := range worker.peers {
					peer.entriesCh <- EntryMsg{entries: nil, term: term, leaderID: id, leaderCommit: leaderCommit}
				}
			}
			break
		}
	}
}

func (worker *Worker) sendAppends(peer *Worker, i int, leaderTerm int, entry *LogEntry, leaderCommit int) {
	prevTerm := -1
	worker.Mux.RLock()
	nextIndex := worker.nextIndex[i] - 1
	id := worker.id
	worker.Mux.RUnlock()
	entries := []LogEntry{}
	entries = append(entries, *entry)
	for {
		if nextIndex != -1 {
			worker.Mux.RLock()
			prevTerm = worker.Log[nextIndex].term
			worker.Mux.RUnlock()
		} else {
			prevTerm = -1
		}
		apply := EntryMsg{
			term:         leaderTerm,
			leaderID:     id,
			prevIndex:    nextIndex,
			prevTerm:     prevTerm,
			entries:      entries,
			leaderCommit: leaderCommit,
		}
		peer.entriesCh <- apply
		response := <-peer.appendResponse
		if response.granted {
			worker.Mux.Lock()
			worker.nextIndex[i] += 1
			worker.matchIndex[i] += 1
			worker.Mux.Unlock()
			worker.numSuccess <- leaderTerm
			return
		} else if response.term > leaderTerm {
			worker.Mux.Lock()
			worker.isCandidate = false
			worker.IsLeader = false
			worker.term = response.term
			worker.Mux.Unlock()
			worker.saveTerm()
			return
		} else {
			worker.Mux.Lock()
			worker.nextIndex[i] -= 1
			worker.Mux.Unlock()
			nextIndex -= 1
		}
	}
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Make(peers []*Worker, me int, persister *Persister, ApplyCh chan ApplyMsg) (worker *Worker) {
	workers := []*Worker{}
	for _, peer := range peers {
		workers = append(workers, peer)
	}
	//copy persister Log
	Log := []*LogEntry{}
	persister.Mux.RLock()
	for _, entry := range persister.Log {
		Log = append(Log, &LogEntry{index:entry.index, term:entry.term, Command: entry.Command})
	}
	persister.Mux.RUnlock()

	new := Worker{
		id:             me,
		ApplyCh:        ApplyCh,
		entriesCh:      make(chan EntryMsg),
		peers:          workers,
		term:           persister.currentTerm,
		commitIndex:    -1,
		lastApplied:    -1,
		IsLeader:       false,
		isCandidate:    false,
		votedFor:       persister.votedFor,
		voteinput:      make(chan VoteRequest, 10),
		votes:          make(chan Response, 10),
		appendResponse: make(chan Response, 10),
		numSuccess:     make(chan int, 10),
		timeout:        make(chan int, 10),
		persister:      persister,
		Log:            Log,
	}

	for _, peer := range peers {
		peer.peers = append(peer.peers, &new)
	}
	return &new
}

func (worker *Worker) Start(Command interface{}) (int, int, bool) {
	worker.Mux.RLock()
	id := worker.id
	IsLeader := worker.IsLeader
	term := worker.term
	worker.Mux.RUnlock()
	var leader *Worker
	leader = nil
	for leader == nil {
		worker.Mux.RLock()
		IsLeader = worker.IsLeader
		worker.Mux.RUnlock()
		if IsLeader {
			leader = worker
			break
		}
		for _, peer := range worker.peers {
			peer.Mux.RLock()
			IsLeaderPeer := peer.IsLeader
			peer.Mux.RUnlock()
			if IsLeaderPeer {
				leader = peer
				break
			}
		}
	}
	go leader.leaderAppend(Command)
	return id, term, IsLeader
}

func (worker *Worker) GetState() (int, bool) {
	worker.Mux.RLock()
	term := worker.term
	IsLeader := worker.IsLeader
	worker.Mux.RUnlock()
	return term, IsLeader
}

func (worker *Worker) PrintLog() {
	worker.Mux.RLock()
	for _, entry := range(worker.Log) {
		fmt.Printf("index %d: term: %d\n", entry.index, entry.term)
	}
	worker.Mux.RUnlock()
}
