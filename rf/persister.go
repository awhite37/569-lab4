package rf

import "sync"

type Persister struct {
	currentTerm int
	votedFor    int
	Log         []*LogEntry
	Mux 			sync.RWMutex
}

func InitPersister() (*Persister) {
	return &Persister {currentTerm: 0, votedFor: -1, Log: []*LogEntry{}}
}


func (worker *Worker) saveTerm(){
	worker.Mux.RLock() 
	worker.persister.Mux.Lock()
	worker.persister.currentTerm = worker.term
	worker.persister.Mux.Unlock()
	worker.Mux.RUnlock()
}

func (worker *Worker) saveVotedFor(){
	worker.Mux.RLock() 
	worker.persister.Mux.Lock() 
	worker.persister.votedFor = worker.votedFor
	worker.persister.Mux.Unlock()
	worker.Mux.RUnlock()
}

func (worker *Worker) saveLog(entry *LogEntry) {
	worker.Mux.RLock() 
	worker.persister.Mux.Lock() 
	worker.persister.Log = append(worker.persister.Log, 
		&LogEntry{index:entry.index, term:entry.term, Command:entry.Command})
	worker.persister.Mux.Unlock()
	worker.Mux.RUnlock()
}