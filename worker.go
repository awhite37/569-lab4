package main

import (
	"time"
	"fmt"
	"sync"
	"os"
	"./mr"
	"./rf"
	"io/ioutil"
	"encoding/json"
	"sort"
)

//time to wait before declaring a node failed
const t_fail = 1000 * time.Millisecond


type Worker struct {
	id				  int
	workers       []*Worker
	table         []*TableEntry
	tableInput    chan []*TableEntry
	workRequests  chan int
	workCompleted chan int
	checkCompleted chan int
	newCommits 		chan int
	logCh				chan *Task
	redoMap		  chan *MapTask
	redoReduce    chan *ReduceTask
	killCh		  chan int
	isLeader      bool	  
	rfWorker		  *rf.Worker
}

type TableEntry struct {
	id 	int
	hb 	int
	t  	int
	mux 	sync.RWMutex
}

type Task struct {
	m 	 	*MapTask
	r 		*ReduceTask
}
type MapTask struct {
	id 	int
	mapf (func(string, string) []mr.KeyVal)
	chunk *os.File
}

type ReduceTask struct {
	id 	int
	reducef (func(string, []string) string)
}

func (worker *Worker) runMaster(maps []*MapTask, reduces []*ReduceTask, restart bool) {
	fmt.Printf("launching master node\n")
	if !restart{ //launch only on init
		go worker.appendLog()
	}
	//make deep copies of task lists for manipulating
	mapTasks := cpyMap(maps)
	reduceTasks := cpyReduce(reduces)
	for len(worker.newCommits) > 0 {
		//wait for processing commits to finish
	}
	//clear work completed channel
	for len(worker.workCompleted) > 0 {
		<- worker.workCompleted
	}
	//init from saved state if it exists
	if len(worker.rfWorker.Log) > 0 {
		fmt.Printf("\ninitializing tasks from log\n")
		for _, command := range(worker.rfWorker.Log) {
			worker.workCompleted <- 1
			task := command.Command
			if task.(*Task).m != nil {
				//delete redundant map tasks
				id := task.(*Task).m.id
				if id+1 >= len(mapTasks) {
					mapTasks = []*MapTask{}
				} else {
					mapTasks = append(mapTasks[:id], mapTasks[id+1:]...)
				}
			} else {
				//delete redundant reduce tasks
				id := task.(*Task).r.id
				if id+1 >= len(mapTasks) {
					reduceTasks = []*ReduceTask{}
				} else {
					reduceTasks = append(reduceTasks[:id], reduceTasks[id+1:]...)
				}
			}
			fmt.Printf("skipping previously completed task\n")
		}
	}
	//read work requests from workers and assign work to them
	//first run all map tasks
	for len(worker.workCompleted) < M {
		if len(worker.killCh) > 0 {
			<- worker.killCh
			//failure simulation, end this task
			return
		}
		//add uncompleted tasks back to list
		if len(worker.redoMap) > 0 {
			task := <- worker.redoMap
			mapTasks = append(mapTasks, task)
		}
		if len(mapTasks) == 0 {
			//wait for tasks to complete
			continue
		}
		requestID := <- worker.workRequests
		//pop first task
		task := mapTasks[0]
		mapTasks = mapTasks[1:]
		go worker.assignMap(requestID, task)
	}
	//run all reduce tasks
	for len(worker.workCompleted) < M+R {
		if len(worker.killCh) > 0 {
			<- worker.killCh
			//failure simulation, end this task
			return
		}
		//add uncompleted tasks back to list
		if len(worker.redoReduce) > 0 {
			task := <- worker.redoReduce
			reduceTasks = append(reduceTasks, task)
		}
		if len(reduceTasks) == 0 {
			//wait for tasks to complete
			continue
		}
		requestID := <- worker.workRequests
		//pop first task
		task := reduceTasks[0]
		reduceTasks = reduceTasks[1:]
		go worker.assignReduce(requestID, task)
	}
}

func (worker *Worker) appendLog() {
	for {
		task := <- worker.logCh
		//save completed task to log
		worker.rfWorker.Start(task)
		//wait for successful commit of log entry
		<-worker.rfWorker.ApplyCh
		<- worker.newCommits
	}
}


func (worker *Worker) run() {
	for {
		//send this workers ID to the master to request a task
		leader := worker.findLeader()
		leader.workRequests <- worker.id
		for len(worker.checkCompleted) == 0 {
			//wait for work to finish
		}
		<- worker.checkCompleted
	}
}

func (worker *Worker) assignMap(requestID int, task *MapTask) {
	go worker.workers[requestID].doMap(task)
	//wait for work completed signal
	select {
		case <-worker.workers[requestID].workCompleted:
			break
		case <-time.After(time.Duration(t_fail)):
			fmt.Printf("\nfailure detected in worker node %d, restarting map task\n",requestID)
			worker.redoMap <- task
			return
	}
	//save completed task to log
   worker.logCh <- &Task{m:task, r:nil}
   worker.newCommits <- 1
	worker.workCompleted <- 1
}

func (worker *Worker) assignReduce(requestID int, task *ReduceTask) {
	go worker.workers[requestID].doReduce(task)
	//wait for work completed signal
	select {
		case <-worker.workers[requestID].workCompleted:
			break
		case <-time.After(time.Duration(t_fail)):
			fmt.Printf("\nfailure detected in worker node %d, restarting reduce task\n",requestID)
			worker.redoReduce <- task
			return
	}
	//save completed task to log
	worker.logCh <- &Task{m:nil, r:task}
	worker.newCommits <- 1
	worker.workCompleted <- 1
}

func (worker *Worker) doMap(task *MapTask) {
	if (worker.id == 5) {
		//simulate failure of node 5, should re-assign task
		return
	}
	chunkFileContent := safeRead(task.chunk.Name())
	kva := task.mapf(task.chunk.Name(),chunkFileContent)
	m := make(map[int][]mr.KeyVal, R)
	for _, kv := range kva {
		partitionNum := hash(kv.Key) % R
		m[partitionNum] = append(m[partitionNum], kv) 
	}
	files := make([]*os.File, R)
	for i := 0; i < R; i++ {
		tempname := fmt.Sprintf("temp-mr-%03d-%03d", task.id, i)
		files[i], _ = ioutil.TempFile("./intermediate_files", tempname)
		enc := json.NewEncoder(files[i])
		for _, kv := range m[i] {
			enc.Encode(&kv)
		}
	}
	//rename files once work is completed
	for i := 0; i < R; i++ {
		filename := fmt.Sprintf("./intermediate_files/mr-%03d-%03d", task.id, i)
		os.Rename(files[i].Name(), filename)
		os.Remove(files[i].Name())
	}
	worker.workCompleted <- 1
	worker.checkCompleted <- 1
}

func (worker *Worker) doReduce(task *ReduceTask) {
	if (worker.id == 3) {
		//simulate failure of node 3, should re-assign task
		return
	}
	oname := fmt.Sprintf("./output_files/mr-out-%03d", task.id)
	ofile, _ := os.Create(oname)

	kva := []mr.KeyVal{}
	for i := 0; i < M; i++ {
		filename := fmt.Sprintf("./intermediate_files/mr-%03d-%03d", i, task.id)
		file := safeOpen(filename, "r")
		dec := json.NewDecoder(file)
		for {
			var kv mr.KeyVal
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(mr.KeyVals(kva))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Val)
		}
		output := task.reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()
	worker.workCompleted <- 1
	worker.checkCompleted <- 1
}

func (worker *Worker) findLeader() (*Worker) {
	var leader *Worker
	leader = nil
	for leader == nil {
		for _, peer := range(worker.workers) {
			if peer.isLeader {
				leader = peer
				break
			}
		}
	}
	return leader
}

func cpyMap(mapTasks []*MapTask) ([]*MapTask){
	new := []*MapTask{}
	for _, task := range(mapTasks) {
		new = append(new, &MapTask{id:task.id, mapf:task.mapf, chunk:task.chunk})
	}
	return new
}

func cpyReduce(reduceTasks []*ReduceTask) ([]*ReduceTask){
	new := []*ReduceTask{}
	for _, task := range(reduceTasks) {
		new = append(new, &ReduceTask{id:task.id, reducef:task.reducef})
	}
	return new
}
