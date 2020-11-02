package main

import (
	"./mr"
	"./rf"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"time"
)

//time to wait before sending heartbeat to master
const X = 50 * time.Millisecond
//time to wait before declaring a node failed
const t_fail = 300 * time.Millisecond

type Worker struct {
	id             int
	workers        []*Worker
	numMapTasks    int 
	restart 			bool
	workRequests   chan int
	workCompleted  chan int
	checkCompleted chan int
	newCommits     chan int
	logCh          chan *Task
	redoMap        chan *MapTask
	redoReduce     chan *ReduceTask
	killCh         chan int
	heartbeat      chan int
	isLeader       bool
	rfWorker       *rf.Worker
}

type TableEntry struct {
	id  int
	hb  int
	t   int
	mux sync.RWMutex
}

type Task struct {
	m *MapTask
	r *ReduceTask
}
type MapTask struct {
	id    int
	mapf  (func(string, string) []mr.KeyVal)
	inputFilePath string
	chunkOffset int64
}

type ReduceTask struct {
	id      int
	reducef (func(string, []string) string)
}

func (worker *Worker) runMaster(mapTasks []*MapTask, reduceTasks []*ReduceTask) {
	fmt.Printf("launching master node\n")
	M := worker.numMapTasks
	if !worker.restart { //launch only on init
		go worker.appendLog()
	} else {
		for len(worker.newCommits) > 0 {
			//wait for processing commits to finish
		}
		//clear out old channel input
		for len(worker.redoMap) > 0 {
			<-worker.redoMap
		}
		for len(worker.redoReduce) > 0 {
			<-worker.redoReduce
		}
		for len(worker.workCompleted) > 0 {
			<-worker.workCompleted
		}
		//init from saved state if it exists
		if len(worker.rfWorker.Log) > 0 {
			fmt.Printf("\ninitializing tasks from log\n")
			for _, command := range worker.rfWorker.Log {
				worker.workCompleted <- 1
				task := command.Command
				if task.(*Task).m != nil {
					//delete redundant map tasks
					id := task.(*Task).m.id
					mapTasks = deleteMap(mapTasks, id)
					fmt.Printf("skipping previously completed map task %d\n", id)
				} else {
					//delete redundant reduce tasks
					id := task.(*Task).r.id
					reduceTasks = deleteReduce(reduceTasks, id)
					fmt.Printf("skipping previously completed reduce task %d\n", id)
				}
			}
		}
	}
	//read work requests from workers and assign work to them
	//first run all map tasks

	for len(worker.workCompleted) < M {
		if len(worker.killCh) > 0 {
			<-worker.killCh
			//failure simulation
			return
		}
		//add uncompleted tasks back to list
		if len(worker.redoMap) > 0 {
			task := <-worker.redoMap
			mapTasks = append(mapTasks, task)
		}

		if len(mapTasks) == 0 {
			//wait for tasks to complete
			continue
		}
		requestID := <-worker.workRequests
		task := mapTasks[0]
		mapTasks = mapTasks[1:]
		go worker.assignMap(requestID, task)
	}
	//run all reduce tasks
	for len(worker.workCompleted) < M+R {
		if len(worker.killCh) > 0 {
			<-worker.killCh
			//failure simulation
			return
		}
		//add uncompleted tasks back to list
		if len(worker.redoReduce) > 0 {
			task := <-worker.redoReduce
			reduceTasks = append(reduceTasks, task)
		}
		if len(reduceTasks) == 0 {
			//wait for tasks to complete
			continue
		}
		requestID := <-worker.workRequests
		//pop first task
		task := reduceTasks[0]
		reduceTasks = reduceTasks[1:]
		go worker.assignReduce(requestID, task)
	}
}

func (worker *Worker) appendLog() {
	for {
		task := <-worker.logCh
		//save completed task to log
		worker.rfWorker.Start(task)
		//wait for successful commit of log entry
		<-worker.rfWorker.ApplyCh
		<-worker.newCommits
	}
}

func (worker *Worker) run() {
	//send heartbeats to master
	go worker.sendHB()
	for {
		//send this workers ID to the master to request a task
		leader := worker.findLeader()
		leader.workRequests <- worker.id
		//wait for task to complete
		<-worker.checkCompleted
	}
}

func (worker *Worker) assignMap(requestID int, task *MapTask) {
	go worker.workers[requestID].doMap(task)
	//wait for work completed signal
	worker.newCommits <- 1
	Loop: for {
		select {
		case <- worker.workers[requestID].workCompleted:
			break Loop
		default:
			select {
			case <-worker.workers[requestID].heartbeat:
				continue Loop
			case <-time.After(time.Duration(t_fail)):
				fmt.Printf("\nfailure detected in worker node %d, restarting map task %d\n", requestID, task.id)
				worker.redoMap <- task
				<- worker.newCommits
				worker.workers[requestID].checkCompleted <- 1
				return
			}
		}
	}
	//save completed task to log
	worker.logCh <- &Task{m: task, r: nil}
	worker.workCompleted <- 1
	worker.workers[requestID].checkCompleted <- 1
}

func (worker *Worker) assignReduce(requestID int, task *ReduceTask) {
	go worker.workers[requestID].doReduce(task)
	//wait for work completed signal
	worker.newCommits <- 1
	Loop: for {
		select {
		case <- worker.workers[requestID].workCompleted:
			break Loop
		default:
			select {
			case <-worker.workers[requestID].heartbeat:
				continue Loop
			case <-time.After(time.Duration(t_fail)):
				fmt.Printf("\nfailure detected in worker node %d, restarting reduce task %d\n", requestID, task.id)
				worker.redoReduce <- task
				<- worker.newCommits
				worker.workers[requestID].checkCompleted <- 1
				return
			}
		}
	}
	//save completed task to log
	worker.logCh <- &Task{m: nil, r: task}
	worker.workCompleted <- 1
	worker.workers[requestID].checkCompleted <- 1
}

func (worker *Worker) doMap(task *MapTask) {
	//chunkFileContent := safeRead(task.chunk.Name())
	chunkContent := readFileByByteRange(task.chunkOffset, chunkSize, task.inputFilePath)
	kva := task.mapf("--REDUNDANT-FILE-NAME--", chunkContent)
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
}

func (worker *Worker) doReduce(task *ReduceTask) {
	oname := fmt.Sprintf("./output_files/mr-out-%03d", task.id)
	ofile, _ := os.Create(oname)
	M := worker.numMapTasks

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
}

func (worker *Worker) sendHB() {
	//these nodes cannot send heartbeats to simulate failure
	if worker.id != 5 {
		for {
			worker.heartbeat <- 1
			time.Sleep(X)
		}
	}
}

func (worker *Worker) findLeader() *Worker {
	var leader *Worker
	leader = nil
	for leader == nil {
		for _, peer := range worker.workers {
			if peer.isLeader {
				leader = peer
				break
			}
		}
	}
	return leader
}

func deleteMap(tasks []*MapTask, id int) []*MapTask {
	for i, task := range tasks {
		if task.id == id {
			if i == 0 {
				tasks = tasks[i+1:]
			} else if i == len(tasks)-1 {
				tasks = tasks[:i]
			} else {
				tasks = append(tasks[:i], tasks[i+1:]...)
			}
			break
		}
	}
	return tasks
}

func deleteReduce(tasks []*ReduceTask, id int) []*ReduceTask {
	for i, task := range tasks {
		if task.id == id {
			if i == 0 {
				tasks = tasks[i+1:]
			} else if i == len(tasks)-1 {
				tasks = tasks[:i]
			} else {
				tasks = append(tasks[:i], tasks[i+1:]...)
			}
			break
		}
	}
	return tasks
}
