package main

import (
	"./rf"
	"fmt"
	"os"
)

const numWorkers = 8

//number of servers for replicated logs
const numReplicas = 2

//number of chunks of input data
const M = 100

//number of reduce tasks
const R = 8

//number of tasks completed to wait for before killing master
const n = 4

func checkArgs(argc int, argv []string) (string, string) {
	if argc != 3 {
		fmt.Fprintf(os.Stderr, "Usage: ./main <input_file.txt> <*.so>\n")
		os.Exit(1)
	}
	if _, err := os.Stat(argv[1]); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "'%s' not in current directory\n", argv[1])
		os.Exit(1)
	}
	return argv[1], argv[2]
}

func main() {
	filename, sofilepath := checkArgs(len(os.Args), os.Args)
	chunkFiles := createChunkFiles(filename)
	//make dir for intermediate files and output files to go in
	path := "./intermediate_files"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0700)
	}
	path = "./output_files"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0700)
	}

	workers, mapTasks, reduceTaks := build(sofilepath, chunkFiles)
	rfWorkers := []*rf.Worker{}
	for i := 0; i < numReplicas; i++ {
		persister := rf.InitPersister()
		applyCh := make(chan rf.ApplyMsg, 10)
		new := rf.Make(rfWorkers, i, persister, applyCh)
		rfWorkers = append(rfWorkers, new)
	}
	rfWorkers[0].IsLeader = true
	rfWorkers[0].InitLeader()
	for _, worker := range rfWorkers {
		go worker.Run()
	}
	//master is worker with id 0
	workers[0].isLeader = true
	workers[0].rfWorker = rfWorkers[0]
	go workers[0].runMaster(mapTasks, reduceTaks, false)
	//launch the rest of the workers
	for i := 1; i < numWorkers; i++ {
		go workers[i].run()
	}

	for len(workers[0].workCompleted) < n {
		//simulate master failure, wait for n tasks to complete then kill master node
	}
	fmt.Printf("\nkilling master node to simulate failure\n")
	workers[0].killCh <- 1
	fmt.Printf("restarting master...\n")
	go workers[0].runMaster(mapTasks, reduceTaks, true)
	for len(workers[0].workCompleted) < M+R {
		//wait for all tasks to finish
	}
	fmt.Printf("\nMapReduce completed successfully\n")
}

func build(sofilepath string, chunkFiles map[string]*os.File) ([]*Worker, []*MapTask, []*ReduceTask) {
	workers := make([]*Worker, numWorkers)
	mapTasks := make([]*MapTask, M)
	reduceTasks := make([]*ReduceTask, R)
	mapf, reducef := loadPlugin(sofilepath)

	chunkFileNames := make([]string, len(chunkFiles))
	i := 0
	for k := range chunkFiles {
		chunkFileNames[i] = k
		i++
	}

	for i := 0; i < numWorkers; i++ {
		workers[i] = &Worker{
			id:             i,
			workers:        workers,
			table:          make([]*TableEntry, numWorkers),
			tableInput:     make(chan []*TableEntry, numWorkers*2),
			workRequests:   make(chan int, M+R),
			workCompleted:  make(chan int, M+R),
			checkCompleted: make(chan int, M+R),
			newCommits:     make(chan int, M+R),
			logCh:          make(chan *Task, M+R),
			redoMap:        make(chan *MapTask, M),
			redoReduce:     make(chan *ReduceTask, R),
			killCh:         make(chan int, M+R),
			heartbeat:      make(chan int, 1),
			isLeader:       false,
		}
		for j := 0; j < numWorkers; j++ {
			workers[i].table[j] = &TableEntry{id: j, hb: 0, t: 0}
		}
	}
	for i := 0; i < M; i++ {
		mapTasks[i] = &MapTask{
			id:    i,
			mapf:  mapf,
			chunk: chunkFiles[chunkFileNames[i]],
		}
	}
	for i := 0; i < R; i++ {
		reduceTasks[i] = &ReduceTask{
			id:      i,
			reducef: reducef,
		}
	}
	return workers, mapTasks, reduceTasks
}
