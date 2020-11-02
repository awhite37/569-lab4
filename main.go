package main

import (
	"./rf"
	"fmt"
	"os"
	"math"
)

const numWorkers = 8

//number of bytes in each inputFile-chunk
const chunkSize = 64000

//number of servers for replicated logs
const numReplicas = 2

//number of reduce tasks
const R = 8

//number of tasks completed to wait for before killing master
//used to simulate failure
const n = 20


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

func getNumMapTasks(filePath string) int {
	file := safeOpen(filePath, "r")
	fileInfo, fileStatErr := file.Stat()
	if fileStatErr != nil {
		fmt.Fprintf(os.Stderr, "error stat on file '%s'\n",filePath);
		os.Exit(1);
	}
	fileSize := fileInfo.Size()
	numMapTasks := float64(fileSize) / float64(chunkSize)
	return int(math.Ceil(numMapTasks))
}

func main() {
	filename, sofilepath := checkArgs(len(os.Args), os.Args)
	//TODO: compute M dynamically based on file size
	//each chunk should be ~64MB
	M := getNumMapTasks(filename)

	//make dir for intermediate files and output files to go in
	path := "./intermediate_files"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0700)
	}
	path = "./output_files"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0700)
	}

	workers, mapTasks, reduceTaks := build(filename, sofilepath, M)
	rfWorkers := []*rf.Worker{}
	for i := 0; i < numReplicas; i++ {
		persister := rf.InitPersister()
		applyCh := make(chan rf.ApplyMsg,M+R)
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
	go workers[0].runMaster(mapTasks, reduceTaks)
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
	workers[0].restart = true
	go workers[0].runMaster(mapTasks, reduceTaks)
	for len(workers[0].workCompleted) < M+R {
		//wait for all tasks to finish
	}
	fmt.Printf("\nMapReduce completed successfully\n")
}

func build(inputFilePath string, sofilepath string, M int) ([]*Worker, []*MapTask, []*ReduceTask) {
	workers := make([]*Worker, numWorkers)
	mapTasks := make([]*MapTask, M)
	reduceTasks := make([]*ReduceTask, R)
	mapf, reducef := loadPlugin(sofilepath)

	for i := 0; i < numWorkers; i++ {
		workers[i] = &Worker{
			id:             i,
			workers:        workers,
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
			restart:			 false,
			numMapTasks:    M,
		}
	}
	for i := 0; i < M; i++ {
		mapTasks[i] = &MapTask{
			id:				i,
			mapf:				mapf,
			inputFilePath:	inputFilePath,
			chunkOffset:	int64(i)*chunkSize,
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



