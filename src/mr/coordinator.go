package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MapTaskInfo struct {
	TaskFile       string
	TaskId         int
	TaskAssignedTo int
	TaskStartTime  time.Time

	Done bool
}

type ReduceTaskInfo struct {
	ReduceId int

	TaskAssignedTo int
	TaskStartTime  time.Time

	Done bool
}
type Coordinator struct {
	// Your definitions here.
	mu    sync.Mutex
	Files []string

	MapTaskDone    []MapTaskInfo
	MapTaskPending []MapTaskInfo
	MapTaskWaiting []MapTaskInfo

	ReduceTaskDone    []ReduceTaskInfo
	ReduceTaskPending []ReduceTaskInfo
	ReduceTaskWaiting []ReduceTaskInfo

	AbandonedWorkers map[int]bool

	NReduce int
}

func (c *Coordinator) init() {
	// Files and NReduce are given
	// Initialize map tasks pending list
	log.Printf("c.init()")
	for index, file := range c.Files {
		c.MapTaskPending = append(c.MapTaskPending, MapTaskInfo{
			TaskFile: file,
			TaskId:   index,
		})
	}
	log.Printf("c.MapTaskPending: %v", c.MapTaskPending)

	// Initialize reduce tasks pending list
	for i := 0; i < c.NReduce; i++ {
		c.ReduceTaskPending = append(c.ReduceTaskPending, ReduceTaskInfo{
			ReduceId: i,
		})
	}

	// Init abandon recorder
	c.AbandonedWorkers = make(map[int]bool)
}

func (c *Coordinator) CheckStaleTasksLoop() {
	for {
		c.mu.Lock()

		// Check stale map tasks

		n := len(c.MapTaskWaiting)

		log.Printf("Checking... There are %v waiting map tasks", n)

		for i := 0; i < n; i++ {
			for index, item := range c.MapTaskWaiting {
				if time.Since(item.TaskStartTime) > time.Duration(10*time.Second) {
					log.Printf("Stale map task found: %v", item)

					// Abandon the corresponding worker
					log.Printf("Abandoning worker %v", item.TaskAssignedTo)
					c.AbandonedWorkers[item.TaskAssignedTo] = true

					// Prepend to Pending list
					c.MapTaskPending = append([]MapTaskInfo{item}, c.MapTaskPending...)

					// Remove from Waiting list
					c.MapTaskWaiting = append(c.MapTaskWaiting[:index], c.MapTaskWaiting[index+1:]...)

					break
				}
			}
		}

		// Check stale reduce tasks

		n = len(c.ReduceTaskWaiting)

		log.Printf("Checking... There are %v waiting reduce tasks", n)

		for i := 0; i < n; i++ {
			for index, item := range c.ReduceTaskWaiting {
				if time.Since(item.TaskStartTime) > time.Duration(10*time.Second) {
					log.Printf("Stale reduce task found: %v", item)

					// Abandon the corresponding worker
					log.Printf("Abandoning worker %v", item.TaskAssignedTo)
					c.AbandonedWorkers[item.TaskAssignedTo] = true

					// Prepend to Pending list
					c.ReduceTaskPending = append([]ReduceTaskInfo{item}, c.ReduceTaskPending...)

					// Remove from Waiting list
					c.ReduceTaskWaiting = append(c.ReduceTaskWaiting[:index], c.ReduceTaskWaiting[index+1:]...)

					break
				}
			}
		}

		c.mu.Unlock()

		time.Sleep(time.Second)
	}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) HandleRequestForTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// First check if all map tasks are done
	if len(c.MapTaskPending) == 0 && len(c.MapTaskWaiting) == 0 {
		// All map tasks are done
		log.Printf("Prepare for a Reduce task")

		if len(c.ReduceTaskPending) == 0 {
			log.Printf("no reduce task available, wait for a second")
			reply.Type = -1
		} else {
			log.Printf("fresh reduce task found")

			reply.Type = 2
			reply.Info.ReduceId = c.ReduceTaskPending[0].ReduceId
			reply.Info.MapTaskNum = len(c.MapTaskDone)

			c.ReduceTaskPending[0].TaskStartTime = time.Now()
			c.ReduceTaskPending[0].TaskAssignedTo = args.WorkerId

			// Append to Waiting list
			c.ReduceTaskWaiting = append(c.ReduceTaskWaiting, c.ReduceTaskPending[0])

			// Remove the first from Pending list
			c.ReduceTaskPending = c.ReduceTaskPending[1:]
		}
	} else {
		// remain undone map tasks
		if len(c.MapTaskPending) == 0 {
			log.Printf("no map task available, wait for a second")
			reply.Type = -1 // Please wait for me
		} else {
			log.Printf("fresh map task found")

			reply.Type = 1 // map task info
			reply.Info.TaskFile = c.MapTaskPending[0].TaskFile
			reply.Info.NReduce = c.NReduce
			reply.Info.TaskId = c.MapTaskPending[0].TaskId

			c.MapTaskPending[0].TaskStartTime = time.Now()
			c.MapTaskPending[0].TaskAssignedTo = args.WorkerId

			// log.Printf("before: %v", len(c.MapTaskWaiting))
			// Append to Waiting list
			c.MapTaskWaiting = append(c.MapTaskWaiting, c.MapTaskPending[0])

			// log.Printf("after: %v", len(c.MapTaskWaiting))
			// Remove the first from Pending list
			c.MapTaskPending = c.MapTaskPending[1:]
		}
	}

	return nil
}

func (c *Coordinator) HandleDoneMapTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// First check if the worker is already abandoned!

	if c.AbandonedWorkers[args.WorkerId] {
		log.Printf("Received done from abandoned worker")
		reply.Type = -2
		return nil
	}

	filenameInWaitingList := false

	matchedIndex := -1

	for index, item := range c.MapTaskWaiting {
		if item.TaskFile == args.Opcode {
			filenameInWaitingList = true
			matchedIndex = index
			break
		}
	}

	if !filenameInWaitingList {
		log.Fatalf("Done map task not found in waiting list")
		reply.Error = true
	} else {
		log.Printf("Map Task Done. Thank you worker %v!", args.WorkerId)

		matchedItem := c.MapTaskWaiting[matchedIndex]

		// Append to Done list
		c.MapTaskDone = append(c.MapTaskDone, matchedItem)

		// log.Printf("before2: %v", len(c.MapTaskWaiting))
		// Remove from Waiting list
		c.MapTaskWaiting = append(c.MapTaskWaiting[:matchedIndex], c.MapTaskWaiting[matchedIndex+1:]...)
		// log.Printf("after2: %v", len(c.MapTaskWaiting))
	}

	return nil
}

func (c *Coordinator) HandleDoneReduceTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// First check if the worker is already abandoned!

	if c.AbandonedWorkers[args.WorkerId] {
		log.Printf("Received done from abandoned worker")
		reply.Type = -2
		return nil
	}

	reduceIdInWaitingList := false

	matchedIndex := -1

	for index, item := range c.ReduceTaskWaiting {
		if item.ReduceId == args.ReduceId {
			reduceIdInWaitingList = true
			matchedIndex = index
		}
	}

	if !reduceIdInWaitingList {
		log.Fatalf("Done reduce task not found in waiting list")
		reply.Error = true
	} else {
		log.Printf("Reduce Task Done. Thank you worker %v", args.WorkerId)

		matchedItem := c.ReduceTaskWaiting[matchedIndex]

		// Append to Done list
		c.ReduceTaskDone = append(c.ReduceTaskDone, matchedItem)

		// Remove from Waiting list
		c.ReduceTaskWaiting = append(c.ReduceTaskWaiting[:matchedIndex], c.ReduceTaskWaiting[matchedIndex+1:]...)
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	ret := false

	// Your code here.

	if len(c.ReduceTaskDone) == c.NReduce {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files: files,

		NReduce: nReduce,
	}

	c.init()

	// Your code here.

	go c.CheckStaleTasksLoop()

	c.server()
	return &c
}
