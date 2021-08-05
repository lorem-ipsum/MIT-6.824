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
	muMap sync.Mutex
	Files []string

	MapTaskDone    []MapTaskInfo
	MapTaskPending []MapTaskInfo
	MapTaskWaiting []MapTaskInfo

	NReduce int
}

func (c *Coordinator) init() {
	// Files and NReduce are given
	// Put all file into Pending
	log.Printf("c.init()")
	for index, file := range c.Files {
		c.MapTaskPending = append(c.MapTaskPending, MapTaskInfo{
			TaskFile: file,
			TaskId:   index,
		})
	}
	log.Printf("c.MapTaskPending: %v", c.MapTaskPending)
}

func (c *Coordinator) CheckStaleMapTasksLoop() {
	for {
		c.muMap.Lock()

		n := len(c.MapTaskWaiting)

		log.Printf("Checking... There are %v Waiting tasks", n)

		for i := 0; i < n; i++ {
			for index, item := range c.MapTaskWaiting {
				if time.Since(item.TaskStartTime) > time.Duration(10*time.Second) {
					log.Printf("Stale task found: %v", item)
					// Append to Pending list
					c.MapTaskPending = append(c.MapTaskPending, item)

					// Remove from Waiting list
					c.MapTaskWaiting = append(c.MapTaskWaiting[:index], c.MapTaskWaiting[index+1:]...)

					break
				}
			}
		}

		c.muMap.Unlock()

		time.Sleep(time.Second)
	}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) HandleRequestForMapTask(args *Args, reply *Reply) error {
	c.muMap.Lock()
	defer c.muMap.Unlock()

	if len(c.MapTaskPending) == 0 {
		log.Printf("no map task found")
		reply.Error = true
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

	return nil
}

func (c *Coordinator) HandleDoneMapTask(args *Args, reply *Reply) error {
	c.muMap.Lock()
	defer c.muMap.Unlock()

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
		log.Printf("Done task not found in waiting list")
		reply.Error = true
	} else {
		log.Printf("Task Done. Thank you!")

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
	ret := false

	// Your code here.

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

	go c.CheckStaleMapTasksLoop()

	c.server()
	return &c
}
