package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

type Args struct {
	WorkerId int
	Operand  string
	Opcode   string

	ReduceId int
}

type Reply struct {
	Type int // 1 for MapTaskInfo, 2 for ReduceTaskInfo
	Info struct {
		// For MapTaskInfo:
		TaskFile string
		NReduce  int
		TaskId   int

		// For ReduceTaskInfo:
		ReduceId   int
		MapTaskNum int
	}
	Error bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
