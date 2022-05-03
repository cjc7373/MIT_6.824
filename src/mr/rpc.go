package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskArgs struct {
}

const (
	TaskMap = 1 << iota
	TaskReduce
	TaskWait
	TaskExit
)

type TaskReply struct {
	TaskType int
	TaskID   int
	Data     string
}

type CompleteTaskArgs TaskReply

type MetadataReply struct {
	NReduce int // the number of reduce tasks
	NMap    int // map task number
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
