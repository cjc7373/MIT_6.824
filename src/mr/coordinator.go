package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"

// task status
const (
	PENDING = iota
	RUNNING
	DONE
)

type TaskStatus struct {
	id         int
	status     int // task status const
	start_time time.Time
}

type Coordinator struct {
	pending_map_tasks    int
	map_tasks            map[string]*TaskStatus
	pending_reduce_tasks int
	reduce_tasks         map[int]*TaskStatus
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	log.Printf("Received RPC: Example, args: %v", args)
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetMetadata(args int, reply *MetadataReply) error {
	log.Printf("Received RPC: GetMetadata, args: %v", args)
	reply.NMap = len(c.map_tasks)
	reply.NReduce = len(c.reduce_tasks)
	return nil
}

func (c *Coordinator) GetTask(args int, reply *TaskReply) error {
	if c.pending_map_tasks != 0 {
		// TODO: mutex
		for file, task := range c.map_tasks {
			if task.status == PENDING {
				reply.TaskType = "map"
				reply.Data = file
				reply.TaskID = task.id
				// TODO: 为什么如果不存指针, 就需要给 map 重新赋值?
				task.status = RUNNING
				task.start_time = time.Now()
				c.pending_map_tasks--
				log.Printf("Assigned map task %v to worker UNKONWN", file)
				// log.Println(c.map_tasks)
				return nil
			}
		}
	} else {
		reply.TaskType = "wait"
		log.Println("Waiting for map tasks to complete")
		return nil
	}
	return nil
}

func (c *Coordinator) CompleteTask(args *TaskArgs, reply *TaskReply) error {
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
		map_tasks:    make(map[string]*TaskStatus),
		reduce_tasks: make(map[int]*TaskStatus),
	}
	c.pending_map_tasks = len(files)
	for index, file := range files {
		c.map_tasks[file] = &TaskStatus{status: PENDING, id: index}
	}

	c.pending_reduce_tasks = nReduce
	for i := 0; i < nReduce; i++ {
		c.reduce_tasks[i] = &TaskStatus{status: PENDING, id: i}
	}

	// Your code here.
	// TODO: status check

	c.server()
	return &c
}
