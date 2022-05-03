package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

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
	mu                  sync.Mutex // Only one goroutine can manipulate this data structure
	undone_map_tasks    int
	map_tasks           map[string]*TaskStatus
	undone_reduce_tasks int
	reduce_tasks        map[int]*TaskStatus
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	DPrintf("Received RPC: Example, args: %v", args)
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetMetadata(args int, reply *MetadataReply) error {
	DPrintf("Received RPC: GetMetadata, args: %v", args)
	reply.NMap = len(c.map_tasks)
	reply.NReduce = len(c.reduce_tasks)
	return nil
}

func (c *Coordinator) GetTask(args int, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.undone_map_tasks != 0 {
		for file, task := range c.map_tasks {
			if task.status == PENDING {
				reply.TaskType = "map"
				reply.Data = file
				reply.TaskID = task.id
				// TODO: 为什么如果不存指针, 就需要给 map 重新赋值?
				task.status = RUNNING
				task.start_time = time.Now()
				DPrintf("Assigned map task %v to worker %v", file, args)
				// DPrintf(c.map_tasks)
				return nil
			}
		}
		// if no more pending but some are still running
		reply.TaskType = "wait"
		DPrintf("Waiting for map tasks to complete")
		return nil
	} else if c.undone_reduce_tasks != 0 {
		reply.TaskType = "reduce"
		for i, task := range c.reduce_tasks {
			if task.status == PENDING {
				reply.TaskID = i
				task.status = RUNNING
				task.start_time = time.Now()
				DPrintf("Assigned reduce task %v to worker %v", i, args)
				return nil
			}
		}
		reply.TaskType = "wait"
		DPrintf("Waiting for other reduce tasks to complete")
		return nil
	}

	// All done!
	reply.TaskType = "exit"
	DPrintf("undone_map_tasks: %v undone_reduce_tasks: %v", c.undone_map_tasks, c.undone_reduce_tasks)
	DPrintf("All tasks are done!")
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == "map" {
		c.map_tasks[args.Data].status = DONE
		c.undone_map_tasks--
		return nil
	} else if args.TaskType == "reduce" {
		c.reduce_tasks[args.TaskID].status = DONE
		c.undone_reduce_tasks--
		return nil
	}
	return errors.New("THIS SHOULD NOT BE EXECUTED")
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
	go http.Serve(l, nil) // 这能够同时处理多个请求吗
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.undone_map_tasks == 0 && c.undone_reduce_tasks == 0 {
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
		map_tasks:    make(map[string]*TaskStatus),
		reduce_tasks: make(map[int]*TaskStatus),
	}
	c.undone_map_tasks = len(files)
	for index, file := range files {
		c.map_tasks[file] = &TaskStatus{status: PENDING, id: index}
	}

	c.undone_reduce_tasks = nReduce
	for i := 0; i < nReduce; i++ {
		c.reduce_tasks[i] = &TaskStatus{status: PENDING, id: i}
	}

	// Your code here.
	// TODO: status check

	c.server()
	return &c
}
