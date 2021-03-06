package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	n_map, n_reduce := get_metadata()
	pid := os.Getpid()

	for {
		reply := TaskReply{}
		ok := call("Coordinator.GetTask", pid, &reply)
		if ok {
			DPrintf("Got task %+v", reply)

			switch reply.TaskType {
			case TaskMap:
				do_map_task(mapf, reply.Data, reply.TaskID, n_reduce)
			case TaskReduce:
				do_reduce_task(reducef, reply.TaskID, n_map)
			case TaskWait:
				time.Sleep(time.Second)
			case TaskExit:
				DPrintf("Receiving task type %v, exiting", reply.TaskType)
				os.Exit(0)
			}
		}

	}

}

func get_metadata() (n_map int, n_reduce int) {
	reply := MetadataReply{}

	ok := call("Coordinator.GetMetadata", 0, &reply)
	if ok {
		DPrintf("metadata %+v", reply)
		n_reduce = reply.NReduce
		n_map = reply.NMap
	} else {
		DPrintf("call failed!")
	}
	return
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func do_map_task(mapf func(string, string) []KeyValue, input_filename string, task_id int, n_reduce int) {
	DPrintf("Doing map task %v", input_filename)
	file, err := os.Open(input_filename)
	if err != nil {
		log.Fatalf("cannot open %v", input_filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", input_filename)
	}
	file.Close()
	kva := mapf(input_filename, string(content))

	intermediate_files := make([]*os.File, n_reduce)
	encs := make([]*json.Encoder, n_reduce)
	for i := 0; i < n_reduce; i++ {
		filename := "mr-" + strconv.Itoa(task_id) + "-" + strconv.Itoa(i)
		intermediate_files[i], err = os.Create(filename)
		if err != nil {
			log.Fatalf("cannot create %v", filename)
		}
		encs[i] = json.NewEncoder(intermediate_files[i])
	}

	for _, kv := range kva {
		index := ihash(kv.Key) % n_reduce
		err := encs[index].Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}
	args := CompleteTaskArgs{TaskType: TaskMap, TaskID: task_id, Data: input_filename}
	reply := 0
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		DPrintf("Done map task %v", input_filename)
	} else {
		DPrintf("ERROR")
	}
}

func do_reduce_task(reducef func(string, []string) string, task_id, n_map int) {
	intermediate := []KeyValue{}

	for i := 0; i < n_map; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task_id)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			// DPrintf("Appending %v", kv)
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate)) // TODO: sort ????????????????

	oname := "mr-out-" + strconv.Itoa(task_id)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	args := CompleteTaskArgs{TaskType: TaskReduce, TaskID: task_id}
	reply := 0
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		DPrintf("Done reduce task %v", task_id)
	} else {
		DPrintf("ERROR")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
