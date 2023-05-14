package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		reply := ackForTask()
		// fmt.Println(reply)
		if reply.Exit_flag {
			// fmt.Println("Worker received Exit_flag, exit")
			os.Exit(0)
		}
		if reply.Has_task {
			if reply.Is_map_task {
				doMapTask(reply, mapf)
			} else {
				doReduceTask(reply, reducef)
			}
			//该部分移至do...()内部
			// done_args := DoneArgs{}
			// done_reply := DoneReply{}
			// done_args.Is_map_task = reply.Is_map_task
			// done_args.Filename = reply.Filename
			// done_args.Task_id = reply.Task_id
			// fmt.Println("task done, call TaskDone method")
			// call("Master.TaskDone", &done_args, &done_reply)
			// if done_reply.Can_write_to_file {

			// }
		} else {
			// fmt.Println("No task left")
		}
		time.Sleep(time.Second)
	}
}

func ackForTask() TaskReply {
	// fmt.Println("ask for task...")
	args := TaskArgs{}
	reply := TaskReply{}

	if !call("Master.GetTask", &args, &reply) {
		// fmt.Println("CallMap() error: call return false")
		return TaskReply{}
	}
	return reply
}

func doMapTask(reply TaskReply, mapf func(string, string) []KeyValue) {
	// fmt.Println("Do map task")
	intermediate := []KeyValue{}
	mapfilename := reply.Filename
	map_task_id := reply.Task_id
	nReduce := reply.Nreduce
	// fmt.Println("reveived file: ", mapfilename)
	pgfileptr, err := os.Open(mapfilename)
	if err != nil {
		log.Fatalf("cannot open %v", mapfilename)
	}
	content, err := ioutil.ReadAll(pgfileptr)
	if err != nil {
		log.Fatalf("cannot read %v", mapfilename)
	}
	pgfileptr.Close()
	kva := mapf(mapfilename, string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))

	mrfilenames := make([]string, 0, nReduce)
	mrfileptrs := make([]*os.File, 0, nReduce)
	encs := make([]*json.Encoder, 0, nReduce)
	mrfile_base_name := "mr-" + strconv.Itoa(map_task_id) + "-"
	// fmt.Println("len of mrfilenames = ", len(mrfilenames))
	for i := 0; i < nReduce; i++ {
		filename := mrfile_base_name + strconv.Itoa(i)
		// fmt.Println("new file: ", filename, " index = ", i)
		mrfilenames = append(mrfilenames, filename)
		// fileptr, err := os.OpenFile(mrfilenames[i], os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0666)
		fileptr, err := ioutil.TempFile(".", "temp"+mrfilenames[i])
		if err != nil {
			log.Fatalf("cannot open or create file: %v", filename)
		}
		mrfileptrs = append(mrfileptrs, fileptr)
		encs = append(encs, json.NewEncoder(mrfileptrs[i]))
	}

	for _, kv := range intermediate {
		// fmt.Printf("Key: %v, Value: %v\n", kv.Key, kv.Value)
		idx := ihash(kv.Key) % nReduce
		err := encs[idx].Encode(&kv)
		if err != nil {
			log.Fatalf("Encode error, Key: %v, Value: %v, idx: %v, filename: %v, file", kv.Key, kv.Value, idx, mrfilenames[idx])
		}
	}
	done_args := DoneArgs{}
	done_reply := DoneReply{}
	done_args.Is_map_task = reply.Is_map_task
	done_args.Filename = reply.Filename
	done_args.Task_id = reply.Task_id
	// fmt.Println("task ", reply.Task_id, " done, call TaskDone method")
	call("Master.TaskDone", &done_args, &done_reply)
	if done_reply.Can_write_to_file {
		for i := 0; i < len(mrfileptrs); i++ {
			os.Rename(mrfileptrs[i].Name(), mrfilenames[i])
			mrfileptrs[i].Close()
		}
	} else {
		for i := 0; i < len(mrfileptrs); i++ {
			os.Remove(mrfileptrs[i].Name())
			mrfileptrs[i].Close()
		}
	}
}

//TODO: 排序，将相同的key合并
func doReduceTask(reply TaskReply, reducef func(string, []string) string) {
	// fmt.Println("Do reduce task")
	dirfiles, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}
	mrfilename := "mr-out-" + strconv.Itoa(reply.Task_id)
	// mrfileptr, err := os.OpenFile(mrfilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	mrfileptr, err := ioutil.TempFile(".", "temp"+mrfilename)
	if err != nil {
		log.Fatal(err)
	}
	intermediate := []KeyValue{}
	for _, file := range dirfiles {
		// fmt.Println("contains file: ", file.Name())
		s := strings.Split(file.Name(), "-")
		// for _, str := range s {
		// 	fmt.Printf("%v, ", str)
		// }
		// fmt.Printf("\n")
		id, err := strconv.Atoi(s[len(s)-1])
		if err != nil {
			continue
		}
		if id != reply.Task_id {
			continue
		}
		// fmt.Println("found file: ", file.Name())
		fileptr, err := os.Open(file.Name())
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(fileptr)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
			// fmt.Println("key: ", kv.Key, "  value: ", kv.Value)
		}
	}
	sort.Sort(ByKey(intermediate))
	for i := 0; i < len(intermediate); {
		j := i + 1
		values := []string{}
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j++
		}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		mrfileptr.WriteString(fmt.Sprintf("%v %v\n", intermediate[i].Key, output))
		i = j
	}
	done_args := DoneArgs{}
	done_reply := DoneReply{}
	done_args.Is_map_task = reply.Is_map_task
	done_args.Task_id = reply.Task_id
	fmt.Println("task ", reply.Task_id, " done, call TaskDone method")
	call("Master.TaskDone", &done_args, &done_reply)
	if done_reply.Can_write_to_file {
		os.Rename(mrfileptr.Name(), mrfilename)
	} else {
		os.Remove(mrfileptr.Name())
	}
	mrfileptr.Close()
}

// func CallMap() MapReply {
// 	args := MapArgs{}
// 	reply := MapReply{}

// 	if !call("Master.GetMapTask", &args, &reply) {
// 		fmt.Println("CallMap() error: call return false")
// 		return MapReply{}
// 	}
// 	return reply
// }

// func CallReduce() ReduceReply {
// 	args := ReduceArgs{}
// 	reply := ReduceReply{}

// 	if !call("Master.GetRecudeTask", &args, &reply) {
// 		fmt.Println("CallReduce() error: call return false")
// 		return ReduceReply{}
// 	}
// 	return reply
// }

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
