package mr

import (
	// "fmt"

	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// type map_chan struct {
// 	filename    string
// 	map_task_id int
// }

type Master struct {
	// Your definitions here.
	Task_chan chan int
	files     []string
	Task_done []bool
	// Task_done_chan chan int
	Nreduce        int
	Work_state     int // 0: map mode, 1: reduce mode
	Work_state_mtx sync.Mutex
	Task_done_mtx  sync.Mutex
	Task_done_cond sync.Cond
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) getMapTask(reply *TaskReply) {
	// fmt.Println("getMapTask() start")
	// fmt.Println("getMapTask() try to get data form channel")
	// file_idx, ok := <-m.Task_chan
	// if ok {
	// 	reply.Filename = m.files[file_idx]
	// 	reply.Task_id = file_idx
	// 	fmt.Println("getMapTask() get task ", file_idx)
	// } else {
	// 	fmt.Println("getMapTask(): channel empty")
	// }
	var task_id int
	select {
	case task_id = <-m.Task_chan:
		reply.Filename = m.files[task_id]
		reply.Task_id = task_id
		reply.Has_task = true
		// fmt.Println("getMapTask() get task ", task_id)
		// fmt.Println("getMapTask(): start a checkTask for ", task_id)
		go m.checkTask(true, task_id)
	default:
		reply.Has_task = false
		// fmt.Println("getMapTask(): channel empty")
	}
	// fmt.Println("getMapTask(): Send file: ", reply.Filename)
	// fmt.Println("getMapTask(): end")
}

func (m *Master) getReduceTask(reply *TaskReply) {
	// task_id, ok := <-m.Task_chan
	// reply.Has_task = ok
	// if ok {
	// 	reply.Task_id = task_id
	// }
	var task_id int
	select {
	case task_id = <-m.Task_chan:
		reply.Has_task = true
		reply.Task_id = task_id
		// fmt.Println("getReduceTask() get task ", task_id)
		// fmt.Println("getReduceTask(): start a checkTask for ", task_id)
		go m.checkTask(false, task_id)
	default:
		reply.Has_task = false
	}
	// fmt.Println("getReduceTask(): end")
}

func (m *Master) GetTask(args *TaskArgs, reply *TaskReply) error {
	// fmt.Println("GetTask(): brfore Lock")
	m.Work_state_mtx.Lock()
	work_state := m.Work_state
	m.Work_state_mtx.Unlock()
	// fmt.Println("GetTask(): after Lock")
	reply.Exit_flag = false
	reply.Nreduce = m.Nreduce
	if work_state == 0 {
		reply.Is_map_task = true
		m.getMapTask(reply)
	} else if work_state == 1 {
		reply.Is_map_task = false
		m.getReduceTask(reply)
	} else {
		reply.Exit_flag = true
	}
	// fmt.Println("GetTask(): end")
	return nil
}

func setTimeout(timeout *bool, mtx *sync.Mutex) {
	mtx.Lock()
	*timeout = true
	mtx.Unlock()
}

func checkTimeout(timeout *bool, mtx *sync.Mutex) bool {
	ret := false
	mtx.Lock()
	ret = *timeout
	mtx.Unlock()
	return ret
}

func (m *Master) waitTime(task_done_chan chan bool, timeout *bool, mtx *sync.Mutex) {
	timer := time.NewTimer(5 * time.Second)
	for {
		select {
		case <-task_done_chan:
			return
		case <-timer.C:
			setTimeout(timeout, mtx)
			m.Task_done_cond.Broadcast()
			return
		}
	}
}

func (m *Master) checkTask(is_map_task bool, task_id int) {
	task_done_chan := make(chan bool, 1)
	all_done := true
	timeout := false
	var timeout_mtx sync.Mutex
	go m.waitTime(task_done_chan, &timeout, &timeout_mtx)

	m.Task_done_mtx.Lock()
	if is_map_task {
		for !m.Task_done[task_id] && !checkTimeout(&timeout, &timeout_mtx) {
			m.Task_done_cond.Wait()
		}
		task_done_chan <- true
		if checkTimeout(&timeout, &timeout_mtx) {
			// fmt.Println("checkTask(): Map task ", task_id, " timeout")
			m.Task_chan <- task_id
			// fmt.Println("task_id ", task_id, "pushed to channel")
		} else {
			// fmt.Println("checkTask(): Map task ", task_id, " done")
			for _, flag := range m.Task_done {
				all_done = all_done && flag
			}
			if all_done {
				m.Work_state_mtx.Lock()
				m.Work_state = 1
				m.Work_state_mtx.Unlock()
				// fmt.Println("TaskDone(): set work_state to 1")
				m.Task_chan = make(chan int, m.Nreduce)
				m.Task_done = make([]bool, m.Nreduce)
				for i := 0; i < m.Nreduce; i++ {
					m.Task_chan <- i
				}
				// fmt.Println("taskDone(): push reduce task to channel success")
			}
		}
	} else {
		for !m.Task_done[task_id] && !checkTimeout(&timeout, &timeout_mtx) {
			m.Task_done_cond.Wait()
		}
		task_done_chan <- true
		if checkTimeout(&timeout, &timeout_mtx) {
			// fmt.Println("checkTask(): Reduce task ", task_id, " timeout")
			m.Task_chan <- task_id
			// fmt.Println("Reduce task ", task_id, "push to channel")
		} else {
			// fmt.Println("checkTask(): Reduce task ", task_id, " done")
			for _, flag := range m.Task_done {
				all_done = all_done && flag
			}
			if all_done {
				m.Work_state_mtx.Lock()
				m.Work_state = 2
				m.Work_state_mtx.Unlock()
				// fmt.Println("TaskDone(): set work_state to 2")
			}
		}
	}
	m.Task_done_mtx.Unlock()
}

func (m *Master) TaskDone(args *DoneArgs, reply *DoneReply) error {

	m.Task_done_mtx.Lock()
	if m.Task_done[args.Task_id] {
		reply.Can_write_to_file = false
	} else {
		m.Task_done[args.Task_id] = true
		reply.Can_write_to_file = true
	}
	m.Task_done_mtx.Unlock()
	m.Task_done_cond.Broadcast()
	return nil
}

func (m *Master) regularBroadcast() {
	for {
		m.Work_state_mtx.Lock()
		work_state := m.Work_state
		m.Work_state_mtx.Unlock()
		if work_state == 2 {
			return
		}
		m.Task_done_cond.Broadcast()
		time.Sleep(time.Second)
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.Work_state_mtx.Lock()
	work_state := m.Work_state
	m.Work_state_mtx.Unlock()
	if work_state == 2 {
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.Task_chan = make(chan int, len(files))
	m.files = files
	// fmt.Println("make map channel")
	// fmt.Println("make reduce channel")
	for i := 0; i < len(files); i++ {
		// fmt.Println("write file ", file, "to channel")
		m.Task_chan <- i
	}
	m.Nreduce = nReduce
	m.Work_state = 0
	m.Task_done = make([]bool, len(files))
	m.Task_done_cond = *sync.NewCond(&m.Task_done_mtx)

	// fmt.Println("start server")
	m.server()
	// go m.regularBroadcast()
	return &m
}
