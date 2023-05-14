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

type MapArgs struct {
}

type MapReply struct {
	Filename         string
	Map_task_id      int
	Nreduce          int
	is_channel_empty bool
}

type ReduceArgs struct {
}

type ReduceReply struct {
	Reduce_task_id   int
	Nreduce          int
	is_channel_empty bool
}

type DoneArgs struct {
	Is_map_task bool
	Filename    string
	Task_id     int
}

type DoneReply struct {
	Can_write_to_file bool
}

type TaskArgs struct {
}

type TaskReply struct {
	Exit_flag   bool
	Has_task    bool
	Is_map_task bool
	Filename    string
	Task_id     int
	Nreduce     int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
