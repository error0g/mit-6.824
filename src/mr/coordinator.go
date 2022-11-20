package mr

import (
	"fmt"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	mapTaskStatus    map[int]int
	reduceTaskStatus map[int]int

	//filename:jobId
	mapTask     map[string]int
	reduceTTask map[string]int

	mapDone    bool
	reduceDone bool

	mapChan    chan Job
	reduceChan chan Job
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Start(id *string, reply *Job) error {
	temp := <-c.mapChan
	reply.Id = temp.Id
	reply.JobType = temp.JobType
	reply.File = temp.File
	return nil
}

func (c *Coordinator) Test() error {
	if len(c.mapChan) > 0 {
		temp := <-c.mapChan
		fmt.Printf("id: %v\n", temp.Id)
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
var id = 0

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	Init(&c, files)
	c.server()
	//c.Test()
	return &c
}

func Init(c *Coordinator, files []string) {
	c.mapTaskStatus = make(map[int]int)
	c.reduceTaskStatus = make(map[int]int)
	c.mapTask = make(map[string]int)
	c.reduceTTask = make(map[string]int)
	c.mapDone = false
	c.reduceDone = false
	c.mapChan = make(chan Job)
	c.reduceChan = make(chan Job)
	// Your code here.
	for _, file := range files {
		c.mapTask[file] = id
		c.mapTaskStatus[id] = 0
		id = id + 1
	}
	go func() {
		for k, v := range c.mapTask {
			job := Job{}
			job.Id = v
			job.JobType = 0
			job.File = k
			c.mapChan <- job
		}
	}()
}
