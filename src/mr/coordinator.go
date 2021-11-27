package mr

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	sync.Mutex
	mapSize      int
	reduceSize   int
	tasks        map[TaskID]Task
	waitingTasks chan TaskID
	stage        TaskType
	mapCnt       int
	reduceCnt    int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Apply(args *ApplyArgs, reply *ApplyReply) error {

	if c.Done() {
		reply.Closed = true
		return nil
	}

	select {
	case item := <-c.waitingTasks:
		fmt.Println("send ", item)
		c.Lock()
		c.tasks[item].SetStatus(Doing)
		c.tasks[item].SetDeadline(time.Now().Add(time.Second * 10))
		reply.Tasks = []Task{c.tasks[item]}
		c.Unlock()
	default:
	}

	return nil
}

func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
	c.Lock()
	defer c.Unlock()
	for _, id := range args.Dones {
		fmt.Println("done: ", id)
		switch ins := c.tasks[id].(type) {
		case MapTask:
			for i := 0; i < c.reduceSize; i++ {
				err := os.Rename(
					tmpMapOutFileName(ins.GetMapID(), i),
					finalMapOutFileName(ins.GetMapID(), i))
				if err != nil {
					panic("TODO")
				}
			}
			ins.SetStatus(Done)
			c.mapCnt--

		case ReduceTask:
			for i := 0; i < c.mapSize; i++ {
				err := os.Remove(
					finalMapOutFileName(i, ins.GetReduceID()))
				if err != nil {
					panic("TODO")
				}
			}
			err := os.Rename(
				tmpReduceOutFileName(ins.GetReduceID()),
				finalReduceOutFileName(ins.GetReduceID()),
			)
			if err != nil {
				panic("TODO")
			}
			ins.SetStatus(Done)
			c.reduceCnt--
		default:
			panic("")
		}
	}
	for _, id := range args.Failed {
		fmt.Println("failed: ", id)
		c.tasks[id].SetStatus(Waiting)
	}
	go func() {
		for _, id := range args.Failed {
			c.waitingTasks <- id
		}
	}()

	c.trans()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	gob.Register(&mapTask{})
	gob.Register(&reduceTask{})
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
	c.Lock()
	defer c.Unlock()
	return c.stage == Null
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapSize:      len(files),
		reduceSize:   nReduce,
		tasks:        make(map[int]Task),
		waitingTasks: make(chan TaskID),
		stage:        Map,
		mapCnt:       len(files),
		reduceCnt:    nReduce,
	}

	go func() {
		c.Lock()
		taskIDs := []TaskID{}
		for i, file := range files {
			task := &mapTask{
				Status:     Waiting,
				Deadline:   time.Now().Add(time.Second * 10),
				ID:         genTaskID(),
				MapID:      i,
				InputFile:  file,
				MapSize:    c.mapSize,
				ReduceSize: c.reduceSize,
			}
			c.tasks[task.ID] = task
			taskIDs = append(taskIDs, task.ID)
		}
		c.Unlock()
		for _, id := range taskIDs {
			c.waitingTasks <- id
		}
	}()

	c.server()

	go c.recycle()

	return &c
}

func (c *Coordinator) trans() {
	if c.stage == Map {
		if c.mapCnt != 0 {
			return
		}
		c.stage = Reduce
		go func() {
			for i := 0; i < c.reduceSize; i++ {
				c.Lock()
				task := &reduceTask{
					Status:     Waiting,
					Deadline:   time.Now().Add(10 * time.Second),
					ID:         genTaskID(),
					ReduceID:   i,
					MapSize:    c.mapSize,
					ReduceSize: c.reduceSize,
				}
				c.tasks[task.ID] = task
				c.Unlock()
				c.waitingTasks <- task.ID
			}
		}()
	} else if c.stage == Reduce {
		if c.reduceCnt != 0 {
			return
		}
		c.stage = Null
	}
}

func (c *Coordinator) recycle() {
	go func() {
		for {
			time.Sleep(1 * time.Second)

			c.Lock()
			for id, task := range c.tasks {
				if task.GetStatus() == Doing && time.Now().After(task.GetDeadline()) {
					task.SetStatus(Waiting)
					c.waitingTasks <- id
				}
			}
			c.Unlock()
		}
	}()
}

var tasks int32

func genTaskID() TaskID {
	return TaskID(atomic.AddInt32(&tasks, 1))
}
