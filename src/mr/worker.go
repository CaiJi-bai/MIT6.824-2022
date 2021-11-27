package mr

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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
	gob.Register(&mapTask{})
	gob.Register(&reduceTask{})

	// Your worker implementation here.
	id := os.Getpid()
	fmt.Println("worker: ", id)
	completeTasks := make(chan struct {
		ID TaskID
		OK bool
	})
	go func() {
		for task := range completeTasks {
			if task.OK {
				reportTasks([]TaskID{task.ID}, []TaskID{})
			} else {
				reportTasks([]TaskID{}, []TaskID{task.ID})
			}
		}
	}()

	for {
		tasks, closed, _ := applyTasks() // TODO retry
		// fmt.Println("apply ", len(tasks), closed)
		if closed {
			// fmt.Println("exit")
			break // nothing to do
		}
		if len(tasks) == 0 {
			time.Sleep(time.Second)
			// fmt.Println("wait")
			continue
		}
		for _, task := range tasks {
			// fmt.Println("do ", task.GetID())
			var err error
			switch ins := task.(type) {
			case MapTask:
				err = executeMapTask(id, ins, mapf) // TODO

			case ReduceTask:
				err = executeReduceTask(id, ins, reducef) // TODO

			default:
				panic("TODO")
			}
			// fmt.Println("complete ", task.GetID())
			completeTasks <- struct {
				ID int
				OK bool
			}{task.GetID(), err != nil}
		}
	}
}

func reportTasks(dones []TaskID, failed []TaskID) error {
	args := ReportArgs{Dones: dones, Failed: failed}
	reply := ReportReply{}
	ok := call("Coordinator.Report", &args, &reply)
	if !ok {
		return fmt.Errorf("something wrong with rpc")
	}
	return nil
}

func applyTasks() ([]Task, bool, error) {
	args := ApplyArgs{}
	reply := ApplyReply{}
	ok := call("Coordinator.Apply", &args, &reply)
	if !ok {
		return nil, false, fmt.Errorf("something wrong with rpc")
	}
	return reply.Tasks, reply.Closed, nil
}

func executeMapTask(worker int, task MapTask, mapf func(string, string) []KeyValue) (err error) {
	defer func() {
		if c := recover(); c != nil {
			err = fmt.Errorf("panic handler")
		}
	}()
	file, _ := os.Open(task.GetInputFile())
	// TODO
	content, _ := ioutil.ReadAll(file)
	// TODO
	kvs := mapf(task.GetInputFile(), string(content))
	kvsGrouped := make(map[int][]KeyValue)
	for _, kv := range kvs {
		hashed := ihash(kv.Key) % task.GetReduceSize()
		kvsGrouped[hashed] = append(kvsGrouped[hashed], kv)
	}
	for i := 0; i < task.GetReduceSize(); i++ {
		ofile, _ := os.Create(tmpMapOutFileName(task.GetMapID(), i))
		// TODO
		for _, kv := range kvsGrouped[i] {
			fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)
		}
		ofile.Close()
	}
	return
}

func executeReduceTask(worker int, task ReduceTask, reducef func(string, []string) string) (err error) {
	defer func() {
		if c := recover(); c != nil {
			err = fmt.Errorf("panic handler")
		}
	}()
	var lines []string
	for mi := 0; mi < task.GetMapSize(); mi++ {
		inputFile := finalMapOutFileName(mi, task.GetReduceID())
		file, _ := os.Open(inputFile)
		// TODO
		content, _ := ioutil.ReadAll(file)
		// TODO
		lines = append(lines, strings.Split(string(content), "\n")...)
	}
	var kvs []KeyValue
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		kvs = append(kvs, KeyValue{
			Key:   parts[0],
			Value: parts[1],
		})
	}
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })

	ofile, _ := os.Create(tmpReduceOutFileName(task.GetReduceID()))
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)
		i = j
	}
	ofile.Close()
	return nil
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

	// fmt.Println(err)
	return false
}
