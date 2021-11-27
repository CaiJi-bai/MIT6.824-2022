package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskType = int
type TaskID = int
type MapID = int
type ReduceID = int

const (
	Map = iota
	Reduce
	Null
)

type TaskStatus int

const (
	Waiting = iota
	Doing
	Done
)

type Task interface {
	GetStatus() TaskStatus
	GetDeadline() time.Time
	SetStatus(TaskStatus)
	SetDeadline(time.Time)
	GetID() TaskID
	GetReduceSize() int
	GetMapSize() int
}

type MapTask interface {
	Task
	GetInputFile() string
	GetMapID() MapID
}

type ReduceTask interface {
	Task
	GetReduceID() ReduceID
}

type mapTask struct {
	Status     TaskStatus
	Deadline   time.Time
	ID         TaskID
	MapID      MapID
	InputFile  string
	MapSize    int
	ReduceSize int
}

func (t *mapTask) GetStatus() TaskStatus {
	return t.Status
}

func (t *mapTask) GetDeadline() time.Time {
	return t.Deadline
}

func (t *mapTask) SetStatus(s TaskStatus) {
	t.Status = s
}

func (t *mapTask) SetDeadline(d time.Time) {
	t.Deadline = d
}

func (t *mapTask) GetID() TaskID {
	return t.ID
}

func (t *mapTask) GetReduceSize() int {
	return t.ReduceSize
}
func (t *mapTask) GetMapSize() int {
	return t.MapSize
}

func (t *mapTask) GetInputFile() string {
	return t.InputFile
}

func (t *mapTask) GetMapID() MapID {
	return t.MapID
}

type reduceTask struct {
	Status     TaskStatus
	Deadline   time.Time
	ID         TaskID
	ReduceID   ReduceID
	MapSize    int
	ReduceSize int
}

func (t *reduceTask) GetStatus() TaskStatus {
	return t.Status
}

func (t *reduceTask) GetDeadline() time.Time {
	return t.Deadline
}

func (t *reduceTask) SetStatus(s TaskStatus) {
	t.Status = s
}

func (t *reduceTask) SetDeadline(d time.Time) {
	t.Deadline = d
}

func (t *reduceTask) GetID() TaskID {
	return t.ID
}

func (t *reduceTask) GetReduceSize() int {
	return t.ReduceSize
}
func (t *reduceTask) GetMapSize() int {
	return t.MapSize
}

func (t *reduceTask) GetReduceID() ReduceID {
	return t.ReduceID
}

type ApplyArgs struct {
}

type ApplyReply struct {
	Tasks  []Task
	Closed bool
}

type ReportArgs struct {
	Dones  []TaskID
	Failed []TaskID
}

type ReportReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func tmpMapOutFileName(mapid, reduce int) string {
	return fmt.Sprintf("mapout-tmp-%d-%d", mapid, reduce)
}

func finalMapOutFileName(mapid, reduce int) string {
	return fmt.Sprintf("mapout-%d-%d", mapid, reduce)
}

func tmpReduceOutFileName(reduce int) string {
	return fmt.Sprintf("mr-out-tmp-%d", reduce)
}

func finalReduceOutFileName(reduce int) string {
	return fmt.Sprintf("mr-out-%d", reduce)
}
