package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Files          []string
	TaskId         int
	ReduceNum      int
	MapChan        chan *Task
	ReduceChan     chan *Task
	TaskMetaHolder map[int]*TaskMetaInfo
	Phase          Phase
}

type TaskMetaInfo struct {
	TaskPtr   *Task
	Status    TaskStatus
	StartTime time.Time
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	mu.Lock()
	defer mu.Unlock()
	if c.Phase == DonePhase {
		//fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		ret = true
	}
	return ret
}

func (c *Coordinator) GenerateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

func (c *Coordinator) MakeMapTasks(files []string) {

	for _, file := range files {
		task := Task{
			TaskId:    c.GenerateTaskId(),
			FileName:  file,
			ReduceNum: c.ReduceNum,
			TaskType:  MapTask,
		}

		c.MapChan <- &task
		c.TaskMetaHolder[task.TaskId] = &TaskMetaInfo{
			TaskPtr: &task,
			Status:  WaittingStatus,
		}
	}
	fmt.Println("MakeMapTasks Done")
}

func (c *Coordinator) MakeReduceTasks() {
	for i := 0; i < c.ReduceNum; i++ {
		task := Task{
			TaskId:    c.GenerateTaskId(),
			ReduceNum: i,
			TaskType:  ReduceTask,
		}
		c.ReduceChan <- &task
		c.TaskMetaHolder[task.TaskId] = &TaskMetaInfo{
			TaskPtr: &task,
			Status:  WaittingStatus,
		}
	}
	//fmt.Println("MakeReduceTasks Done")
}

func (c *Coordinator) ReleaseTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	if c.Phase == MapPhase {
		if len(c.MapChan) > 0 {
			*reply = *<-c.MapChan
			if c.MoveTaskStatusToWorking(reply.TaskId) {
				//fmt.Printf("Map Task %d is working\n", reply.TaskId)
			}
		} else {
			reply.TaskType = WaitingTask
			if c.CheckMapTaskStatusAllDone() {
				c.ToNextPhase()
				return nil
			}
		}
	} else if c.Phase == ReducePhase {
		if len(c.ReduceChan) > 0 {
			*reply = *<-c.ReduceChan
			if c.MoveTaskStatusToWorking(reply.TaskId) {
				//fmt.Printf("Reduce Task %d is working\n", reply.ReduceNum)
			}
		} else {
			reply.TaskType = WaitingTask
			if c.CheckReduceTaskStatusAllDone() {
				c.ToNextPhase()
				return nil
			}
		}
	} else {
		reply.TaskType = ExitTask
	}
	return nil

}

func (c *Coordinator) FinishTask(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	if args.TaskType == MapTask {
		if c.TaskMetaHolder[args.TaskId].Status == WorkingStatus {
			c.TaskMetaHolder[args.TaskId].Status = DoneStatus
			//fmt.Printf("Map Task %d is done\n", args.TaskId)
		}

	} else if args.TaskType == ReduceTask {
		if c.TaskMetaHolder[args.TaskId].Status == WorkingStatus {
			c.TaskMetaHolder[args.TaskId].Status = DoneStatus
			//fmt.Printf("Reduce Task %d is done\n", args.ReduceNum)
		}
	}
	return nil
}

func (c *Coordinator) MoveTaskStatusToWorking(taskId int) bool {
	if c.TaskMetaHolder[taskId].Status == WaittingStatus {
		c.TaskMetaHolder[taskId].Status = WorkingStatus
		c.TaskMetaHolder[taskId].StartTime = time.Now()
		return true
	}
	return false
}

func (c *Coordinator) CheckMapTaskStatusAllDone() bool {
	for _, taskMeta := range c.TaskMetaHolder {
		if taskMeta.TaskPtr.TaskType == MapTask {
			if taskMeta.Status != DoneStatus {
				return false
			}
		} else {
			continue
		}
	}
	//fmt.Println("All Map tasks are done")
	return true
}

func (c *Coordinator) CheckReduceTaskStatusAllDone() bool {
	for _, taskMeta := range c.TaskMetaHolder {
		if taskMeta.TaskPtr.TaskType == ReduceTask {
			if taskMeta.Status != DoneStatus {
				return false
			}
		} else {
			continue
		}
	}
	//fmt.Println("All Reduce tasks are done")
	return true
}

func (c *Coordinator) ToNextPhase() {
	if c.Phase == MapPhase {
		c.Phase = ReducePhase
		c.MakeReduceTasks()
	} else if c.Phase == ReducePhase {
		c.Phase = DonePhase
	}
}

func (c *Coordinator) DetectCrash() {
	for {
		time.Sleep(time.Second)
		mu.Lock()
		for taskId, taskMeta := range c.TaskMetaHolder {
			if taskMeta.Status == WorkingStatus {
				if time.Since(taskMeta.StartTime) > 30*time.Second {
					fmt.Printf("Task %d is crashed\n", taskId)
					taskMeta.Status = WaittingStatus
					if taskMeta.TaskPtr.TaskType == MapTask {
						c.MapChan <- taskMeta.TaskPtr
					} else if taskMeta.TaskPtr.TaskType == ReduceTask {
						c.ReduceChan <- taskMeta.TaskPtr
					}
				}
			}
		}
		mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:          files,
		ReduceNum:      nReduce,
		MapChan:        make(chan *Task, len(files)),
		ReduceChan:     make(chan *Task, nReduce),
		TaskMetaHolder: make(map[int]*TaskMetaInfo, len(files)+nReduce),
	}

	c.MakeMapTasks(files)
	c.server()
	go c.DetectCrash()
	return &c
}
