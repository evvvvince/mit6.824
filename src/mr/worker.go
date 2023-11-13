package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Task struct {
	FileName  string
	TaskId    int
	TaskType  TaskType
	ReduceNum int
}

type TaskArgs struct {
}

type TaskType int

type Phase int

type TaskStatus int

const (
	MapTask     TaskType = 0
	ReduceTask  TaskType = 1
	WaitingTask TaskType = 2
	ExitTask    TaskType = 3
)

const (
	MapPhase    Phase = 0
	ReducePhase Phase = 1
	DonePhase   Phase = 2
)

const (
	WorkingStatus  TaskStatus = 0
	WaittingStatus TaskStatus = 1
	DoneStatus     TaskStatus = 2
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	keepFlag := true
	for keepFlag {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(task, mapf)
				CallDone(task)
			}
		case ReduceTask:
			{
				DoReduceTask(task, reducef)
				CallDone(task)
			}
		case WaitingTask:
			//fmt.Println("Waiting for task")
			time.Sleep(time.Second)
		case ExitTask:
			//fmt.Println("All task done")
			keepFlag = false
		}
	}

}

func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.ReleaseTask", &args, &reply)
	if ok {
		//fmt.Printf("GetTask success!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func CallDone(task Task) Task {
	args := task
	reply := Task{}
	ok := call("Coordinator.FinishTask", &args, &reply)
	if ok {
		//fmt.Printf("FinishTask success!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}
func DoMapTask(task Task, mapf func(string, string) []KeyValue) {
	fileName := task.FileName
	intermediate := []KeyValue{}
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)

	for _, kv := range intermediate {
		reduceNum := ihash(kv.Key) % task.ReduceNum
		oname := fmt.Sprintf("mr-tmp-%d-%d", task.TaskId, reduceNum)
		ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if ofile != nil {
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
			ofile.Close()
		}
	}

}

func DoReduceTask(task Task, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	reduceNum := task.ReduceNum
	fileName := fmt.Sprintf("mr-tmp-*-%d", reduceNum)
	filePaths, _ := filepath.Glob(fileName)
	for _, file := range filePaths {
		file, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		var kv KeyValue
		for {
			_, err := fmt.Fscanf(file, "%s %s\n", &kv.Key, &kv.Value)
			if err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	i := 0
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", reduceNum)
	ofile, _ := os.Create(oname)
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

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
