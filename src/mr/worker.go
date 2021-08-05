package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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

type W struct {
	Id      int
	Mapf    func(string, string) []KeyValue
	Reducef func(string, []string) string
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	w := W{
		Id:      os.Getpid(),
		Mapf:    mapf,
		Reducef: reducef,
	}

	log.Printf("new Worker generated, Id = %v", w.Id)

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		reply := w.CallRequestForMapTask()
		switch reply.Type {
		case 1:
			{
				filename := reply.Info.TaskFile
				nReduce := reply.Info.NReduce
				taskId := reply.Info.TaskId

				log.Printf("Worker %v: get new MapTask(%v)", w.Id, filename)

				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", file)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", file)
				}
				file.Close()
				kva := w.Mapf(filename, string(content))

				intermediate := make([][]KeyValue, len(kva))

				for _, kv := range kva {
					intermediate[ihash(kv.Key)%nReduce] = append(intermediate[ihash(kv.Key)%nReduce], kv)
				}

				for i := 0; i < nReduce; i++ {
					file, err := os.Create(fmt.Sprintf("./mr-tmp/mr-%v-%v", taskId, i))
					if err != nil {
						log.Fatalf("Failed to create file ./mr-tmp/mr-%v-%v", taskId, i)
					}

					enc := json.NewEncoder(file)
					for _, kv := range intermediate[i] {
						err := enc.Encode(&kv)
						if err != nil {
							log.Fatalf("Failed encoding")
						}
					}

				}

				w.CallDoneMapTask(filename)

				log.Printf("Worker %v: MapTask(%v) done", w.Id, filename)
			}
		case 2:
			{
				log.Printf("Worker %v: get new ReduceTask", w.Id)

			}
		default:
			{
				log.Fatalf("Invalid reply.Type %v!", reply.Type)
			}
		}
		time.Sleep(time.Second)
	}

}

func (w *W) CallRequestForMapTask() Reply {
	args := Args{
		WorkerId: w.Id,
		Operand:  "CallRequestForMapTask",
		Opcode:   "",
	}
	reply := Reply{}

	call("Coordinator.HandleRequestForMapTask", &args, &reply)

	if reply.Error {
		log.Fatalf("failed to fetch task")
	}

	return reply
}

func (w *W) CallDoneMapTask(filename string) Reply {
	args := Args{
		WorkerId: w.Id,
		Operand:  "CallDoneMapTask",
		Opcode:   filename,
	}
	reply := Reply{}

	call("Coordinator.HandleDoneMapTask", &args, &reply)

	if reply.Error {
		log.Fatalf("failed to report done task")
	}

	return reply
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