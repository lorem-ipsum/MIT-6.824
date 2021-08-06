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

	for {
		reply := w.CallRequestForTask()
		switch reply.Type {
		case 1: // reply with a map task
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
		case 2: // reply with a reduce task
			{
				reduceId := reply.Info.ReduceId
				mapTaskNum := reply.Info.MapTaskNum

				log.Printf("Worker %v: get new ReduceTask(%v)", w.Id, reduceId)

				kva := []KeyValue{}

				for i := 0; i < mapTaskNum; i++ {
					file, err := os.Open(fmt.Sprintf("./mr-tmp/mr-%v-%v", i, reduceId))
					if err != nil {
						log.Fatalf("failed to open file ./mr-tmp/mr-%v-%v", i, reduceId)
					}

					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
				}

				// Steal code from mrsequential.go

				sort.Sort(ByKey(kva))
				oname := fmt.Sprintf("mr-out-%v", reduceId)
				ofile, _ := os.Create(oname)

				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := w.Reducef(kva[i].Key, values)

					// correct format
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

					i = j
				}

				ofile.Close()

				w.callDoneReduceTask(reduceId)

				log.Printf("Worker %v: ReduceTask(%v) done", w.Id, reduceId)

			}
		case -1: // waiting for other workers to complete their task
			{
				log.Printf("Worker %v is waiting for a new task!", w.Id)
			}
		case -2: // Something bad happened
			{
				log.Fatalf("Worker %v: abandoned by the coordinator", w.Id)
			}
		default:
			{
				log.Fatalf("Invalid reply.Type %v!", reply.Type)
			}
		}
		time.Sleep(time.Second)
	}

}

func (w *W) CallRequestForTask() Reply {
	args := Args{
		WorkerId: w.Id,
		Operand:  "CallRequestForTask",
		Opcode:   "",
	}
	reply := Reply{}

	call("Coordinator.HandleRequestForTask", &args, &reply)

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
		log.Fatalf("failed to report done map task")
	}

	return reply
}

func (w *W) callDoneReduceTask(reduceId int) Reply {
	args := Args{
		WorkerId: w.Id,
		Operand:  "CallDoneReduceTask",
		ReduceId: reduceId,
	}
	reply := Reply{}

	call("Coordinator.HandleDoneReduceTask", &args, &reply)

	if reply.Error {
		log.Fatalf("failed to report done reduce task")
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
