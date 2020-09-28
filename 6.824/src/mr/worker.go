package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


var NReduce int
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
func (a ByKey) Less(i, j int) bool {
	if ihash(a[i].Key) % NReduce != ihash(a[j].Key) % NReduce {
		return ihash(a[i].Key) % NReduce < ihash(a[j].Key) % NReduce
	} else {
		return a[i].Key < a[j].Key
	}

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	reply := RequestWorkReply{}
	args := RequestWorkArgs{}
	args.Type = "Map"
	for true {
		workType := RequestWork(&args, &reply)
		//println(workType)
		if workType == "Map" {
			DoMap(reply, mapf)
		} else if workType == "Reduce" {
			DoReduce(reply, reducef)
		} else if workType == "Wait" {
			//doing nothing
			reply.Type = "Wait"
		} else if workType == "None" {
			break
		}
		time.Sleep(1 * time.Second)
	}

}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func deleteFiles(id int, workers int) {
	prefix := "mr-" + strconv.Itoa(id) + "-"
	for i := 0; i < workers; i++ {
		os.Remove(prefix + strconv.Itoa(i))
	}
}

func DoMap(reply RequestWorkReply, mapf func(string, string) []KeyValue) {
	content, err := ioutil.ReadFile(reply.File)
	if err != nil {
		log.Fatal(err)
	}
	deleteFiles(reply.Id, reply.NReduce)

	// Convert []byte to string and print to screen
	text := string(content)
	resultString := mapf(reply.File, text)
	sort.Sort(ByKey(resultString))
	output := ""
	var f *os.File
	for index, value := range resultString {
		output = "mr-" + strconv.Itoa(reply.Id) + "-" + strconv.Itoa(ihash(value.Key) % reply.NReduce)
		if !fileExists(output) {
			if index != 0 {
				f.Close()
			}
			f, err = os.Create(output)
			if err != nil {
				log.Fatal(err)
			}
			//defer f.Close()
			enc := json.NewEncoder(f)
			err1 := enc.Encode(&resultString[index])
			if err1 != nil {
				log.Fatal(err1)
			}
		} else {
			enc := json.NewEncoder(f)
			err1 := enc.Encode(&resultString[index])
			if err1 != nil {
				log.Fatal(err1)
			}
		}
	}
	f.Close()
}

func DoReduce(reply RequestWorkReply, reducef func(string, []string) string) {
	output := "mr-out-" + reply.File
	if fileExists(output) {
		os.Remove(output)
	}
	outFile, err := os.Create(output)
	if err != nil {
		println(err)
	}
	defer outFile.Close()

	readList := readMap(reply.File)
	//fmt.Printf("%v", result)
	var kva []KeyValue
	for _, fileName := range readList {
		file, err := os.Open(fileName)
		if err != nil {
			println(err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
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
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)

		i = j
	}
}

func readMap(bucket string) []string {
	//var result []string
	var mapOut []string
	prefix := "mr-"
	suffix := "-" + bucket
	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		if strings.HasPrefix(f.Name(), prefix) && strings.HasSuffix(f.Name(), suffix) {
			mapOut = append(mapOut, f.Name())
		}
	}
	return mapOut
}


//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func RequestWork(args *RequestWorkArgs, reply *RequestWorkReply) string {
	call("Master.AssignWork", args, reply)
	NReduce = reply.NReduce
	args.File = reply.File
	args.Type = reply.Type
	return reply.Type
}


//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
