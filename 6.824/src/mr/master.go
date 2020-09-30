package mr

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	files       []string
	mapJob      []string
	mapPending  []string
	reduceJob   []string
	reducePending []string
	nReduce     int
	flag        bool
	mutex sync.Mutex
	set map[string]bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func indexOf(element string, data []string) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1    //not found.
}

func (m *Master) AssignWork(args *RequestWorkArgs, reply *RequestWorkReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.ProcessPrevious(args)
	reply.Type = m.GetMode()
	//m.printStatus("AssignWork " + reply.Type)
	if reply.Type == "Map" {
		reply.File = m.mapJob[0]
		m.mapJob = RemoveIndex(m.mapJob, 0)
		m.mapPending = append(m.mapPending, reply.File)
		reply.Id = indexOf(reply.File, m.files)
		reply.UUID = m.generateUUID()
	} else if reply.Type == "Reduce" {
		reply.File = m.reduceJob[0]
		m.reduceJob = RemoveIndex(m.reduceJob, 0)
		m.reducePending = append(m.reducePending, reply.File)
		reply.Id = indexOf(reply.File, m.files)
		reply.UUID = m.generateUUID()
	} else if reply.Type == "None" {
		//potential check here
	} else if reply.Type == "Wait" {

	}
	reply.NReduce = m.nReduce

	return nil
}

func (m *Master) checkPending() {
	for m.Done() == false {
		time.Sleep(2 * time.Second)
		m.mutex.Lock()
		//m.printStatus("checkPending")
		if len(m.mapPending) != 0 {
			reMap := m.mapPending[0]
			m.mapPending = RemoveIndex(m.mapPending, 0)
			m.mapJob = append(m.mapJob, reMap)
		} else if len(m.reducePending) != 0 {
			reReduce := m.reducePending[0]
			m.reducePending = RemoveIndex(m.reducePending, 0)
			m.reduceJob = append(m.reduceJob, reReduce)
		}
		m.mutex.Unlock()
	}
}

func (m *Master) printStatus(context string) {
	println("--------------------")
	println("in " + context + ":")
	fmt.Printf("%v", m.mapJob)
	println(" mapJob")
	fmt.Printf("%v", m.mapPending)
	println(" mapPending")
	fmt.Printf("%v", m.reduceJob)
	println(" reduceJob")
	fmt.Printf("%v", m.reducePending)
	println(" reducePending")
}

func (m *Master) ProcessPrevious(args *RequestWorkArgs) {
	if args.Type == "None" || args.Type == "Wait" {
		return
	} else if args.Type == "Map" {
		tmp := indexOf(args.File, m.mapPending)
		if tmp != -1 { // in case its job is already done by other workers
			m.mapPending = RemoveIndex(m.mapPending, tmp)
		}

	} else if args.Type == "Reduce" {
		tmp := indexOf(args.File, m.reducePending)
		if tmp != -1 {
			m.reducePending = RemoveIndex(m.reducePending, tmp)
		}

	}
}

func (m *Master) GetMode() string {
	if len(m.mapJob) == 0 && len(m.mapPending) == 0 && len(m.reduceJob) == 0 && len(m.reducePending) == 0 {
		return "None"
	} else if len(m.mapJob) != 0 && len(m.reducePending) == 0 {
		return "Map"
	} else if len(m.mapPending) == 0 && len(m.mapJob) == 0 && len(m.reduceJob) != 0{
		if !m.flag {
			m.printStatus("completed map:")
			m.flag = true
		}

		return "Reduce"
	} else if len(m.mapJob) == 0 && len(m.mapPending) != 0 {
		return "Wait"
	} else if len(m.reduceJob) == 0 && len(m.reducePending) != 0 {
		return "Wait"
	} else {
		println("Wrong!!!!!!!!!")
		return "Wrong"
	}
}


func RemoveIndex(s []string, index int) []string {
	return append(s[:index], s[index + 1:]...)
}



//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.GetMode() == "None" {
		ret = true
	}
	// Your code here.


	return ret
}





//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	m.initJobs(files, nReduce)
	m.server()
	//go m.checkPending()

	return &m
}

func (m *Master) initJobs(files []string, nReduce int) {
	m.files = files
	m.mapJob = append([]string(nil), files...)
	m.nReduce = nReduce
	i := 0
	for i < nReduce {
		m.reduceJob = append(m.reduceJob, strconv.Itoa(i))
		i++
	}
	m.set = make(map[string]bool)
}

func (m *Master) generateUUID() string {
	for true {
		b := make([]byte, 16)
		_, err := rand.Read(b)
		if err != nil {
			log.Fatal(err)
		}
		s := hex.EncodeToString(b)
		if m.set[s] {
			continue
		} else {
			m.set[s] = true
			return s
		}
	}
	return "Wrong UUID"
}