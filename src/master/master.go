package main

import (
	"flag"
	"fmt"
	"genericsmrproto"
	"log"
	"masterproto"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"time"
)

var portnum *int = flag.Int("port", 7087, "Port # to listen on. Defaults to 7087")
var numNodes *int = flag.Int("N", 3, "Number of replicas. Defaults to 3.")
var twoLeaders *bool = flag.Bool("twoLeaders", false, "Two leaders for slowdown tolerance. Defaults to false.")

/* Added by Guanzhou. */

var pinCoreBase *int = flag.Int("pinCoreBase", -1, "If >= 0, set CPU cores affinity to cores starting at base.")

/* ===== */

func pinCoresAtBase(base int) {
	numCores := runtime.NumCPU()
	fmt.Println("Number of CPU cores:", numCores)

	// get current affinity
	// pid := os.Getpid()
	// cmd := exec.Command("taskset", "-p", fmt.Sprintf("%d", pid))
	// out, err := cmd.Output()
	// if err != nil {
	// 	fmt.Println("Error getting current CPU affinity:", err)
	// 	os.Exit(1)
	// }
	// fmt.Printf("%s", out)

	// set desired affinity
	pid := os.Getpid()
	if base < 0 || base > numCores-2 {
		fmt.Println("Error: invalid pinCoreBase", base)
		os.Exit(1)
	}
	mask_str := fmt.Sprintf("%d,%d", base, base+1)
	cmd := exec.Command("taskset", "--cpu-list", "-p", mask_str, fmt.Sprintf("%d", pid))
	out, err := cmd.Output()
	if err != nil {
		fmt.Println("Error setting CPU affinity:", err)
		os.Exit(1)
	}
	fmt.Printf("%s", out)
}

type Master struct {
	N        int
	nodeList []string
	addrList []string
	portList []int
	lock     *sync.Mutex
	nodes    []*rpc.Client
	leader   []bool
	alive    []bool
}

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(2)

	// set CPU cores affinity
	if *pinCoreBase >= 0 {
		pinCoresAtBase(*pinCoreBase)
	}

	log.Printf("Master starting on port %d\n", *portnum)
	log.Printf("...waiting for %d replicas\n", *numNodes)

	master := &Master{*numNodes,
		make([]string, 0, *numNodes),
		make([]string, 0, *numNodes),
		make([]int, 0, *numNodes),
		new(sync.Mutex),
		make([]*rpc.Client, *numNodes),
		make([]bool, *numNodes),
		make([]bool, *numNodes)}

	rpc.Register(master)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum))
	if err != nil {
		log.Fatal("Master listen error:", err)
	}

	go master.run()

	http.Serve(l, nil)
}

func (master *Master) run() {
	for true {
		master.lock.Lock()
		if len(master.nodeList) == master.N {
			master.lock.Unlock()
			break
		}
		master.lock.Unlock()
		time.Sleep(100000000)
	}
	time.Sleep(2000000000)

	// connect to SMR servers
	for i := 0; i < master.N; i++ {
		var err error
		addr := fmt.Sprintf("%s:%d", master.addrList[i], master.portList[i]+1000)
		master.nodes[i], err = rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Fatalf("Error connecting to replica %d\n", i)
		}
		master.leader[i] = false
	}
	master.leader[0] = true

	if master.N >= 2 && *twoLeaders {
		master.leader[1] = true
	}

	for true {
		time.Sleep(3000 * 1000 * 1000)
		new_leader := false
		for i, node := range master.nodes {
			err := node.Call("Replica.Ping", new(genericsmrproto.PingArgs), new(genericsmrproto.PingReply))
			if err != nil {
				//log.Printf("Replica %d has failed to reply\n", i)
				master.alive[i] = false
				if master.leader[i] {
					// neet to choose a new leader
					new_leader = true
					master.leader[i] = false
				}
			} else {
				master.alive[i] = true
			}
		}
		if !new_leader {
			continue
		}
		for i, new_master := range master.nodes {
			if master.alive[i] {
				err := new_master.Call("Replica.BeTheLeader", new(genericsmrproto.BeTheLeaderArgs), new(genericsmrproto.BeTheLeaderReply))
				if err == nil {
					master.leader[i] = true
					// log.Printf("Replica %d is the new leader.", i)
					break
				}
			}
		}
	}
}

func (master *Master) Register(args *masterproto.RegisterArgs, reply *masterproto.RegisterReply) error {

	master.lock.Lock()
	defer master.lock.Unlock()

	nlen := len(master.nodeList)
	index := nlen

	addrPort := fmt.Sprintf("%s:%d", args.Addr, args.Port)

	for i, ap := range master.nodeList {
		if addrPort == ap {
			index = i
			break
		}
	}

	if index == nlen {
		master.nodeList = master.nodeList[0 : nlen+1]
		master.nodeList[nlen] = addrPort
		master.addrList = master.addrList[0 : nlen+1]
		master.addrList[nlen] = args.Addr
		master.portList = master.portList[0 : nlen+1]
		master.portList[nlen] = args.Port
		nlen++
	}

	if nlen == master.N {
		reply.Ready = true
		reply.ReplicaId = index
		reply.NodeList = master.nodeList
	} else {
		reply.Ready = false
	}

	return nil
}

func (master *Master) GetLeader(args *masterproto.GetLeaderArgs, reply *masterproto.GetLeaderReply) error {
	time.Sleep(4 * 1000 * 1000)
	for i, l := range master.leader {
		if l {
			*reply = masterproto.GetLeaderReply{i}
			break
		}
	}
	return nil
}

// slowdown: get two leaders
func (master *Master) GetTwoLeaders(args *masterproto.GetTwoLeadersArgs, reply *masterproto.GetTwoLeadersReply) error {

	time.Sleep(4 * 1000 * 1000)
	reply.Leader1Id = -1
	reply.Leader2Id = -1
	for i, l := range master.leader {
		if l {
			if reply.Leader1Id == -1 {
				reply.Leader1Id = i
			} else {
				reply.Leader2Id = i
				break
			}
		}
	}
	return nil
}

func (master *Master) GetReplicaList(args *masterproto.GetReplicaListArgs, reply *masterproto.GetReplicaListReply) error {
	master.lock.Lock()
	defer master.lock.Unlock()

	if len(master.nodeList) == master.N {
		reply.ReplicaList = master.nodeList
		reply.Ready = true
	} else {
		reply.Ready = false
	}
	return nil
}
