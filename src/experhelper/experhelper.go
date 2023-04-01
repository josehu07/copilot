package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"masterproto"
	"net"
	"net/rpc"
	"os"
)

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7077.")

var epaxos *bool = flag.Bool("epaxos", false, "Querying EPaxos cluster.")
var copilot *bool = flag.Bool("copilot", false, "Querying Copilot cluster.")

var queryStatus *bool = flag.Bool("q", false, "Print cluster status info.")

func main() {
	flag.Parse()
	if *epaxos && *copilot {
		log.Fatalf("Error: cannot set -epaxos and -copilot at the same time\n")
		os.Exit(1)
	}
	if !(*queryStatus) {
		log.Fatalf("Error: no action given to experhelper\n")
		os.Exit(1)
	}

	master, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
	if err != nil {
		log.Fatalf("Error connecting to master\n")
		os.Exit(1)
	}

	replicasReply := new(masterproto.GetReplicaListReply)
	err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), replicasReply)
	if err != nil {
		log.Fatalf("Error making the GetReplicaList RPC\n")
		os.Exit(1)
	}

	N := len(replicasReply.ReplicaList)
	servers := make([]net.Conn, N)
	readers := make([]*bufio.Reader, N)
	writers := make([]*bufio.Writer, N)
	isLeader := make([]bool, N)

	for i := 0; i < N; i++ {
		var err error
		servers[i], err = net.Dial("tcp", replicasReply.ReplicaList[i])
		if err != nil {
			log.Fatalf("Error connecting to replica %d\n", i)
			os.Exit(1)
		}
		readers[i] = bufio.NewReader(servers[i])
		writers[i] = bufio.NewWriter(servers[i])
		isLeader[i] = false
	}

	if !(*epaxos) {
		if !(*copilot) { // classic multipaxos
			leaderReply := new(masterproto.GetLeaderReply)
			if err = master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), leaderReply); err != nil {
				log.Fatalf("Error making the GetLeader RPC\n")
			}
			isLeader[leaderReply.LeaderId] = true
		} else { // copilot
			leaderReply := new(masterproto.GetTwoLeadersReply)
			if err = master.Call("Master.GetTwoLeaders", new(masterproto.GetTwoLeadersArgs), leaderReply); err != nil {
				log.Fatalf("Error making the GetTwoLeaders RPC\n")
			}
			isLeader[leaderReply.Leader1Id] = true
			isLeader[leaderReply.Leader2Id] = true
		}
	}

	if *queryStatus {
		fmt.Println("Cluster status:")
		fmt.Printf("  %2s  %22s  %s\n", "Id", "Addr", "Leader")
		for i := 0; i < N; i++ {
			leaderStr := "false"
			if isLeader[i] {
				leaderStr = "true"
			}
			fmt.Printf("  %2d  %22s  %s\n", i, replicasReply.ReplicaList[i], leaderStr)
		}
	}
}
