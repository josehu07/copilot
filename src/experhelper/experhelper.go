package main

import (
	"bufio"
	"flag"
	"fmt"
	"genericsmrproto"
	"log"
	"masterproto"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
)

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7077.")

var epaxos *bool = flag.Bool("epaxos", false, "Querying EPaxos cluster.")
var copilot *bool = flag.Bool("copilot", false, "Querying Copilot cluster.")

var queryStatus *bool = flag.Bool("q", false, "Print cluster status info.")

var setDurDelay *bool = flag.Bool("dur", false, "Set -durDelay parameters.")
var durReplicaIds *string = flag.String("durReplicaIds", "", "Comma-separated list of replica IDs.")
var durDelayValues *string = flag.String("durDelayValues", "", "Comma-separated list of -durDelay values.")

func main() {
	flag.Parse()
	if *epaxos && *copilot {
		log.Fatalf("Error: cannot set -epaxos and -copilot at the same time\n")
		os.Exit(1)
	}
	if !(*queryStatus) && !(*setDurDelay) {
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
		printStatusQuery(N, replicasReply.ReplicaList, isLeader)
	}

	if *setDurDelay {
		setReplicasDurDelay(N, readers, writers, *durReplicaIds, *durDelayValues)
	}
}

func printStatusQuery(N int, replicaList []string, isLeader []bool) {
	fmt.Println("Cluster status:")
	fmt.Printf("  %2s  %22s  %s\n", "Id", "Addr", "Leader")
	for i := 0; i < N; i++ {
		leaderStr := "false"
		if isLeader[i] {
			leaderStr = "true"
		}
		fmt.Printf("  %2d  %22s  %s\n", i, replicaList[i], leaderStr)
	}
}

func setReplicasDurDelay(N int, readers []*bufio.Reader, writers []*bufio.Writer, targetIdsStr string, targetDelaysStr string) {
	targetIdsSplit := strings.Split(targetIdsStr, ",")
	targetDelaysSplit := strings.Split(targetDelaysStr, ",")
	if len(targetIdsSplit) != len(targetDelaysSplit) {
		log.Fatalf("Error: -durReplicaIds and -durDelayValues contain different numbers of elements\n")
		os.Exit(1)
	}

	fmt.Println("Setting durability delay params:")
	for i := 0; i < len(targetIdsSplit); i++ {
		targetId, err := strconv.Atoi(targetIdsSplit[i])
		if err != nil {
			log.Fatalf("Error parsing targetId %s\n", targetIdsSplit[i])
			os.Exit(1)
		}
		if targetId < 0 || targetId >= N {
			log.Fatalf("Error: invalid targetId %d\n", targetId)
			os.Exit(1)
		}

		targetDelay, err := strconv.Atoi(targetDelaysSplit[i])
		if err != nil {
			log.Fatalf("Error parsing targetDelay %s\n", targetDelaysSplit[i])
			os.Exit(1)
		}
		if targetDelay < 0 {
			log.Fatalf("Error: invalid targetDelay %d\n", targetDelay)
			os.Exit(1)
		}

		paramTweak := &genericsmrproto.ParamTweak{UpdateDurDelay: 1, DurDelay: uint64(targetDelay)}
		writers[targetId].WriteByte(genericsmrproto.PARAM_TWEAK)
		paramTweak.Marshal(writers[targetId])
		writers[targetId].Flush()

		msgType, err := readers[targetId].ReadByte()
		if err != nil {
			log.Fatalf("Error receiving acknowledgement from replica %d\n", targetId)
			os.Exit(1)
		}
		if msgType != genericsmrproto.PARAM_TWEAK_REPLY {
			log.Fatalf("Error: wrong reply type from replica %d, not PARAM_TWEAK_REPLY\n", targetId)
			os.Exit(1)
		}
		ptReply := new(genericsmrproto.ParamTweakReply)
		if err = ptReply.Unmarshal(readers[targetId]); err != nil {
			log.Fatalf("Error parsing ParamTweakReply from replica %d\n", targetId)
		}
		fmt.Printf("  Replica %d  OK? %d\n", targetId, ptReply.OK)
	}
}
