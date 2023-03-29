package main

import (
	"bufio"
	"dlog"
	"flag"
	"fmt"
	"genericsmrproto"
	"log"
	"masterproto"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"os/signal"
	filepath2 "path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"state"
	"strconv"
	"time"
)

const REQUEST_TIMEOUT = 100 * time.Millisecond
const GET_VIEW_TIMEOUT = 100 * time.Millisecond
const GC_DEBUG_ENABLED = false
const PRINT_STATS = false

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7077.")
var reqsNb *int = flag.Int("q", 5000, "Total number of requests. Defaults to 5000.")
var writes *int = flag.Int("w", 100, "Percentage of updates (writes). Defaults to 100%.")
var noLeader *bool = flag.Bool("e", false, "Egalitarian (no leader). Defaults to false.")
var twoLeaders *bool = flag.Bool("twoLeaders", true, "Two leaders for slowdown tolerance. Defaults to false.")
var fast *bool = flag.Bool("f", false, "Fast Paxos: send message directly to all replicas. Defaults to false.")
var rounds *int = flag.Int("r", 1, "Split the total number of requests into this many rounds, and do rounds sequentially. Defaults to 1.")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var check = flag.Bool("check", false, "Check that every expected reply was received exactly once.")
var eps *int = flag.Int("eps", 0, "Send eps more messages per round than the client will wait for (to discount stragglers). Defaults to 0.")
var conflicts *int = flag.Int("c", 0, "Percentage of conflicts. Defaults to 0%")
var s = flag.Float64("s", 2, "Zipfian s parameter")
var v = flag.Float64("v", 1, "Zipfian v parameter")
var cid *int = flag.Int("id", -1, "Client ID.")
var cpuProfile *string = flag.String("cpuprofile", "", "Name of file for CPU profile. If empty, no profile is created.")
var maxRuntime *int = flag.Int("runtime", -1, "Max duration to run experiment in second. If negative, stop after sending up to reqsNb requests")

/* Added by Guanzhou. */

var valueSize = flag.Uint64("vSize", 8, "Value string length")
var pinCoreBase *int = flag.Int("pinCoreBase", -1, "If >= 0, set CPU cores affinity to cores starting at base.")

/* ===== */

// var debug *bool = flag.Bool("debug", false, "Enable debug output.")
var trim *float64 = flag.Float64("trim", 0.25, "Exclude some fraction of data at the beginning and at the end.")
var prefix *string = flag.String("prefix", "", "Path prefix for filenames.")
var hook *bool = flag.Bool("hook", true, "Add shutdown hook.")
var verbose *bool = flag.Bool("verbose", true, "Print throughput to stdout.")
var numKeys *uint64 = flag.Uint64("numKeys", 100000, "Number of keys in simulated store.")
var proxyReplica *int = flag.Int("proxy", -1, "Replica Id to proxy requests to. If id < 0, use request Id mod N as default.")
var sendOnce *bool = flag.Bool("once", false, "Send request to only one leader.")
var tput_interval *float64 = flag.Float64("tput_interval_in_sec", 1, "Time interval to record and print throughput")

// GC debug
var garPercent = flag.Int("garC", 50, "Collect info about GC")

var N int

var clientId uint32

var successful []int
var rsp []bool

// var rarray []int

var latencies []int64
var readlatencies []int64
var writelatencies []int64

var timestamps []time.Time

type DataPoint struct {
	elapse    time.Duration
	reqsCount int64
	t         time.Time
}

type Response struct {
	OpId       int32
	rcvingTime time.Time
	timestamp  int64
}

type View struct {
	ViewId    int32
	PilotId   int32
	ReplicaId int32
	Active    bool
}

var throughputs []DataPoint

func pinCoresAtBase(base int) {
	numCores := runtime.NumCPU()
	fmt.Println("Number of CPU cores:", numCores)

	// var mask uintptr
	// if _, _, errno := syscall.RawSyscall(syscall.SYS_SCHED_GETAFFINITY, 0, uintptr(unsafe.Sizeof(mask)), uintptr(unsafe.Pointer(&mask))); errno != 0 {
	// 	log.Fatalf("Failed to get current CPU affinity: %d\n", errno)
	// 	os.Exit(1)
	// }
	// fmt.Printf("Current CPU affinity: %b\n", mask)
	pid := os.Getpid()
	cmd := exec.Command("taskset", "-p", fmt.Sprintf("%d", pid))
	out, err := cmd.Output()
	if err != nil {
		fmt.Println("Error getting current CPU affinity:", err)
		os.Exit(1)
	}
	fmt.Println("Current CPU affinity:")
	fmt.Printf("%s\n", out)
}

func main() {

	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	// set CPU cores affinity
	if *pinCoreBase >= 0 {
		if *procs != 2 {
			fmt.Println("Error: -pinCoreBase flag only supports the default -procs=2")
			os.Exit(1)
		}
		pinCoresAtBase(*pinCoreBase)
	}

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			dlog.Printf("Error creating CPU profile file %s: %v\n", *cpuProfile, err)
		}
		pprof.StartCPUProfile(f)
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, os.Interrupt)
		go catchKill(interrupt)
		defer pprof.StopCPUProfile()
	}

	if *hook {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		go shutdownHook(c)
	}

	if *cid < 0 {
		clientId = generateRandomClientId()
	} else {
		clientId = uint32(*cid)
	}

	r := rand.New(rand.NewSource(int64(clientId)))
	zipf := rand.NewZipf(r, *s, *v, *numKeys)

	if *conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	master, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
	if err != nil {
		log.Fatalf("Error connecting to master\n")
	}

	rlReply := new(masterproto.GetReplicaListReply)
	err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
	if err != nil {
		log.Fatalf("Error making the GetReplicaList RPC")
	}

	N = len(rlReply.ReplicaList)
	servers := make([]net.Conn, N)
	readers := make([]*bufio.Reader, N)
	writers := make([]*bufio.Writer, N)

	//rarray := make([]int, *reqsNb)
	put := make([]bool, *reqsNb)

	karray := make([]int64, *reqsNb)
	if *noLeader { /*epaxos*/
		for i := 0; i < len(karray); i++ {

			if *conflicts >= 0 {
				r := rand.Intn(100)
				if r < *conflicts {
					karray[i] = 0
				} else {
					// karray[i] = int64(43 + i)
					karray[i] = (int64(i) << 32) | int64(clientId)
				}
				r = rand.Intn(100)
				if r < *writes {
					put[i] = true
				} else {
					put[i] = false
				}
			} else {
				karray[i] = int64(zipf.Uint64())
			}
		}
	} else {
		for i := 0; i < len(karray); i++ {
			karray[i] = rand.Int63n(int64(*numKeys))

			r := rand.Intn(100)
			if r < *writes {
				put[i] = true
			} else {
				put[i] = false
			}
		}
	}

	if *conflicts >= 0 {
		fmt.Println("Uniform distribution")
	} else {
		fmt.Println("Zipfian distribution:")
	}

	for i := 0; i < N; i++ {
		var err error
		servers[i], err = net.Dial("tcp", rlReply.ReplicaList[i])
		if err != nil {
			log.Printf("Error connecting to replica %d\n", i)
		}
		readers[i] = bufio.NewReader(servers[i])
		writers[i] = bufio.NewWriter(servers[i])

	}

	if *twoLeaders {
		fmt.Println("Registering client id", clientId)
		/* Register Client Id */
		for i := 0; i < N; i++ {
			rciArgs := &genericsmrproto.RegisterClientIdArgs{ClientId: clientId}
			writers[i].WriteByte(genericsmrproto.REGISTER_CLIENT_ID)
			rciArgs.Marshal(writers[i])
			writers[i].Flush()
		}
	}

	time.Sleep(1 * time.Second)
	/*registerClientIdSuccessful := waitRegisterClientIdReplies(readers, N)
	fmt.Printf("Client Id Registration succeeds: %d out of %d\n", registerClientIdSuccessful, N)*/

	successful = make([]int, N)
	leader := -1

	// second leader
	leader2 := -1

	isRandomLeader := false

	// views for two leaders
	var views []*View

	if *noLeader == false {

		if *twoLeaders == false {
			reply := new(masterproto.GetLeaderReply)
			if err = master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply); err != nil {
				log.Fatalf("Error making the GetLeader RPC\n")
			}
			leader = reply.LeaderId
			fmt.Printf("The leader is replica %d (%s)\n", leader, rlReply.ReplicaList[leader])

		} else { // two leaders
			reply := new(masterproto.GetTwoLeadersReply)

			if err = master.Call("Master.GetTwoLeaders", new(masterproto.GetTwoLeadersArgs), reply); err != nil {
				log.Fatalf("Error making the GetTwoLeaders")
			}
			leader = reply.Leader1Id
			leader2 = reply.Leader2Id
			//fmt.Printf("The leader 1 is replica %d. The leader 2 is replica %d\n", leader, leader2)
			fmt.Printf("The leader 1 is replica %d (%s); The leader 2 is replica %d (%s)\n", leader, rlReply.ReplicaList[leader], leader2, rlReply.ReplicaList[leader2])

			// Init views. Assume initial view id is 0
			views = make([]*View, 2)
			views[0] = &View{ViewId: 0, PilotId: 0, ReplicaId: int32(leader), Active: true}
			views[1] = &View{ViewId: 0, PilotId: 1, ReplicaId: int32(leader2), Active: true}

		}
	} else if *proxyReplica >= 0 && *proxyReplica < N {
		leader = *proxyReplica
	} else { // epaxos and no designated proxy specified
		isRandomLeader = true
	}

	if *check {
		rsp = make([]bool, *reqsNb)
		for j := 0; j < *reqsNb; j++ {
			rsp[j] = false
		}
	}

	var done chan bool
	var readings chan *DataPoint
	tput_interval_in_sec := time.Duration(*tput_interval * 1e9)
	if *verbose {
		done = make(chan bool, 1)
		readings = make(chan *DataPoint, 600)
		go printer(readings, done)
	}

	var leaderReplyChan chan int32
	var pilot0ReplyChan chan Response
	var viewChangeChan chan *View

	// with pre-specified leader, we know which reader to check reply
	if !*twoLeaders {
		leaderReplyChan = make(chan int32, *reqsNb)
		if isRandomLeader {
			go waitRepliesRandomLeader(readers, N, leaderReplyChan)
		} else {
			go waitReplies(readers, leader, *reqsNb, leaderReplyChan, *reqsNb)
		}
	} else {
		// with another pre-specified leader, we need to check other reply channel, and another reader
		pilot0ReplyChan = make(chan Response, *reqsNb)
		viewChangeChan = make(chan *View, 100)
		for i := 0; i < N; i++ {
			go waitRepliesPilot(readers, i, pilot0ReplyChan, viewChangeChan, *reqsNb*2)
		}
	}

	latencies = make([]int64, 0, *reqsNb)
	readlatencies = make([]int64, 0, *reqsNb)
	writelatencies = make([]int64, 0, *reqsNb)
	timestamps = make([]time.Time, 0, *reqsNb)

	throughputs = make([]DataPoint, 0, 600)

	var reqsCount int64 = 0

	before_total := time.Now()
	lastThroughputTime := before_total

	time.Sleep(1 * time.Second)

	// compose value
	rand.Seed(time.Now().UnixNano())
	vbytes := make([]byte, *valueSize)
	rand.Read(vbytes)
	vstr := string(vbytes)

	var pilotErr, pilotErr1 error
	var lastGVSent0, lastGVSent1 time.Time
	for i := 0; i < *reqsNb; i++ {

		id := int32(i)
		args := genericsmrproto.Propose{id, state.Command{ClientId: clientId, OpId: id, Op: state.PUT, K: 0, V: ""}, time.Now().UnixNano()}

		/* Prepare proposal */
		dlog.Printf("Sending proposal %d\n", id)

		if put[i] {
			args.Command.Op = state.PUT
		} else {
			args.Command.Op = state.GET
		}
		args.Command.K = state.Key(karray[i])
		args.Command.V = state.Value(vstr)
		//args.Timestamp = time.Now().UnixNano()

		before := time.Now()
		timestamps = append(timestamps, before)

		repliedCmdId := int32(-1)
		fromPilot := -1
		var rcvingTime time.Time
		var to *time.Timer
		var batch = int64(-1)
		succeeded := false
		if *twoLeaders {
			for {
				// Check if there is newer view
				for i := 0; i < len(viewChangeChan); i++ {
					newView := <-viewChangeChan
					if newView.ViewId > views[newView.PilotId].ViewId {
						fmt.Printf("New view info: pilotId %v,  ViewId %v, ReplicaId %v\n", newView.PilotId, newView.ViewId, newView.ReplicaId)
						views[newView.PilotId].PilotId = newView.PilotId
						views[newView.PilotId].ReplicaId = newView.ReplicaId
						views[newView.PilotId].ViewId = newView.ViewId
						views[newView.PilotId].Active = true
					}
				}

				// get random server to ask about new view
				serverId := rand.Intn(N)
				if views[0].Active {
					leader = int(views[0].ReplicaId)
					pilotErr = nil
					if leader >= 0 {
						writers[leader].WriteByte(genericsmrproto.PROPOSE)
						args.Marshal(writers[leader])
						pilotErr = writers[leader].Flush()
						if pilotErr != nil {
							views[0].Active = false
						} else {
							succeeded = true
						}
					}
				}
				if !views[0].Active {
					leader = -1
					if lastGVSent0 == (time.Time{}) || time.Since(lastGVSent0) >= GET_VIEW_TIMEOUT {
						for ; serverId == 0; serverId = rand.Intn(N) {
						}
						getViewArgs := &genericsmrproto.GetView{0}
						writers[serverId].WriteByte(genericsmrproto.GET_VIEW)
						getViewArgs.Marshal(writers[serverId])
						writers[serverId].Flush()
						lastGVSent0 = time.Now()
					}
				}

				if views[1].Active {
					leader2 = int(views[1].ReplicaId)
					/* Send to second leader for two-leader protocol */
					pilotErr1 = nil
					if *twoLeaders && !*sendOnce && leader2 >= 0 {
						writers[leader2].WriteByte(genericsmrproto.PROPOSE)
						args.Marshal(writers[leader2])
						pilotErr1 = writers[leader2].Flush()
						if pilotErr1 != nil {
							views[1].Active = false
						} else {
							succeeded = true
						}
					}
				}
				if !views[1].Active {
					leader2 = -1
					if lastGVSent1 == (time.Time{}) || time.Since(lastGVSent1) >= GET_VIEW_TIMEOUT {
						for ; serverId == 1; serverId = rand.Intn(N) {
						}
						getViewArgs := &genericsmrproto.GetView{1}
						writers[serverId].WriteByte(genericsmrproto.GET_VIEW)
						getViewArgs.Marshal(writers[serverId])
						writers[serverId].Flush()
						lastGVSent1 = time.Now()
					}
				}
				if !succeeded {
					continue
				}

				// we successfully sent to at least one pilot
				succeeded = false
				to = time.NewTimer(REQUEST_TIMEOUT)
				toFired := false
				for true {
					select {
					case e := <-pilot0ReplyChan:
						repliedCmdId = e.OpId
						batch = e.timestamp
						rcvingTime = e.rcvingTime
						if repliedCmdId == id {
							to.Stop()
							succeeded = true
						}

					case <-to.C:
						fmt.Printf("Client %v: TIMEOUT for request %v\n", clientId, id)
						repliedCmdId = -1
						rcvingTime = time.Now()
						succeeded = false
						toFired = true

					default:
					}

					if succeeded {
						if *check {
							rsp[id] = true
						}
						reqsCount++
						break
					} else if toFired {
						break
					}

					if repliedCmdId != -1 && repliedCmdId < id {
						// update latency if this response actually arrived ealier
						newLat := int64(rcvingTime.Sub(timestamps[repliedCmdId]) / time.Microsecond)
						if newLat < latencies[repliedCmdId] {
							latencies[repliedCmdId] = newLat
						}
					}
				} // end of foor loop waiting for result
				// successfully get the response. continue with the next request
				if succeeded {
					break
				} else if toFired {
					continue
				}
			} // end of copilot
		} else {
			if isRandomLeader { /*epaxos with random leader*/
				leader = i % N
			} else if *noLeader == false { /*MultiPaxos*/
				leader = 0
			}
			if leader >= 0 {
				writers[leader].WriteByte(genericsmrproto.PROPOSE)
				args.Marshal(writers[leader])
				writers[leader].Flush()
			}
			to = time.NewTimer(REQUEST_TIMEOUT)
			for true {
				select {
				case e := <-leaderReplyChan:
					repliedCmdId = e
					rcvingTime = time.Now()
				default:
				}

				if repliedCmdId == id {
					if *check {
						rsp[id] = true
					}
					reqsCount++
					break
				}
			}
		}

		// Request latency
		lat := int64(rcvingTime.Sub(before) / time.Microsecond)
		latencies = append(latencies, lat)
		if put[i] {
			writelatencies = append(writelatencies, lat)
		} else {
			readlatencies = append(readlatencies, lat)
		}

		if PRINT_STATS && lat >= 10000 {
			if GC_DEBUG_ENABLED {
				var garC debug.GCStats
				debug.ReadGCStats(&garC)
				fmt.Printf("NumGC: %v; PauseTotal: %v; Pause: %v; LastGC: %v\n", garC.NumGC, garC.PauseTotal, garC.Pause, garC.LastGC)
			}
			fmt.Printf("Client %v: request %v: sent at %v; reply from pilot: %v; batch %v; latency: %v\n", clientId, id, before, fromPilot, batch, lat)
		}

		currentTime := time.Now()
		// Throughput every interval
		if currentTime.Sub(lastThroughputTime) >= tput_interval_in_sec {
			p := DataPoint{elapse: currentTime.Sub(before_total), reqsCount: reqsCount, t: currentTime}
			throughputs = append(throughputs, p)

			if *verbose && readings != nil {
				readings <- &p
			}

			lastThroughputTime = currentTime

		}

		if *maxRuntime >= 0 && currentTime.Sub(before_total) > time.Duration(*maxRuntime)*time.Second {
			break
		}

	}
	if *verbose && readings != nil {
		close(readings)
	}
	//fmt.Println(latencies)

	//after_total := time.Now()

	//totalTimeInSec := float64(time.Since(before_total) / time.Second)
	//fmt.Printf("Runtime: %v seconds\n", totalTimeInSec)
	totalRuntime := time.Since(before_total)
	fmt.Println("=========================")
	fmt.Printf("Runtime: %v \n", totalRuntime)
	fmt.Printf("Total requests: %d\n", reqsCount)
	fmt.Printf("Overall average throughput: %v (reqs/sec)\n", uint64(float64(reqsCount)*float64(time.Second)/float64(totalRuntime)))

	if *check {
		for j := int64(0); j < reqsCount; j++ {
			if !rsp[j] {
				fmt.Println("Didn't receive", j)
			}
		}
	}

	// Output latencies and throughput
	writeDataToFiles()
	// GC
	//debug.SetGCPercent(*garPercent)
	//debug.PrintStack()

	if GC_DEBUG_ENABLED {
		var garC debug.GCStats
		debug.ReadGCStats(&garC)
		fmt.Printf("\nLastGC:\t%s", garC.LastGC)         // time of last collection
		fmt.Printf("\nNumGC:\t%d", garC.NumGC)           // number of garbage collections
		fmt.Printf("\nPauseTotal:\t%s", garC.PauseTotal) // total pause for all collections
		fmt.Printf("\nPause:\t%s", garC.Pause)           // pause history, most recent first
	}

	///* Output latencies */
	//writeLatenciesToFile(latencies, "")
	///* Output throughputs */
	//processAndPrintThroughputs(throughputs)

	time.Sleep(1 * time.Second)

	// Clean up
	for _, client := range servers {
		if client != nil {
			client.Close()
		}
	}
	if *verbose && done != nil {
		<-done
	}
	master.Close()
}

func waitReplies(readers []*bufio.Reader, leader int, n int, done chan int32, expected int) {
	var msgType byte
	var err error
	reply := new(genericsmrproto.ProposeReplyTS)

	for true {
		if msgType, err = readers[leader].ReadByte(); err != nil {
			break
		}

		switch msgType {
		case genericsmrproto.PROPOSE_REPLY:
			if err = reply.Unmarshal(readers[leader]); err != nil {
				break
			}
			if reply.OK != 0 {
				successful[leader]++
				//done <- &Response{OpId: reply.CommandId, rcvingTime: time.Now()}
				done <- reply.CommandId
				if expected == successful[leader] {
					return
				}
			}
			break
		default:
			break
		}
	}
}

func waitRepliesRandomLeader(readers []*bufio.Reader, n int, done chan int32) {
	var msgType byte
	var err error
	reply := new(genericsmrproto.ProposeReplyTS)

	for true {
		for i := 0; i < n; i++ {
			if msgType, err = readers[i].ReadByte(); err != nil {
				continue
			}

			switch msgType {
			case genericsmrproto.PROPOSE_REPLY:
				if err = reply.Unmarshal(readers[i]); err != nil {
					continue
				}
				if reply.OK != 0 {
					successful[i]++
					//done <- &Response{OpId: reply.CommandId, rcvingTime: time.Now()}
					done <- reply.CommandId
				}
				break
			default:
				break
			}
		}
	}
}

func waitRepliesPilot(readers []*bufio.Reader, leader int, done chan Response, viewChangeChan chan *View, expected int) {

	var msgType byte
	var err error

	reply := new(genericsmrproto.ProposeReplyTS)
	getViewReply := new(genericsmrproto.GetViewReply)
	for true {
		if msgType, err = readers[leader].ReadByte(); err != nil {
			break
		}

		switch msgType {
		case genericsmrproto.PROPOSE_REPLY:
			if err = reply.Unmarshal(readers[leader]); err != nil {
				break
			}
			if reply.OK != 0 {
				successful[leader]++
				done <- Response{reply.CommandId, time.Now(), reply.Timestamp}
				if expected == successful[leader] {
					return
				}
			}
			break

		case genericsmrproto.GET_VIEW_REPLY:
			if err = getViewReply.Unmarshal(readers[leader]); err != nil {
				break
			}
			if getViewReply.OK != 0 { /*View is active*/
				viewChangeChan <- &View{getViewReply.ViewId, getViewReply.PilotId, getViewReply.ReplicaId, true}
			}
			break

		default:
			break
		}
	}

}

func waitRegisterClientIdReplies(readers []*bufio.Reader, n int) int {

	if n > len(readers) {
		return -1
	}

	success := 0
	reply := new(genericsmrproto.RegisterClientIdReply)
	for i := 0; i < n; i++ {
		i := 0
		//for success < n {
		if err := reply.Unmarshal(readers[i]); err != nil {
			fmt.Println("Error when reading RegisterClientIdReply from replica", i, ":", err)
			i = (i + 1) % n
			continue
		}
		if reply.OK != 0 {
			success++
		}

		i = (i + 1) % n
	}

	return success

}

func generateRandomClientId() uint32 {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	return r.Uint32()
}

func generateRandomOpId() int32 {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	return r.Int31()
}

func printer(dataChan chan *DataPoint, done chan bool) {
	for {
		reading, more := <-dataChan
		if !more {
			if done != nil {
				done <- true
			}
			return
		}
		fmt.Printf("%.1f\t%d\t%.0f\n", float64(reading.elapse)/float64(time.Second), reading.reqsCount, float64(reading.reqsCount)*float64(time.Second)/float64(reading.elapse))
	}

}

/* Trim and sort the latencies */
func processLatencies(latencies []int64) []int64 {

	if len(latencies) <= 0 {
		return latencies
	}
	trimLength := int(float64(len(latencies)) * *trim)
	latencies = latencies[trimLength : len(latencies)-trimLength]
	sort.Sort(int64Slice(latencies))

	return latencies
}

func getLatencyPercentiles(latencies []int64, shouldTrim bool) []int64 {
	if shouldTrim {
		latencies = processLatencies(latencies)
	}

	percentiles := make([]int64, 0, 100)
	l := len(latencies)
	if l == 0 {
		return percentiles
	}

	for i := 1; i < 100; i++ {
		idx := int(float64(l) * float64(i) / 100.0)
		percentiles = append(percentiles, latencies[idx])
	}
	// add 99.9 percentile
	percentiles = append(percentiles, latencies[int(float64(l)*0.999)])
	return percentiles
}

func processAndPrintThroughputs(throughputs []DataPoint) (error, string) {
	var overallTput string = "NaN"
	var instTput string = "NaN"

	filename := fmt.Sprintf("client-%d.throughput.txt", clientId)
	filepath := filepath2.Join(*prefix, filename)
	f, err := os.Create(filepath)

	if err != nil {
		return err, overallTput
	}

	defer f.Close()

	for i, p := range throughputs {
		overallTput = "NaN"
		instTput = "NaN"
		if p.elapse > time.Duration(0) {
			overallTput = strconv.FormatInt(int64(float64(p.reqsCount)*float64(time.Second)/float64(p.elapse)), 10)

		}

		if i == 0 {
			instTput = strconv.FormatInt(p.reqsCount, 10)
		} else if p.elapse > throughputs[i-1].elapse {
			instTput = strconv.FormatInt(int64(
				float64(p.reqsCount-throughputs[i-1].reqsCount)*float64(time.Second)/
					float64(p.elapse-throughputs[i-1].elapse)), 10)
		}
		line := fmt.Sprintf("%.1f\t%d\t%v\t%v\t%.1f\n", float64(p.elapse)/float64(time.Second), p.reqsCount, overallTput, instTput, float64(p.t.UnixNano())*float64(time.Nanosecond)/float64(time.Second))
		_, err = f.WriteString(line)
		fmt.Printf(line)
	}

	// Trimming
	trimmedOverallTput := "NaN"
	trimLength := int(float64(len(throughputs)) * *trim)
	throughputs = throughputs[trimLength : len(throughputs)-trimLength]
	newlen := len(throughputs)
	if newlen == 1 {
		trimmedOverallTput = strconv.FormatInt(int64(
			float64(throughputs[0].reqsCount)*float64(time.Second)/float64(throughputs[0].elapse)), 10)
	} else if newlen > 1 && throughputs[newlen-1].elapse > throughputs[0].elapse {
		trimmedOverallTput = strconv.FormatInt(int64(
			float64(throughputs[newlen-1].reqsCount-throughputs[0].reqsCount)*float64(time.Second)/
				float64(throughputs[newlen-1].elapse-throughputs[0].elapse)), 10)
	}

	fmt.Printf("%s\n", overallTput)
	fmt.Printf("%s\n", trimmedOverallTput)

	_, err = f.WriteString(fmt.Sprintf("%s\n", overallTput))
	_, err = f.WriteString(fmt.Sprintf("%s\n", trimmedOverallTput))

	f.Sync()

	return err, trimmedOverallTput

}

func catchKill(interrupt chan os.Signal) {
	<-interrupt
	if *cpuProfile != "" {
		pprof.StopCPUProfile()
	}
	//fmt.Println(processLatencies(latencies))
	writeLatenciesToFile(latencies, "")
	dlog.Printf("Caught signal and stopped CPU profile before exit.\n")
	os.Exit(0)
}

/* Helper functions to write to file */
func checkError(e error) {
	if e != nil {
		panic(e)
	}
}

func writeLatenciesToFile(latencies []int64, latencyType string) {

	// trimmedLatencies: trimmed and sorted
	trimmedLatencies := processLatencies(latencies)
	filename := fmt.Sprintf("client-%d.%slatency.all.txt", clientId, latencyType)
	filepath := filepath2.Join(*prefix, filename)
	writeSliceToFile(filepath, trimmedLatencies)

	filename = fmt.Sprintf("client-%d.%slatency.percentiles.txt", clientId, latencyType)
	filepath = filepath2.Join(*prefix, filename)
	writeSliceToFile(filepath, getLatencyPercentiles(trimmedLatencies, false))
}

// return the percentiles
func writeLatenciesToFile2(latencies []int64, latencyType string) []int64 {

	// original latencies
	filename := fmt.Sprintf("client-%d.%slatency.orig.txt", clientId, latencyType)
	filepath := filepath2.Join(*prefix, filename)
	writeSliceToFile(filepath, latencies)

	// trimmedLatencies: trimmed and sorted
	trimmedLatencies := processLatencies(latencies)
	filename = fmt.Sprintf("client-%d.%slatency.all.txt", clientId, latencyType)
	filepath = filepath2.Join(*prefix, filename)
	writeSliceToFile(filepath, trimmedLatencies)

	percentiles := getLatencyPercentiles(trimmedLatencies, false)
	filename = fmt.Sprintf("client-%d.%slatency.percentiles.txt", clientId, latencyType)
	filepath = filepath2.Join(*prefix, filename)
	writeSliceToFile(filepath, percentiles)

	return percentiles
}

func writeThroughputLatency(throughput string, latencies []int64, latencyType string) error {

	if len(latencies) < 100 {
		return nil
	}

	filename := fmt.Sprintf("client-%d.tput%slat.txt", clientId, latencyType)
	filepath := filepath2.Join(*prefix, filename)
	f, err := os.Create(filepath)

	if err != nil {
		return err
	}

	defer f.Close()

	text := fmt.Sprintf("%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n", throughput, latencies[0], latencies[24],
		latencies[49], latencies[74], latencies[89], latencies[94], latencies[98], latencies[99])
	_, err = f.WriteString(text)

	if err != nil {
		return err
	}

	return f.Sync()
}

func writeSliceToFile(filename string, arr []int64) error {
	f, err := os.Create(filename)

	if err != nil {
		return err
	}

	defer f.Close()
	//w := bufio.NewWriter(f)

	for _, val := range arr {

		//_, err := w.WriteString(string(val) + "\n")
		text := fmt.Sprintf("%v\n", val)
		_, err := f.WriteString(text)
		//_, err := io.WriteString(f,  text)

		if err != nil {
			return err
		}
	}
	//w.Flush()
	return f.Sync()

}

func writeTimestampsToFile(arr []time.Time, latencies []int64) error {

	filename := fmt.Sprintf("client-%d.timestamps.orig.txt", clientId)
	filepath := filepath2.Join(*prefix, filename)

	f, err := os.Create(filepath)

	if err != nil {
		return err
	}

	defer f.Close()

	var n int
	if len(arr) < len(latencies) {
		n = len(arr)
	} else {
		n = len(latencies)
	}
	for i := 0; i < n; i++ {

		val := arr[i]
		text := fmt.Sprintf("%02d:%02d:%02d.%v\t%v\n", val.Hour(), val.Minute(), val.Second(), val.Nanosecond(), latencies[i])
		_, err := f.WriteString(text)

		if err != nil {
			return err
		}
	}

	return f.Sync()

}

func shutdownHook(c chan os.Signal) {
	sig := <-c
	fmt.Printf("I've got killed by signal %s! Cleaning up...", sig)

	///* Output latencies */
	//writeLatenciesToFile(latencies, "")
	//
	///* Output throughputs */
	//processAndPrintThroughputs(throughputs)
	writeDataToFiles()
	os.Exit(1)
}

func writeDataToFiles() {

	/* Output timestamp */
	writeTimestampsToFile(timestamps, latencies)

	/* Output throughputs */
	_, throughput := processAndPrintThroughputs(throughputs)

	/* Output latencies */
	percentiles := writeLatenciesToFile2(latencies, "")
	writeThroughputLatency(throughput, percentiles, "")

	/* Output read/write latencies */
	// writeLatenciesToFile2(readlatencies, "read")
	// writeLatenciesToFile2(writelatencies, "write")

}

/* Helper interface for sorting int64 */
type int64Slice []int64

func (arr int64Slice) Len() int {
	return len(arr)
}

func (arr int64Slice) Less(i, j int) bool {
	return arr[i] < arr[j]
}

func (arr int64Slice) Swap(i, j int) {
	arr[i], arr[j] = arr[j], arr[i]
}
