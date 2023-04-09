package paxos

import (
	"bytes"
	"dlog"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
	"io"
	"log"
	"os"
	"paxosproto"
	"state"
	"sync/atomic"
	"time"
)

const INJECT_SLOWDOWN = false

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)

const MAX_BATCH = 5000
const BATCH_INTERVAL = 100 * time.Microsecond

const BENCH_LOGGING_INTERVAL = 200 * time.Millisecond

var TIME_BREAK_BASE_DATE = time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC)

type Replica struct {
	*genericsmr.Replica // extends a generic Paxos replica
	prepareChan         chan fastrpc.Serializable
	acceptChan          chan fastrpc.Serializable
	commitChan          chan fastrpc.Serializable
	commitShortChan     chan fastrpc.Serializable
	prepareReplyChan    chan fastrpc.Serializable
	acceptReplyChan     chan fastrpc.Serializable
	prepareRPC          uint8
	acceptRPC           uint8
	commitRPC           uint8
	commitShortRPC      uint8
	prepareReplyRPC     uint8
	acceptReplyRPC      uint8
	IsLeader            bool        // does this replica think it is the leader
	instanceSpace       []*Instance // the space of all instances (used and not yet used)
	crtInstance         int32       // highest active instance number that this replica knows about
	defaultBallot       int32       // default ballot for new instances (0 until a Prepare(ballot, instance->infinity) from a leader)
	Shutdown            bool
	counter             int
	flush               bool
	committedUpTo       int32

	// batch size logging
	batchSizeLogFile *os.File
	batchSizeBuffer  []int

	// timing breakdown logging
	timeBreakLogFile *os.File
	timeBreakBuffers map[string]map[int32]int64
}

type InstanceStatus int

const (
	PREPARING InstanceStatus = iota
	PREPARED
	ACCEPTED
	COMMITTED
)

type Instance struct {
	cmds   []state.Command
	ballot int32
	status InstanceStatus
	lb     *LeaderBookkeeping
}

type LeaderBookkeeping struct {
	clientProposals []*genericsmr.Propose
	maxRecvBallot   int32
	prepareOKs      int
	acceptOKs       int
	nacks           int
	leaderLogged    bool
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool, durDelayPerSector uint64,
	batchSizeLogFile *os.File, timeBreakLogFile *os.File) *Replica {
	r := &Replica{genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply, durable, durDelayPerSector),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
		0, 0, 0, 0, 0, 0,
		false,
		make([]*Instance, 15*1024*1024),
		0,
		-1,
		false,
		0,
		true,
		-1,
		batchSizeLogFile,
		nil,
		timeBreakLogFile,
		make(map[string]map[int32]int64),
	}

	if r.batchSizeLogFile != nil {
		r.timeBreakLogFile.WriteString(fmt.Sprintf("MyReplicaID %d\n", r.Id))
		r.timeBreakLogFile.WriteString("cols ReplicaID BatchSize\n")
	}

	if r.timeBreakLogFile != nil {
		r.timeBreakBuffers["Propose"] = make(map[int32]int64)
		r.timeBreakBuffers["PrepareSend"] = make(map[int32]int64)
		r.timeBreakBuffers["PrepareRecv"] = make(map[int32]int64)
		r.timeBreakBuffers["PrepareReplySend"] = make(map[int32]int64)
		r.timeBreakBuffers["PrepareReplyRecv"] = make(map[int32]int64)
		r.timeBreakBuffers["AcceptSend"] = make(map[int32]int64)
		r.timeBreakBuffers["AcceptRecv"] = make(map[int32]int64)
		r.timeBreakBuffers["AcceptReplySend"] = make(map[int32]int64)
		r.timeBreakBuffers["AcceptReplyRecv"] = make(map[int32]int64)
		r.timeBreakBuffers["Execute"] = make(map[int32]int64)
		r.timeBreakBuffers["Acknowledge"] = make(map[int32]int64)
		r.timeBreakBuffers["IsAbnormal"] = make(map[int32]int64)

		r.timeBreakLogFile.WriteString(fmt.Sprintf("MyReplicaID %d\n", r.Id))
		r.timeBreakLogFile.WriteString("cols Action ReplicaID InstanceID Timestamp(ns)\n")
	}

	r.prepareRPC = r.RegisterRPC(new(paxosproto.Prepare), r.prepareChan)
	r.acceptRPC = r.RegisterRPC(new(paxosproto.Accept), r.acceptChan)
	r.commitRPC = r.RegisterRPC(new(paxosproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(paxosproto.CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RegisterRPC(new(paxosproto.PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RegisterRPC(new(paxosproto.AcceptReply), r.acceptReplyChan)

	go r.run()

	return r
}

// append a log entry to stable storage
func (r *Replica) recordInstanceMetadata(inst *Instance) {
	// inject durability delay
	durDelayPerSector := atomic.LoadUint64(&r.DurDelayPerSector)
	if durDelayPerSector > 0 {
		time.Sleep(time.Duration(durDelayPerSector) * time.Nanosecond)
	}

	if !r.Durable {
		return
	}

	var b [5]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.ballot))
	b[4] = byte(inst.status)
	r.StableStore.Write(b[:])
}

// write a sequence of commands to stable storage
func (r *Replica) recordCommands(cmds []state.Command) {
	// inject durability delay
	durDelayPerSector := atomic.LoadUint64(&r.DurDelayPerSector)
	if durDelayPerSector > 0 {
		payloadLen := uint64(0)
		for i := 0; i < len(cmds); i++ {
			payloadLen += uint64(17 + len(cmds[i].V))
		}

		numSectors := payloadLen / 512
		if payloadLen%512 != 0 {
			numSectors++
		}

		time.Sleep(time.Duration(numSectors*durDelayPerSector) * time.Nanosecond)
	}

	if !r.Durable {
		return
	}

	if cmds == nil {
		return
	}
	for i := 0; i < len(cmds); i++ {
		cmds[i].Marshal(io.Writer(r.StableStore))
	}
}

// sync with the stable store
func (r *Replica) sync() {
	if !r.Durable {
		return
	}

	r.StableStore.Sync()
}

/* RPC to be called by master */

func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs, reply *genericsmrproto.BeTheLeaderReply) error {
	r.IsLeader = true
	return nil
}

func (r *Replica) replyPrepare(replicaId int32, reply *paxosproto.PrepareReply) {
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *paxosproto.AcceptReply) {
	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

/* ============= */

var clockChan chan bool

func (r *Replica) clock() {
	for !r.Shutdown {
		time.Sleep(BATCH_INTERVAL)
		clockChan <- true
	}
}

/**
 * Server-side logging for benchmark purposes.
 */
var benchLoggingClockChan chan bool

func (r *Replica) benchLoggingClock() {
	for !r.Shutdown {
		time.Sleep(BENCH_LOGGING_INTERVAL)
		benchLoggingClockChan <- true
	}
}

func (r *Replica) benchLoggingFlush() {
	// batch size logging
	if r.batchSizeLogFile != nil && len(r.batchSizeBuffer) > 0 {
		var bsBytes bytes.Buffer
		for _, batchSize := range r.batchSizeBuffer {
			bsBytes.WriteString(fmt.Sprintf("bs %d %d\n", r.Id, batchSize))
		}

		_, err := bsBytes.WriteTo(r.batchSizeLogFile)
		if err != nil {
			log.Fatal(err)
		}

		r.batchSizeBuffer = nil // clear the buffer
	}

	// timing breakdown logging
	if r.timeBreakLogFile != nil {
		var tbBytes bytes.Buffer
		for name, tsMap := range r.timeBreakBuffers {
			if len(tsMap) > 0 {
				for inst, ts := range tsMap {
					tbBytes.WriteString(fmt.Sprintf("tb %s %d %d %d\n", name, r.Id, inst, ts))
				}
			}
		}

		if tbBytes.Len() > 0 {
			_, err := tbBytes.WriteTo(r.timeBreakLogFile)
			if err != nil {
				log.Fatal(err)
			}
		}

		for name, _ := range r.timeBreakBuffers { // clear all buffers
			r.timeBreakBuffers[name] = make(map[int32]int64)
		}
	}
}

func (r *Replica) stampBatchSize(batchSize int) {
	if r.batchSizeLogFile != nil && batchSize > 0 {
		// fmt.Println("??? batched", batchSize)
		r.batchSizeBuffer = append(r.batchSizeBuffer, batchSize)
	}
}

func (r *Replica) stampTimeBreak(name string, inst int32) {
	if r.timeBreakLogFile != nil {
		if inst < 0 {
			log.Fatal("caught negative instance number")
		}

		now := time.Now()
		ts := int64(now.Sub(TIME_BREAK_BASE_DATE).Nanoseconds())

		if tsOld, ok := r.timeBreakBuffers[name][inst]; ok {
			// this timestamp is already taken for this instance
			r.timeBreakBuffers["IsAbnormal"][inst] = tsOld
		}
		r.timeBreakBuffers[name][inst] = ts
	}
}

/* Main event processing loop */

func (r *Replica) run() {

	r.ConnectToPeers()

	dlog.Println("Waiting for client connections")

	go r.WaitForClientConnections()

	if r.Exec {
		go r.executeCommands()
	}

	if r.Id == 0 {
		r.IsLeader = true
	}

	clockChan = make(chan bool, 1)
	go r.clock()

	benchLoggingClockChan = make(chan bool, 1)
	go r.benchLoggingClock()

	onOffProposeChan := r.ProposeChan

	var timer05ms *time.Timer
	var timer1ms *time.Timer
	var timer2ms *time.Timer
	var timer5ms *time.Timer
	var timer10ms *time.Timer
	var timer20ms *time.Timer
	var timer40ms *time.Timer
	var timer80ms *time.Timer
	if r.Id == 0 {
		timer05ms = time.NewTimer(48 * time.Second)
		timer1ms = time.NewTimer(49 * time.Second)
		timer2ms = time.NewTimer(50 * time.Second)
		timer5ms = time.NewTimer(51 * time.Second)
		timer10ms = time.NewTimer(52 * time.Second)
		timer20ms = time.NewTimer(53 * time.Second)
		timer40ms = time.NewTimer(54 * time.Second)
		timer80ms = time.NewTimer(55 * time.Second)
	}
	allFired := false

	for !r.Shutdown {

		if r.Id == 0 && INJECT_SLOWDOWN && !allFired {
			select {
			case <-timer05ms.C:
				fmt.Printf("Replica %v: Timer 0.5ms fired at %v\n", r.Id, time.Now())
				time.Sleep(500 * time.Microsecond)
				break

			case <-timer1ms.C:
				fmt.Printf("Replica %v: Timer 1ms fired at %v\n", r.Id, time.Now())
				time.Sleep(1 * time.Millisecond)
				break

			case <-timer2ms.C:
				fmt.Printf("Replica %v: Timer 2ms fired at %v\n", r.Id, time.Now())
				time.Sleep(2 * time.Millisecond)
				break

			case <-timer5ms.C:
				fmt.Printf("Replica %v: Timer 5ms fired at %v\n", r.Id, time.Now())
				time.Sleep(5 * time.Millisecond)
				break

			case <-timer10ms.C:
				fmt.Printf("Replica %v: Timer 10ms fired at %v\n", r.Id, time.Now())
				time.Sleep(10 * time.Millisecond)
				break

			case <-timer20ms.C:
				fmt.Printf("Replica %v: Timer 20ms fired at %v\n", r.Id, time.Now())
				time.Sleep(20 * time.Millisecond)
				break

			case <-timer40ms.C:
				fmt.Printf("Replica %v: Timer 40ms fired at %v\n", r.Id, time.Now())
				time.Sleep(40 * time.Millisecond)
				break

			case <-timer80ms.C:
				fmt.Printf("Replica %v: Timer 80ms fired at %v\n", r.Id, time.Now())
				allFired = true
				time.Sleep(80 * time.Millisecond)
				break
			default:
				break

			}
		}

		select {

		case <-clockChan:
			//activate the new proposals channel
			onOffProposeChan = r.ProposeChan
			break

		case <-benchLoggingClockChan:
			r.benchLoggingFlush()
			break

		case propose := <-onOffProposeChan:
			//got a Propose from a client
			dlog.Printf("Proposal with op %d\n", propose.Command.Op)
			r.handlePropose(propose)
			//deactivate the new proposals channel to prioritize the handling of protocol messages
			if MAX_BATCH > 100 {
				onOffProposeChan = nil
			}
			break

		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*paxosproto.Prepare)
			//got a Prepare message
			dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
			r.handlePrepare(prepare)
			break

		case acceptS := <-r.acceptChan:
			accept := acceptS.(*paxosproto.Accept)
			//got an Accept message
			dlog.Printf("Received Accept from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
			r.handleAccept(accept)
			break

		case commitS := <-r.commitChan:
			commit := commitS.(*paxosproto.Commit)
			//got a Commit message
			dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommit(commit)
			break

		case commitS := <-r.commitShortChan:
			commit := commitS.(*paxosproto.CommitShort)
			//got a Commit message
			dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommitShort(commit)
			break

		case prepareReplyS := <-r.prepareReplyChan:
			prepareReply := prepareReplyS.(*paxosproto.PrepareReply)
			//got a Prepare reply
			dlog.Printf("Received PrepareReply for instance %d\n", prepareReply.Instance)
			r.handlePrepareReply(prepareReply)
			break

		case acceptReplyS := <-r.acceptReplyChan:
			acceptReply := acceptReplyS.(*paxosproto.AcceptReply)
			//got an Accept reply
			dlog.Printf("Received AcceptReply for instance %d\n", acceptReply.Instance)
			r.handleAcceptReply(acceptReply)
			break

		/**
		 * ParamTweak:
		 */
		case ct := <-r.ParamTweakChan:
			r.HandleParamTweakFromClient(ct)
		}
	}
}

func (r *Replica) makeUniqueBallot(ballot int32) int32 {
	return (ballot << 4) | r.Id
}

func (r *Replica) updateCommittedUpTo() {
	for r.instanceSpace[r.committedUpTo+1] != nil &&
		r.instanceSpace[r.committedUpTo+1].status == COMMITTED {
		r.committedUpTo++
	}
}

func (r *Replica) bcastPrepare(instance int32, ballot int32, toInfinity bool) {
	r.stampTimeBreak("PrepareSend", instance)

	defer func() {
		if err := recover(); err != nil {
			log.Println("Prepare bcast failed:", err)
		}
	}()
	ti := FALSE
	if toInfinity {
		ti = TRUE
	}
	args := &paxosproto.Prepare{r.Id, instance, ballot, ti}

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}
	q := r.Id

	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] {
			continue
		}
		sent++
		r.SendMsg(q, r.prepareRPC, args)
	}
}

var pa paxosproto.Accept

func (r *Replica) bcastAccept(instance int32, ballot int32, command []state.Command) {
	r.stampTimeBreak("AcceptSend", instance)

	defer func() {
		if err := recover(); err != nil {
			log.Println("Accept bcast failed:", err)
		}
	}()
	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.Ballot = ballot
	pa.Command = command
	args := &pa
	//args := &paxosproto.Accept{r.Id, instance, ballot, command}

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}
	q := r.Id

	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] {
			continue
		}
		sent++
		r.SendMsg(q, r.acceptRPC, args)
	}
}

var pc paxosproto.Commit
var pcs paxosproto.CommitShort

func (r *Replica) bcastCommit(instance int32, ballot int32, command []state.Command) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Commit bcast failed:", err)
		}
	}()
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.Ballot = ballot
	pc.Command = command
	args := &pc
	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.Ballot = ballot
	pcs.Count = int32(len(command))
	argsShort := &pcs

	//args := &paxosproto.Commit{r.Id, instance, command}

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}
	q := r.Id
	sent := 0

	for sent < n {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] {
			continue
		}
		sent++
		r.SendMsg(q, r.commitShortRPC, argsShort)
	}
	if r.Thrifty && q != r.Id {
		for sent < r.N-1 {
			q = (q + 1) % int32(r.N)
			if q == r.Id {
				break
			}
			if !r.Alive[q] {
				continue
			}
			sent++
			r.SendMsg(q, r.commitRPC, args)
		}
	}
}

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	if !r.IsLeader {
		preply := &genericsmrproto.ProposeReplyTS{FALSE, -1, state.NIL, 0}
		r.ReplyProposeTS(preply, propose.Reply)
		return
	}

	for r.instanceSpace[r.crtInstance] != nil {
		r.crtInstance++
	}

	instNo := r.crtInstance
	r.crtInstance++

	batchSize := len(r.ProposeChan) + 1

	if batchSize > MAX_BATCH {
		batchSize = MAX_BATCH
	}

	dlog.Printf("Batched %d\n", batchSize)
	r.stampBatchSize(batchSize)
	r.stampTimeBreak("Propose", instNo)

	cmds := make([]state.Command, batchSize)
	proposals := make([]*genericsmr.Propose, batchSize)
	cmds[0] = propose.Command
	proposals[0] = propose

	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan
		cmds[i] = prop.Command
		proposals[i] = prop
	}

	if r.defaultBallot == -1 {
		r.instanceSpace[instNo] = &Instance{
			cmds,
			r.makeUniqueBallot(0),
			PREPARING,
			&LeaderBookkeeping{proposals, 0, 0, 0, 0, false}}
		r.bcastPrepare(instNo, r.makeUniqueBallot(0), true)
		dlog.Printf("Classic round for instance %d\n", instNo)
	} else {
		r.instanceSpace[instNo] = &Instance{
			cmds,
			r.defaultBallot,
			PREPARED,
			&LeaderBookkeeping{proposals, 0, 0, 0, 0, false}}

		r.bcastAccept(instNo, r.defaultBallot, cmds)
		dlog.Printf("Fast round for instance %d\n", instNo)

		// logging happens concurrently with follower Accepts
		r.recordInstanceMetadata(r.instanceSpace[instNo])
		r.recordCommands(cmds)
		r.sync()
		r.instanceSpace[instNo].lb.leaderLogged = true
	}
}

func (r *Replica) handlePrepare(prepare *paxosproto.Prepare) {
	r.stampTimeBreak("PrepareRecv", prepare.Instance)

	inst := r.instanceSpace[prepare.Instance]
	var preply *paxosproto.PrepareReply

	if inst == nil {
		ok := TRUE
		if r.defaultBallot > prepare.Ballot {
			ok = FALSE
		}
		preply = &paxosproto.PrepareReply{prepare.Instance, ok, r.defaultBallot, make([]state.Command, 0)}
	} else {
		ok := TRUE
		if prepare.Ballot < inst.ballot {
			ok = FALSE
		}
		preply = &paxosproto.PrepareReply{prepare.Instance, ok, inst.ballot, inst.cmds}
	}

	r.stampTimeBreak("PrepareReplySend", prepare.Instance)
	r.replyPrepare(prepare.LeaderId, preply)

	if prepare.ToInfinity == TRUE && prepare.Ballot > r.defaultBallot {
		r.defaultBallot = prepare.Ballot
	}
}

func (r *Replica) handleAccept(accept *paxosproto.Accept) {
	r.stampTimeBreak("AcceptRecv", accept.Instance)

	inst := r.instanceSpace[accept.Instance]
	var areply *paxosproto.AcceptReply

	if inst == nil {
		if accept.Ballot < r.defaultBallot {
			areply = &paxosproto.AcceptReply{accept.Instance, FALSE, r.defaultBallot}
		} else {
			r.instanceSpace[accept.Instance] = &Instance{
				accept.Command,
				accept.Ballot,
				ACCEPTED,
				nil}
			areply = &paxosproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}
		}
	} else if inst.ballot > accept.Ballot {
		areply = &paxosproto.AcceptReply{accept.Instance, FALSE, inst.ballot}
	} else if inst.ballot < accept.Ballot {
		inst.cmds = accept.Command
		inst.ballot = accept.Ballot
		inst.status = ACCEPTED
		areply = &paxosproto.AcceptReply{accept.Instance, TRUE, inst.ballot}
		if inst.lb != nil && inst.lb.clientProposals != nil {
			//TODO: is this correct?
			// try the proposal in a different instance
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	} else {
		// reordered ACCEPT
		r.instanceSpace[accept.Instance].cmds = accept.Command
		if r.instanceSpace[accept.Instance].status != COMMITTED {
			r.instanceSpace[accept.Instance].status = ACCEPTED
		}
		areply = &paxosproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}
	}

	if areply.OK == TRUE {
		r.recordInstanceMetadata(r.instanceSpace[accept.Instance])
		r.recordCommands(accept.Command)
		r.sync()
	}

	r.stampTimeBreak("AcceptReplySend", accept.Instance)
	r.replyAccept(accept.LeaderId, areply)
}

func (r *Replica) handleCommit(commit *paxosproto.Commit) {
	inst := r.instanceSpace[commit.Instance]

	dlog.Printf("Committing instance %d\n", commit.Instance)

	if inst == nil {
		r.instanceSpace[commit.Instance] = &Instance{
			commit.Command,
			commit.Ballot,
			COMMITTED,
			nil}
	} else {
		r.instanceSpace[commit.Instance].cmds = commit.Command
		r.instanceSpace[commit.Instance].status = COMMITTED
		r.instanceSpace[commit.Instance].ballot = commit.Ballot
		if inst.lb != nil && inst.lb.clientProposals != nil {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	}

	r.updateCommittedUpTo()

	r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
	r.recordCommands(commit.Command)
}

func (r *Replica) handleCommitShort(commit *paxosproto.CommitShort) {
	inst := r.instanceSpace[commit.Instance]

	dlog.Printf("Committing instance %d\n", commit.Instance)

	if inst == nil {
		r.instanceSpace[commit.Instance] = &Instance{nil,
			commit.Ballot,
			COMMITTED,
			nil}
	} else {
		r.instanceSpace[commit.Instance].status = COMMITTED
		r.instanceSpace[commit.Instance].ballot = commit.Ballot
		if inst.lb != nil && inst.lb.clientProposals != nil {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	}

	r.updateCommittedUpTo()

	r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
}

func (r *Replica) handlePrepareReply(preply *paxosproto.PrepareReply) {
	r.stampTimeBreak("PrepareReplyRecv", preply.Instance)

	inst := r.instanceSpace[preply.Instance]

	if inst.status != PREPARING {
		// TODO: should replies for non-current ballots be ignored?
		// we've moved on -- these are delayed replies, so just ignore
		return
	}

	if preply.OK == TRUE {
		inst.lb.prepareOKs++

		if preply.Ballot > inst.lb.maxRecvBallot {
			inst.cmds = preply.Command
			inst.lb.maxRecvBallot = preply.Ballot
			if inst.lb.clientProposals != nil {
				// there is already a competing command for this instance,
				// so we put the client proposal back in the queue so that
				// we know to try it in another instance
				for i := 0; i < len(inst.lb.clientProposals); i++ {
					r.ProposeChan <- inst.lb.clientProposals[i]
				}
				inst.lb.clientProposals = nil
			}
		}

		if inst.lb.prepareOKs+1 > r.N>>1 {
			inst.status = PREPARED
			inst.lb.nacks = 0
			if inst.ballot > r.defaultBallot {
				r.defaultBallot = inst.ballot
			}
			r.bcastAccept(preply.Instance, inst.ballot, inst.cmds)

			// logging happens concurrently with follower Accepts
			r.recordInstanceMetadata(r.instanceSpace[preply.Instance])
			r.sync()
			inst.lb.leaderLogged = true
		}
	} else {
		// TODO: there is probably another active leader
		inst.lb.nacks++
		if preply.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = preply.Ballot
		}
		if inst.lb.nacks >= r.N>>1 {
			if inst.lb.clientProposals != nil {
				// try the proposals in another instance
				for i := 0; i < len(inst.lb.clientProposals); i++ {
					r.ProposeChan <- inst.lb.clientProposals[i]
				}
				inst.lb.clientProposals = nil
			}
		}
	}
}

func (r *Replica) handleAcceptReply(areply *paxosproto.AcceptReply) {
	r.stampTimeBreak("AcceptReplyRecv", areply.Instance)

	inst := r.instanceSpace[areply.Instance]

	if inst.status != PREPARED && inst.status != ACCEPTED {
		// we've move on, these are delayed replies, so just ignore
		return
	}

	if areply.OK == TRUE {
		inst.lb.acceptOKs++
		if inst.lb.acceptOKs+1 > r.N>>1 {
			if !inst.lb.leaderLogged {
				log.Fatal("Error: leader should have finished logging for this instance")
			}

			inst = r.instanceSpace[areply.Instance]
			inst.status = COMMITTED
			if inst.lb.clientProposals != nil && !r.Dreply {
				// give client the all clear
				for i := 0; i < len(inst.cmds); i++ {
					propreply := &genericsmrproto.ProposeReplyTS{
						TRUE,
						inst.lb.clientProposals[i].CommandId,
						state.NIL,
						inst.lb.clientProposals[i].Timestamp}
					r.ReplyProposeTS(propreply, inst.lb.clientProposals[i].Reply)
				}
			}

			r.recordInstanceMetadata(r.instanceSpace[areply.Instance])
			r.sync() //is this necessary?

			r.updateCommittedUpTo()

			r.bcastCommit(areply.Instance, inst.ballot, inst.cmds)
		}
	} else {
		// TODO: there is probably another active leader
		inst.lb.nacks++
		if areply.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = areply.Ballot
		}
		if inst.lb.nacks >= r.N>>1 {
			// TODO
		}
	}
}

func (r *Replica) executeCommands() {

	var timer05ms *time.Timer
	var timer1ms *time.Timer
	var timer2ms *time.Timer
	var timer5ms *time.Timer
	var timer10ms *time.Timer
	var timer20ms *time.Timer
	var timer40ms *time.Timer
	var timer80ms *time.Timer
	if r.Id == 0 && INJECT_SLOWDOWN {
		timer05ms = time.NewTimer(48 * time.Second)
		timer1ms = time.NewTimer(49 * time.Second)
		timer2ms = time.NewTimer(50 * time.Second)
		timer5ms = time.NewTimer(51 * time.Second)
		timer10ms = time.NewTimer(52 * time.Second)
		timer20ms = time.NewTimer(53 * time.Second)
		timer40ms = time.NewTimer(54 * time.Second)
		timer80ms = time.NewTimer(55 * time.Second)
	}
	allFired := false

	i := int32(0)
	for !r.Shutdown {
		executed := false

		if r.Id == 0 && INJECT_SLOWDOWN && !allFired {
			select {
			case <-timer05ms.C:
				fmt.Printf("Replica %v: ExecTimer 0.5ms fired at %v\n", r.Id, time.Now())
				time.Sleep(500 * time.Microsecond)
				break

			case <-timer1ms.C:
				fmt.Printf("Replica %v: ExecTimer 1ms fired at %v\n", r.Id, time.Now())
				time.Sleep(1 * time.Millisecond)
				break

			case <-timer2ms.C:
				fmt.Printf("Replica %v: ExecTimer 2ms fired at %v\n", r.Id, time.Now())
				time.Sleep(2 * time.Millisecond)
				break

			case <-timer5ms.C:
				fmt.Printf("Replica %v: ExecTimer 5ms fired at %v\n", r.Id, time.Now())
				time.Sleep(5 * time.Millisecond)
				break

			case <-timer10ms.C:
				fmt.Printf("Replica %v: ExecTimer 10ms fired at %v\n", r.Id, time.Now())
				time.Sleep(10 * time.Millisecond)
				break

			case <-timer20ms.C:
				fmt.Printf("Replica %v: ExecTimer 20ms fired at %v\n", r.Id, time.Now())
				time.Sleep(20 * time.Millisecond)
				break

			case <-timer40ms.C:
				fmt.Printf("Replica %v: ExecTimer 40ms fired at %v\n", r.Id, time.Now())
				time.Sleep(40 * time.Millisecond)
				break

			case <-timer80ms.C:
				fmt.Printf("Replica %v: ExecTimer 80ms fired at %v\n", r.Id, time.Now())
				allFired = true
				time.Sleep(80 * time.Millisecond)
				break
			default:
				break

			}
		}

		for i <= r.committedUpTo {
			if r.instanceSpace[i].cmds != nil {
				r.stampTimeBreak("Execute", i)

				inst := r.instanceSpace[i]
				for j := 0; j < len(inst.cmds); j++ {
					val := inst.cmds[j].Execute(r.State)
					if r.Dreply && inst.lb != nil && inst.lb.clientProposals != nil {
						propreply := &genericsmrproto.ProposeReplyTS{
							TRUE,
							inst.lb.clientProposals[j].CommandId,
							val,
							inst.lb.clientProposals[j].Timestamp}

						r.stampTimeBreak("Acknowledge", i)
						r.ReplyProposeTS(propreply, inst.lb.clientProposals[j].Reply)
					}
				}
				i++
				executed = true
			} else {
				break
			}
		}

		if !executed {
			time.Sleep(1000)
		}
	}

}
