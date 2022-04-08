// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

type handler func(pb.Message)

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// handler map
	handlerMap map[pb.MessageType]handler
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, _ := c.Storage.InitialState()
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	prs := make(map[uint64]*Progress)
	for _, p := range c.peers {
		prs[p] = new(Progress)
	}
	raft := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              prs,
		State:            StateFollower,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		handlerMap:       make(map[pb.MessageType]handler),
	}
	raft.initHandlers()
	return raft
}

func (r *Raft) initHandlers() {
	r.handlerMap[pb.MessageType_MsgHup] = r.handleMsgUp
	r.handlerMap[pb.MessageType_MsgBeat] = r.handleMsgBeat
	r.handlerMap[pb.MessageType_MsgPropose] = r.handleMsgPropose
	r.handlerMap[pb.MessageType_MsgAppend] = r.handleMsgAppend
	r.handlerMap[pb.MessageType_MsgAppendResponse] = r.handleMsgAppendResponse
	r.handlerMap[pb.MessageType_MsgRequestVote] = r.handleMsgRequestVote
	r.handlerMap[pb.MessageType_MsgRequestVoteResponse] = r.handleMsgRequestVoteResponse
	r.handlerMap[pb.MessageType_MsgSnapshot] = r.handleMsgSnapshot
	r.handlerMap[pb.MessageType_MsgHeartbeat] = r.handleMsgHeartbeat
	r.handlerMap[pb.MessageType_MsgHeartbeatResponse] = r.handleMsgHeartbeatResponse
	r.handlerMap[pb.MessageType_MsgTransferLeader] = r.handleMsgTransferLeader
	r.handlerMap[pb.MessageType_MsgTimeoutNow] = r.handleMsgTimeoutNow
}

func (r *Raft) sendMsg(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

func (r *Raft) broadcast(f func(to uint64) bool) {
	for to, _ := range r.Prs {
		if to != r.id {
			f(to)
		}
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	index := r.Prs[to].Next - 1
	logTerm, err := r.RaftLog.Term(index)
	if err != nil || index+1 < r.RaftLog.firstIndex {
		r.sendSnapshot(to)
		return true
	}
	var entries []*pb.Entry
	for i := index + 1; i <= r.RaftLog.LastIndex(); i++ {
		entries = append(entries, &r.RaftLog.entries[i-r.RaftLog.firstIndex])
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		Index:   index,
		Term:    r.Term,
		LogTerm: logTerm,
		Entries: entries,
		Commit:  r.RaftLog.committed,
		From:    r.id,
		To:      to,
	}
	r.sendMsg(msg)
	return true
}

func (r *Raft) sendSnapshot(to uint64) bool {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return false
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		Term:     r.Term,
		Snapshot: &snapshot,
		From:     r.id,
		To:       to,
	}
	r.sendMsg(msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) bool {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		Term:    r.Term,
		Commit:  min(r.Prs[to].Match, r.RaftLog.committed),
		To:      to,
		From:    r.id,
	}
	r.sendMsg(msg)
	return true
}

func (r *Raft) sendRequestVote(to uint64) bool {
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		Term:    r.Term,
		Index:   lastLogIndex,
		LogTerm: lastLogTerm,
		To:      to,
		From:    r.id,
	}
	r.sendMsg(msg)
	return true
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
		}
	default:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.heartbeatElapsed = 0
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Lead = None
	r.votes = make(map[uint64]bool)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Term:    r.Term,
		From:    r.id,
		To:      r.id,
		Reject:  false,
	}
	r.Step(msg)
	r.heartbeatElapsed = 0
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.heartbeatElapsed = 0
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	for t, p := range r.Prs {
		if t != r.id {
			p.Match = 0
			p.Next = r.RaftLog.LastIndex() + 1
		} else {
			p.Match = r.RaftLog.LastIndex()
			p.Next = p.Match + 1
		}
	}
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{new(pb.Entry)},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term >= r.Term || IsLocalMsg(m.MsgType) || m.MsgType == pb.MessageType_MsgPropose {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handlerMap[m.MsgType](m)
	}
	return nil
}

func (r *Raft) handleMsgUp(m pb.Message) {
	switch r.State {
	case StateLeader:
	default:
		r.becomeCandidate()
		r.broadcast(r.sendRequestVote)
	}
}

func (r *Raft) handleMsgBeat(m pb.Message) {
	switch r.State {
	case StateFollower:
	case StateCandidate:
	case StateLeader:
		r.broadcast(r.sendHeartbeat)
	}
}

func (r *Raft) handleMsgPropose(m pb.Message) {
	switch r.State {
	case StateFollower:
		msg := pb.Message{
			MsgType: pb.MessageType_MsgPropose,
			Entries: m.Entries,
			From:    r.id,
			To:      r.Lead,
		}
		r.sendMsg(msg)
	case StateCandidate:
	case StateLeader:
		lastIndex := r.RaftLog.LastIndex()
		for i, entry := range m.Entries {
			entry.Term = r.Term
			entry.Index = lastIndex + uint64(i) + 1
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			Reject:  false,
			From:    r.id,
			To:      r.id,
		}
		r.Step(msg)
		r.broadcast(r.sendAppend)
	}
}

func (r *Raft) handleMsgAppend(m pb.Message) {
	r.handleAppendEntries(m)
}

func (r *Raft) handleMsgAppendResponse(m pb.Message) {
	switch r.State {
	case StateFollower:
	case StateCandidate:
		r.becomeFollower(m.Term, m.From)
	case StateLeader:
		if !m.Reject {
			r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
			r.Prs[m.From].Next = r.Prs[m.From].Match + 1
			matchTerm, _ := r.RaftLog.Term(m.Index)
			if m.Index > r.RaftLog.committed && matchTerm == r.Term {
				count := 0
				for _, p := range r.Prs {
					if p.Match >= m.Index {
						count++
					}
					if count > len(r.Prs)/2 {
						r.RaftLog.committed = m.Index
						r.broadcast(r.sendAppend)
						break
					}
				}
			}
		} else {
			r.Prs[m.From].Next = min(m.Index+1, r.Prs[m.From].Next-1)
			r.sendAppend(m.From)
		}
	}
}

func (r *Raft) handleMsgRequestVote(m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Term:    r.Term,
		Reject:  true,
		From:    r.id,
		To:      m.From,
	}
	switch r.State {
	case StateLeader:
	default:
		r.Lead = None
		t, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		if r.Vote == None || r.Vote == m.From {
			if m.LogTerm > t || m.LogTerm == t && m.Index >= r.RaftLog.LastIndex() {
				msg.Reject = false
				r.Vote = m.From
			}
		}
	}
	r.sendMsg(msg)
}

func (r *Raft) handleMsgRequestVoteResponse(m pb.Message) {
	switch r.State {
	case StateFollower:
	case StateCandidate:
		r.votes[m.From] = !m.Reject
		agree := 0
		disagree := 0
		for _, v := range r.votes {
			if v {
				agree++
				if agree > len(r.Prs)/2 {
					r.becomeLeader()
				}
			} else {
				disagree++
				if disagree > len(r.Prs)/2 {
					r.becomeFollower(r.Term, None)
				}
			}
		}
	case StateLeader:
	}
}

func (r *Raft) handleMsgSnapshot(m pb.Message) {
	r.handleSnapshot(m)
}

func (r *Raft) handleMsgHeartbeat(m pb.Message) {
	r.handleHeartbeat(m)
}

func (r *Raft) handleMsgHeartbeatResponse(m pb.Message) {
	switch r.State {
	case StateFollower:
	case StateCandidate:
	case StateLeader:
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}
}

func (r *Raft) handleMsgTransferLeader(m pb.Message) {
	switch r.State {
	case StateFollower:
	case StateCandidate:
	case StateLeader:
	}
}

func (r *Raft) handleMsgTimeoutNow(m pb.Message) {
	switch r.State {
	case StateFollower:
	case StateCandidate:
	case StateLeader:
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.Lead = m.From
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
		}
		prevLogTerm, err := r.RaftLog.Term(m.Index)
		if err != nil || prevLogTerm != m.LogTerm {
			msg.Reject = true
			msg.Index = r.RaftLog.LastIndex()
		} else {
			msg.Reject = false
			lastIndex := r.RaftLog.LastIndex()
			for _, entry := range m.Entries {
				if entry.Index <= lastIndex {
					term, _ := r.RaftLog.Term(entry.Index)
					if term != entry.Term {
						r.RaftLog.entries = r.RaftLog.entries[:entry.Index-r.RaftLog.firstIndex]
						r.RaftLog.entries = append(r.RaftLog.entries, *entry)
						lastIndex = r.RaftLog.LastIndex()
						r.RaftLog.stabled = m.Index
					}
				} else {
					r.RaftLog.entries = append(r.RaftLog.entries, *entry)
				}
			}
			if m.Commit > r.RaftLog.committed {
				committed := min(m.Commit, m.Index+uint64(len(m.Entries)))
				r.RaftLog.committed = min(committed, r.RaftLog.LastIndex())
			}
			msg.Index = m.Index + uint64(len(m.Entries))
		}
		r.sendMsg(msg)
	case StateCandidate:
		r.becomeFollower(m.Term, m.From)
	case StateLeader:
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.Lead = m.From
		r.RaftLog.committed = max(r.RaftLog.committed, m.Commit)
		r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
		msg := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			Term:    r.Term,
			To:      m.From,
			From:    r.id,
		}
		r.sendMsg(msg)
	case StateCandidate:
		r.becomeFollower(m.Term, m.From)
	case StateLeader:
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	switch r.State {
	case StateFollower:

	case StateCandidate:
		r.becomeFollower(m.Term, m.From)
	case StateLeader:
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
