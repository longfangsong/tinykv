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
	// election interval
	thisTermElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	peers := make(map[uint64]*Progress)
	for _, peerId := range c.peers {
		if peerId != c.ID {
			peers[peerId] = &Progress{}
		}
	}
	return &Raft{
		id:                      c.ID,
		Term:                    0,
		Vote:                    0,
		RaftLog:                 newLog(c.Storage),
		Prs:                     peers,
		State:                   StateFollower,
		votes:                   make(map[uint64]bool),
		msgs:                    nil,
		Lead:                    0,
		heartbeatTimeout:        c.HeartbeatTick,
		electionTimeout:         c.ElectionTick,
		thisTermElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
		heartbeatElapsed:        0,
		electionElapsed:         0,
		leadTransferee:          0,
		PendingConfIndex:        0,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Entries: []*pb.Entry{
			{
				EntryType: pb.EntryType_EntryNormal,
				Term:      r.Term,
				Index:     0,
				Data:      nil,
			},
		},
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeat, From: r.id, To: to, Term: r.Term})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed == r.heartbeatTimeout {
			for i := range r.Prs {
				r.sendHeartbeat(i)
			}
		}
	} else {
		r.electionElapsed++
		if r.thisTermElectionTimeout == r.electionElapsed {
			r.becomeCandidate()

			for id := range r.Prs {
				term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
				r.msgs = append(r.msgs, pb.Message{
					Term:    r.Term,
					From:    r.id,
					To:      id,
					MsgType: pb.MessageType_MsgRequestVote,
					Index:   r.RaftLog.LastIndex(),
					LogTerm: term})
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.Vote = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term++
	r.votes[r.id] = true
	sayYesCount := 0
	for _, vote := range r.votes {
		if vote {
			sayYesCount++
		}
	}
	if sayYesCount > (len(r.Prs)+1)/2 {
		r.becomeLeader()
	}
	r.thisTermElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.electionElapsed = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	for i := range r.Prs {
		r.sendHeartbeat(i)
	}
}

func (r *Raft) canVote(m pb.Message) bool {
	voted := r.Vote != None
	votedToThisCandidate := r.Vote == m.From
	alreadyInALaterTerm := m.Term < r.Term
	lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	logNewerThanCandidate := lastTerm > m.LogTerm || (lastTerm == m.LogTerm && r.RaftLog.LastIndex() > m.Index)
	canVoteForThisCandidate := (!voted || votedToThisCandidate) &&
		!alreadyInALaterTerm &&
		!logNewerThanCandidate
	return canVoteForThisCandidate
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()

		for id := range r.Prs {
			term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
			r.msgs = append(r.msgs, pb.Message{
				Term:    r.Term,
				From:    r.id,
				To:      id,
				MsgType: pb.MessageType_MsgRequestVote,
				Index:   r.RaftLog.LastIndex(),
				LogTerm: term})
		}
	case pb.MessageType_MsgAppend:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, r.Lead)
		}
	case pb.MessageType_MsgRequestVote:
		canVote := r.canVote(m)
		if canVote {
			r.Vote = m.From
		}
		r.msgs = append(r.msgs, pb.Message{
			From:    m.To,
			To:      m.From,
			Term:    m.Term,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  !canVote})
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		canVote := r.canVote(m)
		if canVote {
			r.Vote = m.From
		}
		r.msgs = append(r.msgs, pb.Message{
			From:    m.To,
			To:      m.From,
			Term:    m.Term,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  !canVote})
	case pb.MessageType_MsgHup:
		r.becomeCandidate()

		for id := range r.Prs {
			term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
			r.msgs = append(r.msgs, pb.Message{
				Term:    r.Term,
				From:    r.id,
				To:      id,
				MsgType: pb.MessageType_MsgRequestVote,
				Index:   r.RaftLog.LastIndex(),
				LogTerm: term})
		}
	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
	case pb.MessageType_MsgRequestVoteResponse:
		r.votes[m.From] = !m.Reject
		sayYesCount := 0
		for _, vote := range r.votes {
			if vote {
				sayYesCount++
			}
		}
		if sayYesCount > (len(r.Prs)+1)/2 {
			r.becomeLeader()
		}
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		for i := range r.Prs {
			r.sendHeartbeat(i)
		}
	case pb.MessageType_MsgRequestVote:
		canVote := r.canVote(m)
		if canVote {
			r.Vote = m.From
		}
		r.msgs = append(r.msgs, pb.Message{
			From:    m.To,
			To:      m.From,
			Term:    m.Term,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  !canVote})
	case pb.MessageType_MsgBeat:
		for i := range r.Prs {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgHeartbeat,
				To:      i,
				From:    r.id,
				Term:    r.Term,
			})
		}
	}
	return nil
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return errors.New("invalid state")
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.RaftLog.committed = max(m.Commit, r.RaftLog.committed)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    m.Term,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
