package cluster

import (
	"fmt"
	"math"
)

type NodeState int

const (
	VoterState NodeState = iota
	LeaderState
	FollowerState
)

type Node struct {
	ID      int
	Choosen int

	Online      bool
	Attachement interface{}
}

type LeaderElection struct {
	selfID   int
	leaderID int
	state    NodeState

	nodes map[int]*Node
}

func NewLeaderElection(selfID int, nodeIDs []int) *LeaderElection {
	n := &LeaderElection{selfID: selfID, state: VoterState}
	n.nodes = make(map[int]*Node)

	for _, id := range nodeIDs {
		n.nodes[id] = &Node{ID: id}
	}

	if _, ok := n.nodes[selfID]; !ok {
		panic(fmt.Errorf("selfID is not in nodeIDs"))
	}

	n.Vote()

	return n
}

func (n *LeaderElection) electLeader() int {
	leaderID := math.MaxInt32

	for id, node := range n.nodes {
		if node.Online {
			if id < leaderID {
				leaderID = id
			}
		}
	}

	return leaderID
}

func (n *LeaderElection) SelfID() int {
	return n.selfID
}

func (n *LeaderElection) LeaderID() int {
	return n.leaderID
}

func (n *LeaderElection) Clear() {
	for _, n := range n.nodes {
		n.Online = false
	}
}

func (n *LeaderElection) Exit(nodeID int) {
	node := n.nodes[nodeID]
	node.Online = false
	node.Choosen = 0
}

func (n *LeaderElection) Choosen(nodeID int) int {
	return n.nodes[nodeID].Choosen
}

func (n *LeaderElection) Vote() int {
	node := n.nodes[n.selfID]
	node.Online = true
	node.Choosen = n.electLeader()

	return node.Choosen
}

func (n *LeaderElection) OtherVote(nodeID int, choosen int) {
	node := n.nodes[nodeID]
	node.Online = true
	node.Choosen = choosen
}

func (n *LeaderElection) CheckChanged() bool {
	switch n.state {
	case FollowerState:
		if !n.nodes[n.leaderID].Online {
			n.state = VoterState
			return true
		}
	case LeaderState:
		half := len(n.nodes) / 2

		onlineCount := 0

		for _, node := range n.nodes {
			if node.Online {
				onlineCount++
			}
		}

		if onlineCount < half {
			n.state = VoterState
			return true
		}
	case VoterState:
		half := len(n.nodes) / 2

		votes := make(map[int]int)

		for _, node := range n.nodes {
			if node.Online {
				voteCount := votes[node.Choosen]
				voteCount++

				votes[node.Choosen] = voteCount
			}
		}

		for nodeID, v := range votes {
			if v > half {
				n.leaderID = nodeID
				if nodeID == n.selfID {
					n.state = LeaderState
				} else {
					n.state = FollowerState
				}
				return true
			}
		}
	}

	return false
}
