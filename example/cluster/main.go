package main

import (
	"context"
	"fmt"

	"github.com/YiCodes/go-network/cluster"
)

func testClose() {
	fmt.Println("close")
}

func main() {
	nodes := make([]*cluster.TCPServerInfo, 3)
	nodes[0] = &cluster.TCPServerInfo{ID: 1, Address: ":9000"}
	nodes[1] = &cluster.TCPServerInfo{ID: 2, Address: ":9001"}
	nodes[2] = &cluster.TCPServerInfo{ID: 3, Address: ":9002"}

	for _, n := range nodes {
		go func(n *cluster.TCPServerInfo) {
			s := cluster.NewTCPServer(n.ID, nodes, nil)
			s.Start()
		}(n)
	}

	<-context.Background().Done()
}
