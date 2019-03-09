package cluster

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type buffer struct {
	data []byte
}

func (b *buffer) EnsureSize(size int) {
	if cap(b.data) < size {
		b.data = make([]byte, size)
	}

	b.data = b.data[:size]
}

func (r *buffer) Read(b []byte) (int, error) {
	count := copy(b, r.data)
	return count, nil
}

type MessageCodec interface {
	Encode(o interface{}, data []byte) ([]byte, error)
	Decode(msgType int32, data []byte) (interface{}, error)
}

type defaultMessageCodec struct {
}

func (codec *defaultMessageCodec) Encode(o interface{}, data []byte) ([]byte, error) {
	return json.Marshal(o)
}

func (codec *defaultMessageCodec) Decode(msgType int32, data []byte) (interface{}, error) {
	switch msgType {
	case MessageTypeVote, MessageTypeVoteResponse:
		o := &VoteMessage{}
		err := json.Unmarshal(data, o)

		return o, err
	case MessageTypeFollow:
		o := &FollowMessage{}
		err := json.Unmarshal(data, o)

		return o, err
	case MessageTypeFollowResponse:
		o := &FollowResponseMessage{}
		err := json.Unmarshal(data, o)

		return o, err
	}

	return nil, fmt.Errorf("unknown message")
}

const (
	MessageTypeNone int32 = iota
	MessageTypeVote
	MessageTypeVoteResponse
	MessageTypeFollow
	MessageTypeFollowResponse
)

type TCPClient struct {
	Connection net.Conn
	State      NodeState
	ID         int
	Codec      MessageCodec
}

func DailTCP(address string, codec MessageCodec) (c *TCPClient, err error) {
	conn, err := net.Dial("tcp", address)

	if err != nil {
		return c, err
	}

	c = &TCPClient{Connection: conn, Codec: codec}

	return c, nil
}

func (c *TCPClient) Close() error {
	return c.Connection.Close()
}

func (c *TCPClient) SendMessage(msgType int32, o interface{}) error {
	data, err := c.Codec.Encode(o, nil)

	if err != nil {
		return err
	}

	err = binary.Write(c.Connection, binary.LittleEndian, msgType)

	if err != nil {
		return err
	}

	dataLen := len(data)
	err = binary.Write(c.Connection, binary.LittleEndian, int32(dataLen))

	if err != nil {
		return err
	}

	_, err = c.Connection.Write(data)

	return err
}

func (c *TCPClient) receive(conn net.Conn, buf []byte) error {
	len := len(buf)
	offset := 0

	for offset < len {
		readed, err := conn.Read(buf[offset:])

		if err != nil {
			return err
		}

		offset += readed
	}

	return nil
}

func (c *TCPClient) ReceiveMessage() (msgType int32, msg interface{}, err error) {
	buffer := buffer{}
	buffer.EnsureSize(4)

	err = c.receive(c.Connection, buffer.data)
	if err != nil {
		return msgType, msg, err
	}
	binary.Read(&buffer, binary.LittleEndian, &msgType)

	err = c.receive(c.Connection, buffer.data)
	if err != nil {
		return msgType, msg, err
	}
	var msgLen int32
	binary.Read(&buffer, binary.LittleEndian, &msgLen)

	buffer.EnsureSize(int(msgLen))
	err = c.receive(c.Connection, buffer.data)

	if err != nil {
		return msgType, msg, err
	}

	msg, err = c.Codec.Decode(msgType, buffer.data)

	return msgType, msg, err
}

type VoteMessage struct {
	ID      int
	Choosen int
}

type FollowMessage struct {
	ID int
}

type FollowResponseMessage struct {
	OK bool
}

type TcpServerMode interface {
	Mode() NodeState
	Run() error
}

type TCPServerInfo struct {
	ID      int
	Address string
}

type TCPServer struct {
	codec    MessageCodec
	mode     TcpServerMode
	info     *TCPServerInfo
	listener net.Listener
	nodeMap  map[int]*TCPServerInfo

	acceptedClients chan *TCPClient

	lock sync.Mutex

	voter    *Voter
	leader   *Leader
	follower *Follower
}

func NewTCPServer(selfID int, nodes []*TCPServerInfo, codec MessageCodec) *TCPServer {
	s := &TCPServer{}

	nodeIDs := make([]int, len(nodes))
	s.nodeMap = make(map[int]*TCPServerInfo)
	s.acceptedClients = make(chan *TCPClient)

	for i, n := range nodes {
		s.nodeMap[n.ID] = n
		nodeIDs[i] = n.ID
	}

	s.info = s.nodeMap[selfID]

	if codec == nil {
		codec = &defaultMessageCodec{}
	}
	s.codec = codec

	s.voter = NewVoter(s, nodeIDs)
	s.leader = NewLeader(s, int32(len(s.nodeMap)))
	s.follower = NewFollower(s)

	return s
}

func (s *TCPServer) SelfInfo() *TCPServerInfo {
	return s.info
}

func (s *TCPServer) NodeMap() map[int]*TCPServerInfo {
	return s.nodeMap
}

func (s *TCPServer) ChangeMode(mode NodeState) {
	switch mode {
	case VoterState:
		s.mode = s.voter
	case LeaderState:
		s.mode = s.leader
	case FollowerState:
		s.mode = s.follower
	}
}

func (s *TCPServer) AcceptedClients() <-chan *TCPClient {
	return s.acceptedClients
}

func (s *TCPServer) Start() error {
	listener, err := net.Listen("tcp", s.info.Address)

	if err != nil {
		return err
	}
	defer listener.Close()

	s.listener = listener

	go func() {
		for {
			conn, err := listener.Accept()

			if err != nil {
				return
			}

			s.acceptedClients <- &TCPClient{Connection: conn, State: VoterState, Codec: s.codec}
		}
	}()

	s.ChangeMode(VoterState)

	for {
		a := s.mode
		if err = a.Run(); err != nil {
			return err
		}
	}

	return nil
}

type Voter struct {
	server *TCPServer

	election *LeaderElection
}

func NewVoter(server *TCPServer, nodeIDs []int) *Voter {
	v := &Voter{server: server}
	v.election = NewLeaderElection(server.info.ID, nodeIDs)

	return v
}

func (v *Voter) Mode() NodeState {
	return VoterState
}

func (v *Voter) listen(ctx context.Context, lock *sync.Mutex) <-chan struct{} {
	listenDone := make(chan struct{})

	go func() {
		clients := sync.Map{}
		server := v.server

		accepted := server.AcceptedClients()

		for {
			var client *TCPClient
			select {
			case <-ctx.Done():
				goto Clean
			case client = <-accepted:
				clients.Store(client, client)
			}

			go func(c *TCPClient) {
				defer func() {
					c.Close()
					clients.Delete(c)
				}()

				for {
					msgType, _, err := c.ReceiveMessage()

					if err != nil {
						return
					}

					switch msgType {
					case MessageTypeVote:
						lock.Lock()
						choosen := v.election.Choosen(v.election.SelfID())
						lock.Unlock()

						c.SendMessage(MessageTypeVoteResponse, &VoteMessage{ID: v.election.SelfID(), Choosen: choosen})
					case MessageTypeFollow:
						c.SendMessage(MessageTypeFollowResponse, &FollowResponseMessage{OK: false})
					default:
						return
					}
				}
			}(client)
		}

	Clean:
		clients.Range(func(k, v interface{}) bool {
			(k.(*TCPClient)).Close()
			return true
		})

		close(listenDone)
	}()

	return listenDone
}

func (v *Voter) talk(ctx context.Context, lock *sync.Mutex, duration time.Duration) {
	clients := sync.Map{}
	openingNodes := make(chan *TCPServerInfo, len(v.server.NodeMap()))

	f := func(client *TCPClient, info *TCPServerInfo) {
		defer func() {
			client.Close()
			clients.Delete(client)

			openingNodes <- info
		}()

		for {
			lock.Lock()
			choosen := v.election.Choosen(v.election.SelfID())
			lock.Unlock()

			err := client.SendMessage(MessageTypeVote,
				&VoteMessage{
					ID:      v.election.SelfID(),
					Choosen: choosen})

			if err != nil {
				return
			}

			_, msg, err := client.ReceiveMessage()

			if err != nil {
				return
			}

			res := msg.(*VoteMessage)

			lock.Lock()
			v.election.OtherVote(res.ID, res.Choosen)
			lock.Unlock()

			select {
			case <-ctx.Done():
				return
			case <-time.After(duration):
			}
		}
	}

	for _, n := range v.server.NodeMap() {
		if n.ID == v.server.info.ID {
			continue
		}

		openingNodes <- n
	}

	for {
		select {
		case <-ctx.Done():
			goto Clean
		case n := <-openingNodes:
			client, err := DailTCP(n.Address, v.server.codec)

			if err == nil {
				clients.Store(client, client)
				go f(client, n)
			} else {
				time.Sleep(time.Microsecond * 50)
				openingNodes <- n
			}
		}
	}

Clean:
	clients.Range(func(k, v interface{}) bool {
		(k.(*TCPClient)).Close()
		return true
	})
}

func (v *Voter) Run() error {
	fmt.Println("voter")
	ctx, cancel := context.WithCancel(context.Background())
	v.election.Clear()
	v.election.Vote()

	lock := sync.Mutex{}

	listenDone := v.listen(ctx, &lock)
	go v.talk(ctx, &lock, time.Microsecond*150)

	for {
		time.Sleep(time.Millisecond * 100)

		lock.Lock()

		v.election.Vote()
		if v.election.CheckChanged() {
			lock.Unlock()
			break
		}

		lock.Unlock()
	}

	cancel()
	<-listenDone

	lock.Lock()

	server := v.server
	if v.election.state == LeaderState {
		server.ChangeMode(LeaderState)
	} else if v.election.state == FollowerState {
		server.follower.SetLeaderInfo(server.nodeMap[v.election.LeaderID()])
		server.ChangeMode(FollowerState)
	}

	lock.Unlock()

	return nil
}

type Leader struct {
	server        *TCPServer
	clientMap     sync.Map
	clientMapByID sync.Map
	onlineCount   int32
	nodeCount     int32

	msgHandleFunc func(client *TCPClient, msgType int, msg interface{})
}

func NewLeader(server *TCPServer, nodeCount int32) *Leader {
	l := new(Leader)
	l.server = server
	l.nodeCount = nodeCount

	return l
}

func (l *Leader) IsLossManyFollowers() bool {
	return l.nodeCount/2 >= l.onlineCount+1
}

func (l *Leader) AddClient(client *TCPClient) {
	atomic.AddInt32(&l.onlineCount, 1)
	l.clientMap.Store(client, 0)
}

func (l *Leader) SetFollower(client *TCPClient, nodeID int) {
	l.clientMapByID.Store(nodeID, client)
}

func (l *Leader) CloseAndRemoveClient(client *TCPClient) {
	atomic.AddInt32(&l.onlineCount, -1)
	client.Close()

	if v, ok := l.clientMap.Load(client); ok {
		id := v.(int)
		l.clientMapByID.Delete(id)
		l.clientMap.Delete(client)
	}
}

func (l *Leader) CloseClients() {
	l.clientMap.Range(func(k, v interface{}) bool {
		(k.(*TCPClient)).Close()
		return true
	})
}

func (l *Leader) Mode() NodeState {
	return LeaderState
}

func (l *Leader) listen(ctx context.Context) <-chan struct{} {
	listenDone := make(chan struct{})

	go func() {
		server := l.server
		accepted := server.AcceptedClients()

		for {
			var client *TCPClient
			select {
			case <-ctx.Done():
				goto Clean
			case client = <-accepted:
				l.AddClient(client)
			}

			go func(c *TCPClient) {
				defer l.CloseAndRemoveClient(c)

				for {
					msgType, msg, err := c.ReceiveMessage()

					if err != nil {
						return
					}

					switch msgType {
					case MessageTypeVote:
						c.SendMessage(MessageTypeVoteResponse, &VoteMessage{ID: server.info.ID, Choosen: server.info.ID})
					case MessageTypeFollow:
						l.SetFollower(c, (msg.(*FollowMessage)).ID)
						c.SendMessage(MessageTypeFollowResponse, &FollowResponseMessage{OK: true})
					default:
						return
					}
				}
			}(client)
		}

	Clean:
		l.CloseClients()

		close(listenDone)
	}()

	return listenDone
}

func (l *Leader) Run() error {
	fmt.Println("leader")

	server := l.server

	ctx, cancel := context.WithCancel(context.Background())

	listenDone := l.listen(ctx)

	for {
		time.Sleep(time.Second * 1)

		if l.IsLossManyFollowers() {
			break
		}
	}

	cancel()
	<-listenDone

	l.CloseClients()

	server.ChangeMode(VoterState)

	return nil
}

type Follower struct {
	server     *TCPServer
	leaderInfo TCPServerInfo
	client     *TCPClient
	handleFunc func(msgType int32, msg interface{})
}

func NewFollower(server *TCPServer) *Follower {
	f := new(Follower)
	f.server = server

	return f
}

func (f *Follower) SetLeaderInfo(leaderInfo *TCPServerInfo) {
	f.leaderInfo = *leaderInfo
}

func (f *Follower) Mode() NodeState {
	return FollowerState
}

func (f *Follower) ConnectLeader() (*TCPClient, bool) {
	c, err := DailTCP(f.leaderInfo.Address, f.server.codec)

	if err != nil {
		return nil, false
	}

	c.SendMessage(MessageTypeFollow, FollowMessage{ID: f.server.info.ID})

	msgType, msg, err := c.ReceiveMessage()

	if err != nil && msgType != MessageTypeFollowResponse {
		c.Close()
		return nil, false
	}

	res := msg.(*FollowResponseMessage)

	if !res.OK {
		c.Close()
		return nil, false
	}

	return c, true
}

func (f *Follower) listen(ctx context.Context) <-chan struct{} {
	listenDone := make(chan struct{})

	go func() {
		clients := sync.Map{}
		server := f.server

		accepted := server.AcceptedClients()

		for {
			var client *TCPClient
			select {
			case <-ctx.Done():
				goto Clean
			case client = <-accepted:
				clients.Store(client, client)
			}

			go func(c *TCPClient) {
				defer func() {
					c.Close()
					clients.Delete(c)
				}()

				for {
					msgType, _, err := c.ReceiveMessage()

					if err != nil {
						return
					}

					switch msgType {
					case MessageTypeVote:
						c.SendMessage(MessageTypeVoteResponse, &VoteMessage{
							ID: f.server.info.ID, Choosen: f.leaderInfo.ID})
					case MessageTypeFollow:
						c.SendMessage(MessageTypeFollowResponse, &FollowResponseMessage{OK: false})
					default:
						return
					}
				}
			}(client)
		}

	Clean:
		clients.Range(func(k, v interface{}) bool {
			(k.(*TCPClient)).Close()
			return true
		})

		close(listenDone)
	}()

	return listenDone
}

func (f *Follower) Run() error {
	ctx, cancel := context.WithCancel(context.Background())

	listenDone := f.listen(ctx)

	failed := 0
	for {
		if failed >= 3 {
			break
		}

		leader, ok := f.ConnectLeader()
		f.client = leader

		if !ok {
			failed++
			time.Sleep(time.Millisecond * 150)
			continue
		}
		failed = 0

		for msgType, msg, err := leader.ReceiveMessage(); err == nil; {
			f.handleFunc(msgType, msg)
		}
		leader.Close()
	}

	cancel()
	<-listenDone

	f.server.ChangeMode(VoterState)

	return nil
}
