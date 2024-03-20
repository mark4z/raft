package main

import (
	context "context"
	"fmt"
	"github.com/mark4z/raft/api"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"
)

type Core struct {
	term           int
	voted          int
	role           Role
	leader         int
	localPort      int
	peers          []int
	lastFromLeader time.Time
	voteFor        int
	peersClient    map[int]raft.CoreClient
	raft.UnimplementedCoreServer
}

func (c *Core) Start() {
	fmt.Println("Starting core on port", c.localPort, c.peers)
	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", c.localPort))
	if err != nil {
		panic(err)
	}
	go c.initClients()
	go c.serve()
	server := grpc.NewServer()
	raft.RegisterCoreServer(server, c)
	fmt.Println("Starting core on port", c.localPort)
	if err := server.Serve(listen); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (c *Core) initClients() {
	for i := range c.peers {
		conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", c.peers[i]), grpc.WithInsecure())
		if err != nil {
			panic(err)
		}
		c.peersClient[c.peers[i]] = raft.NewCoreClient(conn)
	}
}

type Role int

const (
	CANDIDATE Role = iota
	FOLLOWER
	LEADER
)

func NewCore(localPort int, peers []int) *Core {
	var newPeers []int
	for i := range peers {
		if peers[i] != localPort {
			newPeers = append(newPeers, peers[i])
		}
	}
	return &Core{
		term:        0,
		role:        CANDIDATE,
		localPort:   localPort,
		peers:       newPeers,
		peersClient: make(map[int]raft.CoreClient),
		voteFor:     localPort,
	}
}

func (c *Core) Vote(ctx context.Context, req *raft.VoteReq) (*raft.VoteRes, error) {
	log.Printf("%d Received [%d] vote request to %s", c.localPort, req.GetTerm(), req.GetVoteFor())
	if req.Term > int32(c.term) {
		c.term = int(req.Term) - 1
		c.voteFor, _ = strconv.Atoi(req.VoteFor)
		c.voted = 0
	} else if req.Term == int32(c.term) {
		if req.VoteFor == strconv.Itoa(c.localPort) {
			c.voted++
			if c.voted > len(c.peers)/2 {
				c.role = LEADER
			}
		}
	}
	return &raft.VoteRes{}, nil
}

func (c *Core) Heartbeat(ctx context.Context, req *raft.HeartbeatReq) (*raft.HeartbeatRes, error) {
	c.lastFromLeader = time.Now()
	if c.role == CANDIDATE {
		c.role = FOLLOWER
		c.leader, _ = strconv.Atoi(req.Name)
		c.lastFromLeader = time.Now()
		log.Printf("%d Following %d", c.localPort, c.leader)
	} else if c.role == LEADER {
		if req.Term > int32(c.term) {
			c.role = FOLLOWER
			c.leader, _ = strconv.Atoi(req.Name)
			c.term = int(req.Term)
			log.Printf("%d Following %d", c.localPort, c.leader)
		}
	}
	return &raft.HeartbeatRes{}, nil
}

func (c *Core) serve() {
	for {
		switch c.role {
		case FOLLOWER:
			if time.Since(c.lastFromLeader) > 3*time.Second {
				c.role = CANDIDATE
				c.voted = 0
				c.voteFor = c.localPort
				log.Printf("%d [%d]Becoming candidate", c.term, c.localPort)
			}
		case CANDIDATE:
			log.Printf("%d [%d]Starting election", c.term, c.localPort)
			time.Sleep(time.Duration(rand.Int()%1000) * time.Millisecond)
			c.term++
			if c.voteFor == c.localPort {
				c.voted = 1
			}
			log.Printf("%d [%d]Sending vote request to %d", c.localPort, c.term, c.voteFor)
			for i := range c.peersClient {
				_, err := c.peersClient[i].Vote(context.Background(), &raft.VoteReq{Term: int32(c.term), VoteFor: strconv.Itoa(c.voteFor)})
				if err != nil {
					log.Printf("%d Error sending vote [%d] request to %d", c.localPort, c.voteFor, i)
				}
			}
		case LEADER:
			log.Printf("%d [%d]Sending heartbeat to all", c.localPort, c.term)
			for i := range c.peersClient {
				_, err := c.peersClient[i].Heartbeat(context.Background(), &raft.HeartbeatReq{Term: int32(c.term), Name: strconv.Itoa(c.localPort)})
				if err != nil {
					log.Printf("[%d]Error sending heartbeat to %d", c.term, i)
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
}
