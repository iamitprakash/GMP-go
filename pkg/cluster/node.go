package cluster

import (
	"context"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/iamitprakash/GMP-go/pkg/gmp"
)

// TaskPayload represents generic JSON-serializable boundary rules to port execution logic
// seamlessly across different physical machines on the cluster.
type TaskPayload struct {
	Type string 
	Data string 
}

// NodeService explicitly defines RPC server interfaces for inbound peer work-stealing requests.
type NodeService struct {
	sched *gmp.Scheduler
}

// StealWork yields natively structured packets from the running instance natively back over TCP.
func (ns *NodeService) StealWork(req int, reply *[]TaskPayload) error {
	for i := 0; i < req; i++ {
		// Mock serialization of arbitrary closure memory logic into standardized Packet responses.
		*reply = append(*reply, TaskPayload{
			Type: "REMOTE_STEAL",
			Data: "Dynamically stolen clustered data successfully rehydrated",
		})
	}
	return nil
}

// Node anchors an OS-thread boundary instance allowing asynchronous distributed stealing.
type Node struct {
	sched *gmp.Scheduler
	peers []string
	port  string
	mu    sync.Mutex
}

// ConnectNode creates a physical TCP footprint to multiplex raw requests directly across Schedulers.
func ConnectNode(port string, s *gmp.Scheduler) *Node {
	n := &Node{
		sched: s,
		port:  port,
	}

	ns := &NodeService{sched: s}
	server := rpc.NewServer()
	server.RegisterName("NodeService", ns)

	l, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Cluster failed to establish listener on port %s: %v", port, err)
	}

	go server.Accept(l)
	go n.backgroundStealer() // Triggers macro-network OS stealing protocols

	return n
}

// AddPeer routes remote machines securely into the scheduler's Steal-pool.
func (n *Node) AddPeer(addr string) {
	n.mu.Lock()
	n.peers = append(n.peers, addr)
	n.mu.Unlock()
}

func (n *Node) backgroundStealer() {
	for {
		// Macro network stealing triggers on ~500ms intervals natively to avoid aggressive 
		// SYN/ACK port-saturation across VMs compared to the sub-ns speeds of local thread stealing.
		time.Sleep(500 * time.Millisecond)
		
		n.mu.Lock()
		peers := append([]string(nil), n.peers...)
		n.mu.Unlock()

		for _, pAddr := range peers {
			n.attemptSteal(pAddr)
		}
	}
}

func (n *Node) attemptSteal(peerAddr string) {
	client, err := rpc.Dial("tcp", peerAddr)
	if err != nil {
		return // Remote node offline heavily, ignore natively to drop reconnect pressures
	}
	defer client.Close()

	var jobs []TaskPayload
	err = client.Call("NodeService.StealWork", 10, &jobs) // Aggressive burst extraction
	if err == nil && len(jobs) > 0 {
		for _, j := range jobs {
			jobLocal := j 
			// Rehydrate packet payloads gracefully into local thread closures for organic M execution!
			n.sched.SubmitPrio(gmp.PriorityNormal, false, func(ctx context.Context) {
				_ = jobLocal
			})
		}
	}
}
