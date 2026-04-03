package dag

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/iamitprakash/GMP-go/pkg/gmp"
)

// Node captures a single execution graph stage mathematically.
type Node struct {
	ID       string
	Fn       func(context.Context)
	
	indegree int32
	children []*Node
}

// Graph embeds Topologically Sorted Directed Acyclic routines naturally mapping to User-Space clusters.
type Graph struct {
	nodes map[string]*Node
	mu    sync.Mutex
}

func NewGraph() *Graph {
	return &Graph{
		nodes: make(map[string]*Node),
	}
}

// AddNode embeds logic payloads autonomously.
func (g *Graph) AddNode(id string, fn func(context.Context)) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.nodes[id]; !exists {
		g.nodes[id] = &Node{ID: id, Fn: fn}
	}
}

// AddEdge enforces chronological Execution mapping (from completing strictly before to). 
func (g *Graph) AddEdge(from, to string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	fromNode := g.nodes[from]
	toNode := g.nodes[to]
	
	if fromNode != nil && toNode != nil {
		fromNode.children = append(fromNode.children, toNode)
		atomic.AddInt32(&toNode.indegree, 1) // Atomically block resolution logic internally 
	}
}

// Execute parses the internal tree natively stripping roots with 0 inbound borders autonomously into Thread pools.
func (g *Graph) Execute(sched *gmp.Scheduler) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	for _, node := range g.nodes {
		if atomic.LoadInt32(&node.indegree) == 0 {
			g.dispatch(sched, node)
		}
	}
}

func (g *Graph) dispatch(sched *gmp.Scheduler, n *Node) {
	sched.SubmitPrio(gmp.PriorityNormal, false, func(ctx context.Context) {
		// Run core logic autonomously natively over an `M` thread 
		n.Fn(ctx)

		// Topologically unlock trailing sequence dependents seamlessly directly into Global Queue sequences
		for _, child := range n.children {
			remain := atomic.AddInt32(&child.indegree, -1)
			if remain == 0 {
				g.dispatch(sched, child)
			}
		}
	})
}
