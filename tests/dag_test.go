package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/iamitprakash/GMP-go/pkg/dag"
	"github.com/iamitprakash/GMP-go/pkg/gmp"
)

func TestDirectedAcyclicGraph(t *testing.T) {
	s := gmp.NewScheduler(2, 64)
	s.Start()
	defer s.Stop()

	graph := dag.NewGraph()
	var wg sync.WaitGroup

	var order []string
	var mu sync.Mutex

	record := func(id string) {
		mu.Lock()
		order = append(order, id)
		mu.Unlock()
		wg.Done()
	}

	wg.Add(4)
	graph.AddNode("A", func(ctx context.Context) { time.Sleep(10 * time.Millisecond); record("A") })
	graph.AddNode("B", func(ctx context.Context) { time.Sleep(5 * time.Millisecond); record("B") })
	graph.AddNode("C", func(ctx context.Context) { time.Sleep(10 * time.Millisecond); record("C") })
	graph.AddNode("D", func(ctx context.Context) { record("D") })

	// B and C run simultaneously after A resolves natively.
	// D runs strictly after B and C resolve fully!
	graph.AddEdge("A", "B")
	graph.AddEdge("A", "C")
	graph.AddEdge("B", "D")
	graph.AddEdge("C", "D")

	graph.Execute(s)
	wg.Wait()

	if order[0] != "A" {
		t.Errorf("Expected A first logically, got %s", order[0])
	}
	if order[3] != "D" {
		t.Errorf("Expected D mathematically last topologically, got %s", order[3])
	}
}
