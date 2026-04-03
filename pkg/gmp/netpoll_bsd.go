//go:build darwin || freebsd
// +build darwin freebsd

package gmp

import (
	"context"
	"sync"
	"syscall"
)

// HardwareNetpoller handles physical OS interrupts avoiding OS thread blocking.
type HardwareNetpoller struct {
	sched *Scheduler
	kq    int
	mu    sync.Mutex
	cbs   map[int]func() // Maps raw FD -> Closure Callback
}

// NewHardwareNetpoller securely bridges physical POSIX interrupts to the GMP Priority system.
func NewHardwareNetpoller(s *Scheduler) (*HardwareNetpoller, error) {
	kq, err := syscall.Kqueue()
	if err != nil {
		return nil, err
	}

	hp := &HardwareNetpoller{
		sched: s,
		kq:    kq,
		cbs:   make(map[int]func()),
	}

	go hp.pollLoop()

	return hp, nil
}

// Watch registers a physical File Descriptor (like a TCP socket) to trigger natively.
func (hp *HardwareNetpoller) Watch(fd int, cb func()) error {
	hp.mu.Lock()
	hp.cbs[fd] = cb
	hp.mu.Unlock()

	// Hardware registration: EVFILT_READ triggers when buffer has data. Use EV_ONESHOT to prevent duplicate wakes.
	change := syscall.Kevent_t{
		Ident:  uint64(fd),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD | syscall.EV_ENABLE | syscall.EV_ONESHOT,
	}

	_, err := syscall.Kevent(hp.kq, []syscall.Kevent_t{change}, nil, nil)
	return err
}

func (hp *HardwareNetpoller) pollLoop() {
	events := make([]syscall.Kevent_t, 32)
	for {
		n, err := syscall.Kevent(hp.kq, nil, events, nil)
		if err != nil {
			if err == syscall.EINTR {
				continue // Ignore interruption signals internally
			}
			return
		}

		hp.mu.Lock()
		for i := 0; i < n; i++ {
			fd := int(events[i].Ident)
			if cb, exists := hp.cbs[fd]; exists {
				// Delete because ONESHOT necessitates re-registration on execution.
				delete(hp.cbs, fd)
				
				// Route physical hardware interrupt explicitly onto highest priority GMP system!
				hp.sched.SubmitPrio(PriorityHigh, false, func(ctx context.Context) {
					cb()
				})
			}
		}
		hp.mu.Unlock()
	}
}
