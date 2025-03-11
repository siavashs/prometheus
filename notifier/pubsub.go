package notifier

import (
	"fmt"
	"sync"
)

type notification struct {
	alerts  []*Alert
	retries int
}

type pubSub struct {
	mtx      sync.Mutex
	capacity int
	subs     map[string]chan notification
	closed   bool
}

// newAgent creates a new Agent
func newPubSub(capacity int) *pubSub {
	return &pubSub{
		capacity: capacity,
		subs:     make(map[string]chan notification),
	}
}

// Publish publishes a message to a topic
func (p *pubSub) publish(n notification) (dropped map[string]int) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	dropped = make(map[string]int)

	if p.closed {
		return
	}

	for sub, ch := range p.subs {
		select {
		case ch <- n:
			continue
		default:
			// If the channel is full, drop the oldest message
			old := <-ch // Remove the oldest message
			dropped[sub] = len(old.alerts)
			ch <- n // Send new message
		}
	}
	return
}

// Publish publishes a message to a topic
func (p *pubSub) republish(n notification, sub string) (dropped int) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	dropped = 0

	if p.closed {
		return
	}

	if n.retries > 2 {
		// too many reties, drop
		dropped = len(n.alerts)
		return
	}
	n.retries++

	select {
	case p.subs[sub] <- n:
		fmt.Println("republished!")
		break
	default:
		// full queue, drop
		dropped = len(n.alerts)
	}

	return
}

// len returns the length of the subscriber subs
func (p *pubSub) len() (length map[string]int) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	length = make(map[string]int, len(p.subs))

	if p.closed {
		return
	}

	for sub := range p.subs {
		length[sub] = len(p.subs[sub])
	}

	return
}

// subscribe subscribes to a topic
func (p *pubSub) subscribe(sub string) chan notification {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if p.closed {
		return nil
	}

	ch := make(chan notification, p.capacity)
	p.subs[sub] = ch
	return ch
}

// subscribe subscribes to a topic
func (p *pubSub) unsubscribe(sub string) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if p.closed {
		return
	}

	close(p.subs[sub])
	delete(p.subs, sub)
}

// close closes the agent
func (p *pubSub) close() {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if p.closed {
		return
	}

	p.closed = true

	for _, ch := range p.subs {
		close(ch)
	}
}
