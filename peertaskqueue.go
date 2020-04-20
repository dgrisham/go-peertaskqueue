package peertaskqueue

import (
	"fmt"
	"sync"

	pq "github.com/ipfs/go-ipfs-pq"
	"github.com/ipfs/go-peertaskqueue/peertask"
	"github.com/ipfs/go-peertaskqueue/peertracker"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type peerTaskQueueEvent int

const (
	peerAdded   = peerTaskQueueEvent(1)
	peerRemoved = peerTaskQueueEvent(2)
	roundSize   = 10000 // total data to distribute among peers per round
)

type hookFunc func(p peer.ID, event peerTaskQueueEvent)

// PeerTaskQueue is a prioritized list of tasks to be executed on peers.
// Tasks are added to the queue, then popped off alternately between peers (roughly)
// to execute the block with the highest priority, or otherwise the one added
// first if priorities are equal.
type PeerTaskQueue struct {
	lock           sync.Mutex
	pQueue         pq.PQ
	peerTrackers   map[peer.ID]*peertracker.PeerTracker
	frozenPeers    map[peer.ID]struct{}
	hooks          []hookFunc
	ignoreFreezing bool
	taskMerger     peertracker.TaskMerger
	currentQueue   int // which queue we're currently serving (0 <= currentQueue < numLanes)
}

// Option is a function that configures the peer task queue
type Option func(*PeerTaskQueue) Option

func chain(firstOption Option, secondOption Option) Option {
	return func(ptq *PeerTaskQueue) Option {
		firstReverse := firstOption(ptq)
		secondReverse := secondOption(ptq)
		return chain(secondReverse, firstReverse)
	}
}

// IgnoreFreezing is an option that can make the task queue ignore freezing and unfreezing
func IgnoreFreezing(ignoreFreezing bool) Option {
	return func(ptq *PeerTaskQueue) Option {
		previous := ptq.ignoreFreezing
		ptq.ignoreFreezing = ignoreFreezing
		return IgnoreFreezing(previous)
	}
}

// TaskMerger is an option that specifies merge behaviour when pushing a task
// with the same Topic as an existing Topic.
func TaskMerger(tmfp peertracker.TaskMerger) Option {
	return func(ptq *PeerTaskQueue) Option {
		previous := ptq.taskMerger
		ptq.taskMerger = tmfp
		return TaskMerger(previous)
	}
}

func removeHook(hook hookFunc) Option {
	return func(ptq *PeerTaskQueue) Option {
		for i, testHook := range ptq.hooks {
			if &hook == &testHook {
				ptq.hooks = append(ptq.hooks[:i], ptq.hooks[i+1:]...)
				break
			}
		}
		return addHook(hook)
	}
}

func addHook(hook hookFunc) Option {
	return func(ptq *PeerTaskQueue) Option {
		ptq.hooks = append(ptq.hooks, hook)
		return removeHook(hook)
	}
}

// OnPeerAddedHook adds a hook function that gets called whenever the ptq adds a new peer
func OnPeerAddedHook(onPeerAddedHook func(p peer.ID)) Option {
	hook := func(p peer.ID, event peerTaskQueueEvent) {
		if event == peerAdded {
			onPeerAddedHook(p)
		}
	}
	return addHook(hook)
}

// OnPeerRemovedHook adds a hook function that gets called whenever the ptq adds a new peer
func OnPeerRemovedHook(onPeerRemovedHook func(p peer.ID)) Option {
	hook := func(p peer.ID, event peerTaskQueueEvent) {
		if event == peerRemoved {
			onPeerRemovedHook(p)
		}
	}
	return addHook(hook)
}

// New creates a new PeerTaskQueue
func New(options ...Option) *PeerTaskQueue {
	ptq := &PeerTaskQueue{
		peerTrackers: make(map[peer.ID]*peertracker.PeerTracker),
		frozenPeers:  make(map[peer.ID]struct{}),
		taskMerger:   &peertracker.DefaultTaskMerger{},
	}

	ptq.pQueue = pq.New(peertracker.PeerCompare)
	ptq.Options(options...)
	return ptq
}

// Options uses configuration functions to configure the peer task queue.
// It returns an Option that can be called to reverse the changes.
func (ptq *PeerTaskQueue) Options(options ...Option) Option {
	if len(options) == 0 {
		return nil
	}
	if len(options) == 1 {
		return options[0](ptq)
	}
	reverse := options[0](ptq)
	return chain(ptq.Options(options[1:]...), reverse)
}

func (ptq *PeerTaskQueue) callHooks(to peer.ID, event peerTaskQueueEvent) {
	for _, hook := range ptq.hooks {
		hook(to, event)
	}
}

func (ptq *PeerTaskQueue) SetWeight(id peer.ID, weight int) error {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()

	tracker, ok := ptq.peerTrackers[id]
	if !ok {
		return fmt.Errorf("Peer with id %s not found", id)
	}

	tracker.SetWeight(weight)
	return nil
}

// PushTasks adds a new group of tasks for the given peer to the queue
func (ptq *PeerTaskQueue) PushTasks(to peer.ID, tasks ...peertask.Task) {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()

	peerTracker, ok := ptq.peerTrackers[to]
	if !ok {
		peerTracker = peertracker.New(to, ptq.taskMerger)
		// ptq.pQueues[0].Push(peerTracker) // default to lowest priority until SetPriority is called for this peer
		ptq.peerTrackers[to] = peerTracker
		ptq.callHooks(to, peerAdded)
	}

	peerTracker.PushTasks(tasks...)
}

func (ptq *PeerTaskQueue) newRound() {
	totalWeight := 0
	for _, tracker := range ptq.peerTrackers {
		totalWeight += tracker.Weight()
	}

	for _, tracker := range ptq.peerTrackers {
		tracker.SetRemainingData((roundSize * tracker.Weight()) / totalWeight)
		ptq.pQueue.Push(tracker)
	}
}

// PopTasks finds the peer with the highest priority and pops as many tasks
// off the peer's queue as necessary to cover targetMinWork, in priority order.
// If there are not enough tasks to cover targetMinWork it just returns
// whatever is in the peer's queue.
// - Peers with the most "active" work are deprioritized.
//   This heuristic is for fairness, we try to keep all peers "busy".
// - Peers with the most "pending" work are prioritized.
//   This heuristic is so that peers with a lot to do get asked for work first.
// The third response argument is pending work: the amount of work in the
// queue for this peer.
func (ptq *PeerTaskQueue) PopTasks(targetMinWork int) (peer.ID, []*peertask.Task, int) {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()

	if ptq.pQueue.Len() == 0 {
		// possible that the round has been exhausted, reset
		ptq.newRound()
		if ptq.pQueue.Len() == 0 { // queue still empty, return
			return "", nil, -1
		}
	}

	var peerTracker *peertracker.PeerTracker

	// Choose the highest priority peer
	peerTracker = ptq.pQueue.Peek().(*peertracker.PeerTracker)
	if peerTracker == nil {
		return "", nil, -1
	}

	if targetMinWork > peerTracker.RemainingData() {
		// Only serve up to the peer's remaining data for this round
		targetMinWork = peerTracker.RemainingData()
	}

	// Get the highest priority tasks for the given peer
	out, pendingWork := peerTracker.PopTasks(targetMinWork)
	peerTracker.SetRemainingData(peerTracker.RemainingData() - targetMinWork)

	// If the peer has no more tasks, remove its peer tracker
	if peerTracker.IsIdle() {
		ptq.pQueue.Pop()
		target := peerTracker.Target()
		delete(ptq.peerTrackers, target)
		delete(ptq.frozenPeers, target)
		ptq.callHooks(target, peerRemoved)
	} else if peerTracker.RemainingData() == 0 {
		// We've finished serving the peer for this round, remove their tracker from the pQueue
		ptq.pQueue.Pop()
	} else {
		// We may have modified the peer tracker's state (by popping tasks), so
		// update its position in the priority queue
		ptq.pQueue.Update(peerTracker.Index())
	}

	return peerTracker.Target(), out, pendingWork
}

// TasksDone is called to indicate that the given tasks have completed
// for the given peer
func (ptq *PeerTaskQueue) TasksDone(to peer.ID, tasks ...*peertask.Task) {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()

	// Get the peer tracker for the peer
	peerTracker, ok := ptq.peerTrackers[to]
	if !ok {
		return
	}

	// Tell the peer tracker that the tasks have completed
	for _, task := range tasks {
		peerTracker.TaskDone(task)
	}

	// This may affect the peer's position in the peer queue, so update if
	// necessary
	ptq.pQueue.Update(peerTracker.Index())
}

// Remove removes a task from the queue.
func (ptq *PeerTaskQueue) Remove(topic peertask.Topic, p peer.ID) {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()

	peerTracker, ok := ptq.peerTrackers[p]
	if ok {
		if peerTracker.Remove(topic) {
			// we now also 'freeze' that partner. If they sent us a cancel for a
			// block we were about to send them, we should wait a short period of time
			// to make sure we receive any other in-flight cancels before sending
			// them a block they already potentially have
			if !ptq.ignoreFreezing {
				if !peerTracker.IsFrozen() {
					ptq.frozenPeers[p] = struct{}{}
				}

				peerTracker.Freeze()
			}
			ptq.pQueue.Update(peerTracker.Index())
		}
	}
}

// FullThaw completely thaws all peers in the queue so they can execute tasks.
func (ptq *PeerTaskQueue) FullThaw() {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()

	for p := range ptq.frozenPeers {
		peerTracker, ok := ptq.peerTrackers[p]
		if ok {
			peerTracker.FullThaw()
			delete(ptq.frozenPeers, p)
			ptq.pQueue.Update(peerTracker.Index())
		}
	}
}

// ThawRound unthaws peers incrementally, so that those have been frozen the least
// become unfrozen and able to execute tasks first.
func (ptq *PeerTaskQueue) ThawRound() {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()

	for p := range ptq.frozenPeers {
		peerTracker, ok := ptq.peerTrackers[p]
		if ok {
			if peerTracker.Thaw() {
				delete(ptq.frozenPeers, p)
			}
			ptq.pQueue.Update(peerTracker.Index())
		}
	}
}
