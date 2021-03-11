package peertaskqueue

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/ipfs/go-peertaskqueue/peertask"
	"github.com/ipfs/go-peertaskqueue/testutil"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func TestPushPop(t *testing.T) {
	ptq := New()
	partner := testutil.GeneratePeers(1)[0]
	alphabet := strings.Split("abcdefghijklmnopqrstuvwxyz", "")
	vowels := strings.Split("aeiou", "")
	consonants := func() []string {
		var out []string
		for _, letter := range alphabet {
			skip := false
			for _, vowel := range vowels {
				if letter == vowel {
					skip = true
				}
			}
			if !skip {
				out = append(out, letter)
			}
		}
		return out
	}()
	sort.Strings(alphabet)
	sort.Strings(vowels)
	sort.Strings(consonants)

	// add a bunch of blocks. cancel some. drain the queue. the queue should only have the kept tasks

	for _, index := range rand.Perm(len(alphabet)) { // add blocks for all letters
		letter := alphabet[index]
		t.Log(letter)

		// add tasks out of order, but with in-order priority
		ptq.PushTasks(partner, peertask.Task{Topic: letter, Priority: math.MaxInt32 - index})
	}
	for _, consonant := range consonants {
		ptq.Remove(consonant, partner)
	}

	ptq.FullThaw()

	var out []string
	for {
		_, received, _ := ptq.PopTasks(100)
		if len(received) == 0 {
			break
		}

		for _, task := range received {
			out = append(out, task.Topic.(string))
		}
	}

	// Tasks popped should already be in correct order
	for i, expected := range vowels {
		if out[i] != expected {
			t.Fatal("received", out[i], "expected", expected)
		}
	}
}

// TestPeerWeightsOneRoundConstantWeights gives all peers the same weight and a single task each. Popping one task
// for each peer should exhaust a single round-robin round and leave all peers with no work
// remaining.
func TestPeerWeightsOneRoundConstantWeights(t *testing.T) {
	numPeers := rand.Int()%100 + 1   // [1, 100]
	blockSize := rand.Int()%1000 + 1 // [1, 1000]
	ptq := NewWithRoundSize(numPeers * blockSize)
	peerIDs := testutil.GeneratePeers(numPeers)

	// add a task for every peer
	for _, id := range peerIDs {
		ptq.PushTasks(id, peertask.Task{Topic: "a", Work: blockSize})
		// same weight for each peer
		ptq.SetWeight(id, 1)
	}

	// initialize round
	ptq.newRound()

	// every peer should have workSize as their initial remaining work
	for _, peer := range ptq.peerTrackers {
		if peer.WorkRemaining() != blockSize {
			t.Fatalf("Peer %s: expected %d work remaining for all peers, got %d", peer.Target(), blockSize, peer.WorkRemaining())
		}
	}

	// call PopTasks once for each peer (should result in serving each peer once)
	for i := 0; i < numPeers; i++ {
		_, received, _ := ptq.PopTasks(blockSize)
		if received == nil {
			t.Fatalf("Failed to pop tasks for peer before all peers were served")
		}
	}

	// all peers should have 0 work remaining
	for _, peer := range ptq.peerTrackers {
		if peer.WorkRemaining() != 0 {
			t.Errorf("Expected 0 work remaining for all peers, got %d for peer %s", peer.WorkRemaining(), peer.Target())
		}
	}
}

// TestPeerWeightsOneRoundVaryingWeights: for each peer `i` in `[0, n)`, we push `i` tasks for
// that peer, and set the weight of that peer to `i`. The round size is equal to the total number
// of blocks we push. By the end of a single round, we should have popped all of every peer's blocks.
func TestPeerWeightsOneRoundVaryingWeights(t *testing.T) {
	numPeers := rand.Int()%100 + 1
	blockSize := rand.Int()%1000 + 1
	numBlocks := numPeers * (numPeers - 1) / 2
	ptq := NewWithRoundSize(blockSize * numBlocks)
	peerIDs := testutil.GeneratePeers(numPeers)

	// add task(s) for every peer
	for i, id := range peerIDs {
		for j := 0; j < i; j++ { // peer 0 wants 0 blocks, peer 1 wants 1, ...
			ptq.PushTasks(id, peertask.Task{Topic: fmt.Sprintf("%d-%d", i, j), Work: blockSize})
		}
	}

	// set weight of each peer based on its index
	for index, id := range peerIDs {
		ptq.SetWeight(id, float64(index))
	}

	// calculate work for each peer
	ptq.newRound()

	poppedBlocks := make(map[peer.ID]int)
	for _, id := range peerIDs {
		poppedBlocks[id] = 0
	}

	// pop all tasks
	for i := 0; i < numBlocks; i++ {
		id, received, _ := ptq.PopTasks(blockSize)
		if received == nil {
			t.Fatalf("No more tasks to pop, expected %d more", numBlocks-i)
		}
		poppedBlocks[id] += len(received)
	}

	// check that we popped the expected number blocks for each peer
	totalPopped := 0
	for i, peer := range peerIDs {
		numPopped, ok := poppedBlocks[peer]
		if !ok {
			if i != 0 {
				t.Fatalf("Peer %s was not in poppedBlocks; this should never happen", peer)
			}
		}
		if numPopped != i {
			t.Errorf("Peer %s: expected %d blocks to be popped for peer, actual was %d", peer, i, numPopped)
		}

		totalPopped += numPopped
	}

	if totalPopped != numBlocks {
		t.Fatalf("Expected %d total blocks to be popped off, got %d", numBlocks, totalPopped)
	}

	for _, peer := range ptq.peerTrackers {
		t.Logf("Got %d work remaining for peer %s", peer.WorkRemaining(), peer.Target())
	}
	for _, peer := range ptq.peerTrackers {
		if peer.WorkRemaining() != 0 {
			t.Errorf("Expected 0 work remaining for all peers, got %d for peer %s", peer.WorkRemaining(), peer.Target())
		}
	}

	// This must be called after the WorkRemaining() checks above because it'll start the next round +
	// recalculate the peers' WorkRemaining() values
	_, received, _ := ptq.PopTasks(blockSize)
	if received != nil {
		t.Fatalf("Expected no more tasks")
	}
}

// Same as TestPeerWeightsSingleRoundVaryingWeights, but with a random number of rounds > 2.
func TestPeerWeightsMultiRound(t *testing.T) {
	numPeers := rand.Int()%100 + 1
	blockSize := rand.Int()%1000 + 1
	numRounds := rand.Int()%20 + 2
	numBlocksPerRound := numPeers * (numPeers - 1) / 2
	ptq := NewWithRoundSize(blockSize * numBlocksPerRound)
	peerIDs := testutil.GeneratePeers(numPeers)

	// add a task for every peer
	for i, id := range peerIDs {
		for j := 0; j < i; j++ { // peer 0 wants 0 blocks, peer 1 wants 1, ...
			ptq.PushTasks(id, peertask.Task{Topic: fmt.Sprintf("%d-%d", i, j), Work: blockSize})
		}
	}

	// set weight of each peer based on its index
	for index, id := range peerIDs {
		ptq.SetWeight(id, float64(index))
	}

	for round := 0; round < numRounds; round++ {
		t.Logf("round %d", round)
		// calculate work for each peer
		ptq.newRound()

		poppedBlocks := make(map[peer.ID]int)
		for _, id := range peerIDs {
			poppedBlocks[id] = 0
		}

		// pop all tasks
		for i := 0; i < numBlocksPerRound; i++ {
			id, received, _ := ptq.PopTasks(blockSize)
			if received == nil {
				t.Fatalf("No more tasks to pop, expected %d more", numBlocksPerRound-i)
			}
			poppedBlocks[id] += len(received)
		}

		// check that we popped the expected number blocks for each peer
		totalPopped := 0
		for i, peer := range peerIDs {
			numPopped, ok := poppedBlocks[peer]
			if !ok {
				if i != 0 {
					t.Fatalf("Peer %s was not in poppedBlocks; this should never happen", peer)
				}
			}
			if numPopped != i {
				t.Errorf("Peer %s: expected %d blocks to be popped for peer, actual was %d", peer, i, numPopped)
			}

			totalPopped += numPopped
		}

		if totalPopped != numBlocksPerRound {
			t.Fatalf("Expected %d total blocks to be popped off, got %d", numBlocksPerRound, totalPopped)
		}

		for _, peer := range ptq.peerTrackers {
			if peer.WorkRemaining() != 0 {
				t.Errorf("Expected 0 work remaining for all peers, got %d for peer %s", peer.WorkRemaining(), peer.Target())
			}
		}

		// add a task for every peer
		for i, id := range peerIDs {
			for j := 0; j < i; j++ { // peer 0 wants 0 blocks, peer 1 wants 1, ...
				ptq.PushTasks(id, peertask.Task{Topic: fmt.Sprintf("%d-%d-%d", round, i, j), Work: blockSize})
			}
		}
	}
}

// func TestFreezeUnfreeze(t *testing.T) {
// 	ptq := New()
// 	peers := testutil.GeneratePeers(4)
// 	a := peers[0]
// 	b := peers[1]
// 	c := peers[2]
// 	d := peers[3]

// 	// Push 5 blocks to each peer
// 	for i := 0; i < 5; i++ {
// 		is := fmt.Sprint(i)
// 		ptq.PushTasks(a, peertask.Task{Topic: is, Work: 1})
// 		ptq.PushTasks(b, peertask.Task{Topic: is, Work: 1})
// 		ptq.PushTasks(c, peertask.Task{Topic: is, Work: 1})
// 		ptq.PushTasks(d, peertask.Task{Topic: is, Work: 1})
// 	}

// 	// now, pop off four tasks, there should be one from each
// 	matchNTasks(t, ptq, 4, a.Pretty(), b.Pretty(), c.Pretty(), d.Pretty())

// 	ptq.Remove("1", b)

// 	// b should be frozen, causing it to get skipped in the rotation
// 	matchNTasks(t, ptq, 3, a.Pretty(), c.Pretty(), d.Pretty())

// 	ptq.ThawRound()

// 	matchNTasks(t, ptq, 1, b.Pretty())

// 	// remove none existent task
// 	ptq.Remove("-1", b)

// 	// b should not be frozen
// 	matchNTasks(t, ptq, 4, a.Pretty(), b.Pretty(), c.Pretty(), d.Pretty())
// }

// func TestFreezeUnfreezeNoFreezingOption(t *testing.T) {
// 	ptq := New(IgnoreFreezing(true))
// 	peers := testutil.GeneratePeers(4)
// 	a := peers[0]
// 	b := peers[1]
// 	c := peers[2]
// 	d := peers[3]

// 	// Have each push some blocks

// 	for i := 0; i < 5; i++ {
// 		is := fmt.Sprint(i)
// 		ptq.PushTasks(a, peertask.Task{Topic: is, Work: 1})
// 		ptq.PushTasks(b, peertask.Task{Topic: is, Work: 1})
// 		ptq.PushTasks(c, peertask.Task{Topic: is, Work: 1})
// 		ptq.PushTasks(d, peertask.Task{Topic: is, Work: 1})
// 	}

// 	// now, pop off four tasks, there should be one from each
// 	matchNTasks(t, ptq, 4, a.Pretty(), b.Pretty(), c.Pretty(), d.Pretty())

// 	ptq.Remove("1", b)

// 	// b should not be frozen, so it wont get skipped in the rotation
// 	matchNTasks(t, ptq, 4, a.Pretty(), b.Pretty(), c.Pretty(), d.Pretty())
// }

// // This test checks that ordering of peers is correct
// func TestPeerOrder(t *testing.T) {
// 	ptq := New()
// 	peers := testutil.GeneratePeers(3)
// 	a := peers[0]
// 	b := peers[1]
// 	c := peers[2]

// 	ptq.PushTasks(a, peertask.Task{Topic: "1", Work: 3, Priority: 2})
// 	ptq.PushTasks(a, peertask.Task{Topic: "2", Work: 1, Priority: 1})

// 	ptq.PushTasks(b, peertask.Task{Topic: "3", Work: 1, Priority: 3})
// 	ptq.PushTasks(b, peertask.Task{Topic: "4", Work: 3, Priority: 2})
// 	ptq.PushTasks(b, peertask.Task{Topic: "5", Work: 1, Priority: 1})

// 	ptq.PushTasks(c, peertask.Task{Topic: "6", Work: 2, Priority: 2})
// 	ptq.PushTasks(c, peertask.Task{Topic: "7", Work: 2, Priority: 1})

// 	// All peers have nothing in their active queue, so equal chance of any
// 	// peer being chosen
// 	var ps []string
// 	var ids []string
// 	for i := 0; i < 3; i++ {
// 		p, tasks, _ := ptq.PopTasks(1)
// 		ps = append(ps, p.String())
// 		ids = append(ids, fmt.Sprint(tasks[0].Topic))
// 	}
// 	matchArrays(t, ps, []string{a.String(), b.String(), c.String()})
// 	matchArrays(t, ids, []string{"1", "3", "6"})

// 	// Active queues:
// 	// a: 3            Pending: [1]
// 	// b: 1            Pending: [3, 1]
// 	// c: 2            Pending: [2]
// 	// So next peer should be b (least work in active queue)
// 	p, tsk, pending := ptq.PopTasks(1)
// 	if len(tsk) != 1 || p != b || tsk[0].Topic != "4" {
// 		t.Fatal("Expected ID 4 from peer b")
// 	}
// 	if pending != 1 {
// 		t.Fatal("Expected pending work to be 1")
// 	}

// 	// Active queues:
// 	// a: 3            Pending: [1]
// 	// b: 1 + 3        Pending: [1]
// 	// c: 2            Pending: [2]
// 	// So next peer should be c (least work in active queue)
// 	p, tsk, pending = ptq.PopTasks(1)
// 	if len(tsk) != 1 || p != c || tsk[0].Topic != "7" {
// 		t.Fatal("Expected ID 7 from peer c")
// 	}
// 	if pending != 0 {
// 		t.Fatal("Expected pending work to be 0")
// 	}

// 	// Active queues:
// 	// a: 3            Pending: [1]
// 	// b: 1 + 3        Pending: [1]
// 	// c: 2 + 2
// 	// So next peer should be a (least work in active queue)
// 	p, tsk, pending = ptq.PopTasks(1)
// 	if len(tsk) != 1 || p != a || tsk[0].Topic != "2" {
// 		t.Fatal("Expected ID 2 from peer a")
// 	}
// 	if pending != 0 {
// 		t.Fatal("Expected pending work to be 0")
// 	}

// 	// Active queues:
// 	// a: 3 + 1
// 	// b: 1 + 3        Pending: [1]
// 	// c: 2 + 2
// 	// a & c have no more pending tasks, so next peer should be b
// 	p, tsk, pending = ptq.PopTasks(1)
// 	if len(tsk) != 1 || p != b || tsk[0].Topic != "5" {
// 		t.Fatal("Expected ID 5 from peer b")
// 	}
// 	if pending != 0 {
// 		t.Fatal("Expected pending work to be 0")
// 	}

// 	// Active queues:
// 	// a: 3 + 1
// 	// b: 1 + 3 + 1
// 	// c: 2 + 2
// 	// No more pending tasks, so next pop should return nothing
// 	_, tsk, pending = ptq.PopTasks(1)
// 	if len(tsk) != 0 {
// 		t.Fatal("Expected no more tasks")
// 	}
// 	if pending != 0 {
// 		t.Fatal("Expected pending work to be 0")
// 	}
// }

// func TestHooks(t *testing.T) {
// 	var peersAdded []string
// 	var peersRemoved []string
// 	onPeerAdded := func(p peer.ID) {
// 		peersAdded = append(peersAdded, p.Pretty())
// 	}
// 	onPeerRemoved := func(p peer.ID) {
// 		peersRemoved = append(peersRemoved, p.Pretty())
// 	}
// 	ptq := New(OnPeerAddedHook(onPeerAdded), OnPeerRemovedHook(onPeerRemoved))
// 	peers := testutil.GeneratePeers(2)
// 	a := peers[0]
// 	b := peers[1]
// 	ptq.PushTasks(a, peertask.Task{Topic: "1"})
// 	ptq.PushTasks(b, peertask.Task{Topic: "2"})
// 	expected := []string{a.Pretty(), b.Pretty()}
// 	sort.Strings(expected)
// 	sort.Strings(peersAdded)
// 	if len(peersAdded) != len(expected) {
// 		t.Fatal("Incorrect number of peers added")
// 	}
// 	for i, s := range peersAdded {
// 		if expected[i] != s {
// 			t.Fatal("unexpected peer", s, expected[i])
// 		}
// 	}

// 	p, task, _ := ptq.PopTasks(100)
// 	ptq.TasksDone(p, task...)
// 	p, task, _ = ptq.PopTasks(100)
// 	ptq.TasksDone(p, task...)
// 	ptq.PopTasks(100)
// 	ptq.PopTasks(100)

// 	sort.Strings(peersRemoved)
// 	if len(peersRemoved) != len(expected) {
// 		t.Fatal("Incorrect number of peers removed")
// 	}
// 	for i, s := range peersRemoved {
// 		if expected[i] != s {
// 			t.Fatal("unexpected peer", s, expected[i])
// 		}
// 	}
// }

// func TestCleaningUpQueues(t *testing.T) {
// 	ptq := New()

// 	peer := testutil.GeneratePeers(1)[0]
// 	var peerTasks []peertask.Task
// 	for i := 0; i < 5; i++ {
// 		is := fmt.Sprint(i)
// 		peerTasks = append(peerTasks, peertask.Task{Topic: is})
// 	}

// 	// push a block, pop a block, complete everything, should be removed
// 	ptq.PushTasks(peer, peerTasks...)
// 	p, task, _ := ptq.PopTasks(100)
// 	ptq.TasksDone(p, task...)
// 	_, task, _ = ptq.PopTasks(100)

// 	if len(task) != 0 || len(ptq.peerTrackers) > 0 || ptq.pQueue.Len() > 0 {
// 		t.Fatal("PeerTracker should have been removed because it's idle")
// 	}

// 	// push a block, remove each of its entries, should be removed
// 	ptq.PushTasks(peer, peerTasks...)
// 	for _, peerTask := range peerTasks {
// 		ptq.Remove(peerTask.Topic, peer)
// 	}
// 	_, task, _ = ptq.PopTasks(100)

// 	if len(task) != 0 || len(ptq.peerTrackers) > 0 || ptq.pQueue.Len() > 0 {
// 		t.Fatal("Partner should have been removed because it's idle")
// 	}
// }

// func matchNTasks(t *testing.T, ptq *PeerTaskQueue, n int, expected ...string) {
// 	var targets []string
// 	for i := 0; i < n; i++ {
// 		p, tsk, _ := ptq.PopTasks(1)
// 		if len(tsk) != 1 {
// 			t.Fatal("expected 1 task at a time")
// 		}
// 		targets = append(targets, p.Pretty())
// 	}

// 	matchArrays(t, expected, targets)
// }

// func matchArrays(t *testing.T, str1, str2 []string) {
// 	if len(str1) != len(str2) {
// 		t.Fatal("array lengths did not match", str1, str2)
// 	}

// 	sort.Strings(str1)
// 	sort.Strings(str2)

// 	t.Log(str1)
// 	t.Log(str2)
// 	for i, s := range str2 {
// 		if str1[i] != s {
// 			t.Fatal("unexpected peer", s, str1[i])
// 		}
// 	}
// }
