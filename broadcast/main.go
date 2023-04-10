package main

import (
	"encoding/json"
	"log"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	cmap "github.com/orcaman/concurrent-map/v2"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// var seenMessages = make(map[float64]bool)
// var seenMu sync.Mutex

// var queue = make(chan BroadcastMsgs)
// // list of messages seen by broadcast
// var queMu sync.Mutex
// var msgValues = []float64{}

var lg *log.Logger
var logFile *os.File
var sent = 0
var shouldSend = 0

// str => destination
type BroadcastMsgs = map[string]BroadcastPayload

type BroadcastPayload struct {
	MsgType string  `json:"type"`
	Message float64 `json:"message"`
	MsgId   float64 `json:"msg_id"`
}

type Node struct {
	*maelstrom.Node
	queMu sync.Mutex
	queue chan *BroadcastMsgs
	// seen map[float64]bool
	// seen   *sync.Map
	seen       cmap.ConcurrentMap[float64, bool]
	seenMu     sync.RWMutex
	values     *AtomicSlice
	neighbours *[]string
}

type AtomicSlice struct {
	value unsafe.Pointer
}

func NewAtomicSlice(slice []int) *AtomicSlice {
	return &AtomicSlice{value: unsafe.Pointer(&slice)}
}

func (s *AtomicSlice) Get() []int {
	return *(*[]int)(atomic.LoadPointer(&s.value))
}

func sharding(key float64) uint32 {
	return math.Float32bits(float32(key))
}

func NewNode() *Node {
	return &Node{
		Node:  maelstrom.NewNode(),
		queue: make(chan *BroadcastMsgs),
		// seen:   make(map[float64]bool),
		seen:       cmap.NewWithCustomShardingFunction[float64, bool](sharding),
		values:     NewAtomicSlice([]int{}),
		neighbours: nil,
	}
}

func (n *Node) consumeBroadcast(msg maelstrom.Message, payload BroadcastPayload) {
	// n.seenMu.RLock()
	val := payload.Message
	// if a value has not been seen send the values to neighbouring nodes and save the seen message to the list
	if l, _ := n.seen.Get(val); l == false {
		// lg.Println(n.seen.Keys())
		n.seen.Set(val, true)
		// currentNumbers := n.values.Get()
		// newNumbers := append(currentNumbers, int(val))
		// atomic.StorePointer(&n.values.value, unsafe.Pointer(&newNumbers))

		send := make(BroadcastMsgs)
		for _, i := range n.getNeighbours() {
			if i == msg.Src {
				if i == msg.Src {
					lg.Printf(" dst == msg.Src %s \n", i)
				}
				continue
			}
			send[i] = payload
			// if sending failes try to resend
			// if n.RPC(i, payload, func(msg maelstrom.Message) error {
			// 	// our attention is not required
			// 	return nil
			// }) != nil {
			// 	send[i] = payload
			// }
		}
		if len(send) > 0 {
			n.queMu.Lock()
			n.queue <- &send
			defer n.queMu.Unlock()
		}
	}
}

func (n *Node) getNeighbours() []string {
	if n.neighbours == nil {
		ns := []string{}
		for _, i := range n.NodeIDs() {
			if i != n.ID() {
				ns = append(ns, i)
			}
		}
		n.neighbours = &ns
	}
	return *n.neighbours
}

func (n *Node) broadcastMessages() {
	for {
		msg := <-n.queue
		go func(node *Node, m *BroadcastMsgs) {
			resend := make(BroadcastMsgs)
			// try to send a message from broadcast to neighbour nodes
			for dst, payload := range *m {
				if node.RPC(dst, payload, func(msg maelstrom.Message) error {
					res := make(map[string]any)
					err := json.Unmarshal(msg.Body, &res)
					if err != nil || res["type"] != "broadcast_ok" {
						node.queMu.Lock()
						node.queue <- &BroadcastMsgs{
							dst: payload,
						}
						node.queMu.Unlock()
					}
					return nil
				}) != nil {
					resend[dst] = payload
				}
			}
			node.queMu.Lock()
			node.queue <- &resend
			node.queMu.Unlock()
		}(n, msg)
	}
}

// returns a new logger and a new log file in question, remember to close log file after the process exists
func newLogger() (*log.Logger, *os.File) {
	// because the logging get's too noisy when using with maelstrom
	fileName := "/home/peke/Documents/code/distributed-systems-challenge/broadcast/logFile.log"

	// open log file
	logFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
		return nil, nil
	}

	lg := log.New(logFile, "LOGGER: ", log.Ldate|log.Ltime)

	return lg, logFile
}

func main() {
	lg, logFile = newLogger()
	if logFile == nil {
		panic("Log file couldn't be created")
	}
	defer logFile.Close()

	n := NewNode()

	// handle messages to be sent to neighbour nodes
	// go n.broadcastMessages()

	// TODO Nodes should only return copies of their own local values?
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message payload as an loosely-typed map.
		var payload BroadcastPayload
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}

		defer n.consumeBroadcast(msg, payload)
		// Echo the original message back with the updated message type.
		return n.Reply(msg, map[string]any{
			"type":   "broadcast_ok",
			"msg_id": payload.MsgId,
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if body == nil {
			body = make(map[string]any)
		}
		body["type"] = "read_ok"
		body["messages"] = n.seen.Keys()
		// body["messages"] a= n.values.Get()

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if body == nil {
			return n.Reply(msg, map[string]string{
				"type": "topology_ok",
			})
		}
		delete(body, "topology")
		body["type"] = "topology_ok"
		return n.Reply(msg, body)
	})
	if err := n.Run(); err != nil {
		lg.Fatal(err)
		log.Fatal(err)
	}
}
