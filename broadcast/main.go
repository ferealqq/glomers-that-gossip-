package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

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
	MsgType string   `json:"type"`
	Message float64  `json:"message"`
	MsgId   *float64 `json:"msg_id,omitempty"`
}

type Node struct {
	*maelstrom.Node
	queMu sync.Mutex
	queue chan BroadcastMsgs
	// seen map[float64]bool
	seen   *sync.Map
	seenMu sync.RWMutex
	values *AtomicSlice
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

func NewNode() *Node {
	return &Node{
		Node:  maelstrom.NewNode(),
		queue: make(chan BroadcastMsgs),
		// seen:   make(map[float64]bool),
		seen:   &sync.Map{},
		values: NewAtomicSlice([]int{}),
	}
}

func (n *Node) consumeBroadcast(msg maelstrom.Message, payload BroadcastPayload) {
	// n.seenMu.RLock()
	val := payload.Message
	// if a value has not been seen send the values to neighbouring nodes and save the seen message to the list
	if _, l := n.seen.LoadOrStore(val, true); l == false {
		lg.Println(n.seen)
		// currentNumbers := n.values.Get()
		// newNumbers := append(currentNumbers, int(val))
		// atomic.StorePointer(&n.values.value, unsafe.Pointer(&newNumbers))

		send := make(BroadcastMsgs)
		for _, i := range n.NodeIDs() {
			// if sending failes try to resend
			if n.RPC(i, payload, func(msg maelstrom.Message) error {
				// our attention is not required
				return nil
			}) != nil {
				send[i] = payload
			}
		}
		shouldSend += len(send)
		if len(send) > 0 {
			n.queMu.Lock()
			n.queue <- send
			defer n.queMu.Unlock()
		}
	}
}

func (n *Node) handleFailed() {
	for msg := range n.queue {
		resend := make(BroadcastMsgs)
		// try to send a message from broadcast to neighbour nodes
		for dst, payload := range msg {
			if n.RPC(dst, payload, func(msg maelstrom.Message) error {
				// our attention is not required
				return nil
			}) != nil {
				resend[dst] = payload
				sent += 1
			}
		}
		lg.Printf("sent %d should sent %d \n", sent, shouldSend)
		n.queue <- resend
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
	go n.handleFailed()

	// TODO Nodes should only return copies of their own local values?
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message payload as an loosely-typed map.
		var payload BroadcastPayload
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}

		// it's not necessary to respond to messages that doesn't have msg_id.
		// if the message has msg_id it requires a response from the client but if not no reponse is required
		// https://github.com/jepsen-io/maelstrom/blob/main/doc/03-broadcast/01-broadcast.md
		res := map[string]string{
			"type": "broadcast_ok",
		}

		defer n.consumeBroadcast(msg, payload)
		// Echo the original message back with the updated message type.
		return n.Reply(msg, res)
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
		keys := []float64{}
		n.seen.Range(func(key, value any) bool {
			keys = append(keys, key.(float64))
			return true
		})
		body["messages"] = keys
		// body["messages"] = n.values.Get()

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
