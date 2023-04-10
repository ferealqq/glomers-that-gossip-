package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	cmap "github.com/orcaman/concurrent-map/v2"
)

// var seenMessages = make(map[float64]bool)
// var seenMu sync.Mutex

// var queue = make(chan BroadcastMsgs)
// // list of messages seen by broadcast
// var queMu sync.Mutex
// var msgValues = []float64{}

var lg *log.Logger
var logFile *os.File
var MAX_RETRY = 10

// str => destination
type BroadcastMsg struct {
	Payload BroadcastPayload
	Dst     string
}

type BroadcastPayload struct {
	MsgType string  `json:"type"`
	Message float64 `json:"message"`
	MsgId   float64 `json:"msg_id"`
}

type Node struct {
	*maelstrom.Node
	queue chan BroadcastMsg
	// store      map[int]struct{}
	store cmap.ConcurrentMap[float64, struct{}]
	// storeMu    sync.RWMutex
	neighbours []string
}

func sharding(key float64) uint32 {
	return math.Float32bits(float32(key))
}

func NewNode() *Node {
	return &Node{
		Node:  maelstrom.NewNode(),
		queue: make(chan BroadcastMsg),
		// store:      make(map[int]struct{}),
		store:      cmap.NewWithCustomShardingFunction[float64, struct{}](sharding),
		neighbours: nil,
	}
}

func (n *Node) broadcastMessage(src string, payload BroadcastPayload) error {
	lg.Println(n.neighbours)
	for _, dst := range n.neighbours {
		if dst == src {
			continue
		}
		n.queue <- BroadcastMsg{
			payload,
			dst,
		}
	}

	return nil
}

func (n *Node) sendBroadcast() {
	for {
		msg := <-n.queue
		go func(node *Node, m BroadcastMsg) {
			// try to send a message from broadcast to neighbour nodes
			if node.sendRPC(m) != nil {
				for i := 0; i < MAX_RETRY; i++ {
					if node.sendRPC(m) == nil {
						break
					}
				}
			}
		}(n, msg)
	}
}

func (n *Node) sendRPC(m BroadcastMsg) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*250)
	defer cancel()
	_, err := n.SyncRPC(ctx, m.Dst, m.Payload)
	return err
}

// returns a new logger and a new log file in question, remember to close log file after the process exists
func newLogger() (*log.Logger, *os.File) {
	// because the logging get's too noisy when using with maelstrom
	fileName := "/tmp/maelstrom.log"

	// open log file
	logFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
		return nil, nil
	}

	lg := log.New(logFile, "LOGGER: ", log.Ldate|log.Ltime)

	return lg, logFile
}

func (n *Node) Keys() []float64 {
	return n.store.Keys()
}

func main() {
	lg, logFile = newLogger()
	if logFile == nil {
		panic("Log file couldn't be created")
	}
	defer logFile.Close()

	n := NewNode()

	// handle messages to be sent to neighbour nodes
	go n.sendBroadcast()

	// TODO Nodes should only return copies of their own local values?
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message payload as an loosely-typed map.
		var payload BroadcastPayload
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}
		// n.storeMu.Lock()
		if _, ok := n.store.Get(payload.Message); ok {
			// n.storeMu.Unlock()
			return n.Reply(msg, map[string]any{
				"type": "broadcast_ok",
			})
		}
		n.store.Set(payload.Message, struct{}{})
		// n.store[val] = struct{}{}
		// n.storeMu.Unlock()
		if err := n.broadcastMessage(msg.Src, payload); err != nil {
			return err
		}
		// defer n.consumeBroadcast(msg, payload)
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
		body["messages"] = n.Keys()
		// body["messages"] a= n.values.Get()

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		nbrs := body["topology"].(map[string]interface{})[n.ID()]
		neighbors := []string{}
		for _, nei := range nbrs.([]interface{}) {
			neighbors = append(neighbors, nei.(string))
		}
		n.neighbours = neighbors

		return n.Reply(msg, map[string]string{
			"type": "topology_ok",
		})
	})
	if err := n.Run(); err != nil {
		lg.Fatal(err)
		log.Fatal(err)
	}
}
