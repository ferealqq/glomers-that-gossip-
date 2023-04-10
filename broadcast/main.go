package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var lg *log.Logger
var logFile *os.File
var MAX_RETRY = 35
var TIMEOUT = time.Millisecond * 650

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
	queue         chan BroadcastMsg
	store         map[float64]struct{}
	storeMu       sync.RWMutex
	optNeighbours []string // neighbours that doesn't have conflicting neighbours (i.e if neighbour has a same neighbour as I do don't send broadcast, because you'll neighbour will do it)
}

func sharding(key float64) uint32 {
	return math.Float32bits(float32(key))
}

func newNode() *Node {
	return &Node{
		Node:          maelstrom.NewNode(),
		queue:         make(chan BroadcastMsg),
		store:         make(map[float64]struct{}),
		optNeighbours: nil,
	}
}

func (n *Node) broadcastMessage(src string, payload BroadcastPayload) error {
	for _, dst := range n.optNeighbours {
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
					} else {
						time.Sleep(TIMEOUT)
						continue
					}
				}
			}
		}(n, msg)
	}
}

func (n *Node) sendRPC(m BroadcastMsg) error {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	_, err := n.SyncRPC(ctx, m.Dst, m.Payload)
	return err
}

// if neighbour has a same neighbour as I do don't send broadcast, because you'll neighbour will do it
// call only if node.neighbours has been defined
func (n *Node) optimzeNeighbours(topology map[string][]string) []string {
	neighbours := make(map[string]struct{})
	for _, n := range topology[n.ID()] {
		neighbours[n] = struct{}{}
	}
	delete(topology, n.ID())
	/**
	-> our id =
	topology := map[string][]string{
		"n0": {"n1", "n2"},
		"n1": {"n3", "n4"},
		"n2": {"n1"},
	}
	assert.Equal(t, optimizeNeighbours(topology)[0], "n2")
	assert.Equal(t, len(optimizeNeighbours(topology)), 1)
	expected result: [n2]

	topology := map[string][]string{
		"n0": {"n1", "n2"},
		"n1": {"n2"},
		"n2": {"n3", "n4"},
	}
	assert.Equal(t, optimizeNeighbours(topology)[0], "n1")
	assert.Equal(t, len(optimizeNeighbours(topology)), 1)
	expected result: [n1]
	**/
	for key, ns := range topology {
		if _, ok := neighbours[key]; ok {
			for _, k := range ns {
				if _, i := neighbours[k]; i {
					delete(neighbours, k)
				}
			}
		}
	}
	neighbourIds := make([]string, 0, len(neighbours))
	for k := range neighbours {
		neighbourIds = append(neighbourIds, k)
	}
	n.optNeighbours = neighbourIds
	return neighbourIds
}

// returns a new logger and a new log file in question, remember to close log file after the process exists
// new logger because the default logging get's too noisy by the maelstorm go library
func newLogger() (*log.Logger, *os.File) {
	fileName := "/tmp/maelstrom.log"

	logFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
		return nil, nil
	}

	lg := log.New(logFile, "LOGGER: ", log.Ldate|log.Ltime)

	return lg, logFile
}

func (n *Node) keys() []float64 {
	n.storeMu.RLock()
	keys := make([]float64, 0, len(n.store))
	for k := range n.store {
		keys = append(keys, k)
	}
	n.storeMu.RUnlock()
	return keys
}

func main() {
	lg, logFile = newLogger()
	if logFile == nil {
		panic("Log file couldn't be created")
	}
	defer logFile.Close()

	n := newNode()

	// send broadcastmessages to neighbour nodes
	go n.sendBroadcast()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message payload as an loosely-typed map.
		var payload BroadcastPayload
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}
		n.storeMu.Lock()
		if _, ok := n.store[payload.Message]; ok {
			n.storeMu.Unlock()
			return n.Reply(msg, map[string]any{
				"type":   "broadcast_ok",
				"msg_id": payload.MsgId,
			})
		}
		n.store[payload.Message] = struct{}{}
		n.storeMu.Unlock()
		if err := n.broadcastMessage(msg.Src, payload); err != nil {
			return err
		}

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
		body["messages"] = n.keys()

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		lg.Println(body)
		topoRaw := body["topology"].(map[string]interface{})
		topology := make(map[string][]string)
		for key, ns := range topoRaw {
			for _, s := range ns.([]interface{}) {
				if topology[key] == nil {
					topology[key] = []string{s.(string)}
				} else {
					topology[key] = append(topology[key], s.(string))
				}
			}
		}
		n.optimzeNeighbours(topology)

		return n.Reply(msg, map[string]string{
			"type": "topology_ok",
		})
	})
	if err := n.Run(); err != nil {
		lg.Fatal(err)
		log.Fatal(err)
	}
}
