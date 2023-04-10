package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

//str => destination
//any => body
type FailedMsg = map[string]any

func main() {
	n := maelstrom.NewNode()
	lst := []int{}
	failChan := make(chan FailedMsg)
	// handle failed messages
	go func ()  {
		// try to resend failed messages 
		for msg := range failChan {
			for dst, body := range msg {
				n.Send(dst,body)
			}
		}
	}()

	// TODO Nodes should only return copies of their own local values?
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// try to unpack message
		switch body["message"].(type) {
		case float64:
			lst = append(lst, int(body["message"].(float64)))
		case int:
			lst = append(lst, body["message"].(int))
		default:
			return n.Reply(msg, map[string]string{
				"type": "error",
			})
		}
		// push failed messages to failed channel which will try to resend failed messages
		failed := make(FailedMsg)
		for _,i := range n.NodeIDs() {
			if i == n.ID() {continue}
			if err := n.Send(i, body); err != nil {
				failed[i] = body
			}
		}
		if len(failed) > 0 {
			failChan <- failed			
		}
		res := map[string]string{
			"type": "broadcast_ok",
		}
		// Echo the original message back with the updated message type.
		return n.Reply(msg, res)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		log.Print(body)
		if body == nil {
			body = make(map[string]any)
		}

		body["type"] = "read_ok"
		body["messages"] = lst

		return n.Reply(msg,body)
	})

	n.Handle("topology", func (msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		log.Print(body)
		if body == nil {
			body = make(map[string]any)
		}
		if body["topology"] != nil {
			delete(body, "topology")
		}
		body["type"] = "topology_ok"

		return n.Reply(msg,body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
