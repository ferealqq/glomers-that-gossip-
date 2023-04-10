package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	lst := []int{}
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
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
