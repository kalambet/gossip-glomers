package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	node      *maelstrom.Node
	seen      map[any]bool
	neighbors []string
)

func main() {
	node = maelstrom.NewNode()
	seen = make(map[any]bool)

	node.Handle("broadcast", broadcast)
	node.Handle("broadcast_ok", broadcastOK)
	node.Handle("read", read)
	node.Handle("topology", topology)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func broadcastOK(msg maelstrom.Message) error {
	// Make sure that we can handle the broadcast acknowledgement
	return nil
}

func broadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	message := body["message"]

	// Send back the acknowledgement
	body = make(map[string]any)
	body["type"] = "broadcast_ok"
	err := node.Reply(msg, body)
	if err != nil {
		return err
	}

	// If this node already seen the message it will skip its broadcast
	if _, ok := seen[message]; ok {
		return nil
	}
	// Otherwise let's mark it as seen
	seen[message] = true

	// Broadcast message through topology
	return gossip(msg.Src, message)
}

func gossip(src string, message any) error {
	body := make(map[string]any)
	body["type"] = "broadcast"
	body["message"] = message

	// Let's go through all the neighbors and pass the message
	for _, dst := range neighbors {
		// We should skip sending message to the source back since it already have seen it
		if dst == src {
			continue
		}
		
		err := node.Send(dst, body)
		if err != nil {
			return err
		}
	}
	return nil
}

func read(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "read_ok"
	messages := make([]any, 0)
	for message, _ := range seen {
		messages = append(messages, message)
	}
	body["messages"] = messages

	return node.Reply(msg, body)
}

func topology(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Collect the topology from the topology topic
	if t, ok := body["topology"].(map[string]any); ok {
		if neighbors, ok = t[node.ID()].([]string); !ok {
			neighbors = node.NodeIDs()
		}
	}

	body = make(map[string]any)
	body["type"] = "topology_ok"

	return node.Reply(msg, body)
}
