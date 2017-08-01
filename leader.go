package dolly

import (
	"fmt"
	"time"

	zmq "github.com/pebbe/zmq4"
)

// Leader defines a server that can respond to both Get and Put requests and
// publishes state to all replica subscribers.
type Leader struct {
	Replica
}

// Serve the leader, publishing state updates and responding to snapshot
// requests as well as GET and PUT requests.
func (l *Leader) Serve(ctx *zmq.Context, echan chan<- error) {
	var err error

	// Initialize the store and save state
	l.context = ctx
	l.store = make(map[string]*Message)

	// Connect all of the sockets
	if err = l.Bind(); err != nil {
		echan <- err
		return
	}

	// Create a poller to collect info from the sockets
	poller := zmq.NewPoller()
	poller.Add(l.snapshots, zmq.POLLIN)
	poller.Add(l.requests, zmq.POLLIN)

	// Run the replica server forever
	for {
		// Poll the sockets with a 1 second timeout
		items, err := poller.Poll(time.Second * 1)
		if err != nil {
			echan <- err
			return
		}

		// Go through each item to handle requests
		for _, item := range items {

			// Handle Requests
			if item.Socket == l.requests {
				if err := l.onRequests(); err != nil {
					echan <- err
					return
				}
			}

			// Handle Snapshots
			if item.Socket == l.snapshots {
				if err := l.onSnapshots(); err != nil {
					echan <- err
					return
				}
			}

		}
	}
}

// Bind the sockets on the appropriate ports
func (l *Leader) Bind() (err error) {

	// Create the snapshots socket
	if l.snapshots, err = l.context.NewSocket(zmq.ROUTER); err != nil {
		return err
	}
	endpoint := fmt.Sprintf("tcp://*:%d", l.Snapshots)
	if err = l.snapshots.Bind(endpoint); err != nil {
		return err
	}
	info("bound snapshots ROUTER socket to %s", endpoint)

	// Create the publish socket
	if l.updates, err = l.context.NewSocket(zmq.PUB); err != nil {
		return err
	}
	endpoint = fmt.Sprintf("tcp://*:%d", l.Updates)
	if err = l.updates.Bind(endpoint); err != nil {
		return err
	}
	info("bound updates PUB socket to %s", endpoint)

	// Create the requests socket
	if l.requests, err = l.context.NewSocket(zmq.ROUTER); err != nil {
		return err
	}
	endpoint = fmt.Sprintf("tcp://*:%d", l.Requests)
	if err = l.requests.Bind(endpoint); err != nil {
		return err
	}
	info("bound requests ROUTER socket to %s", endpoint)

	return nil
}

// Handle a request from a client.
func (l *Leader) onRequests() error {
	// Get the message from the socket
	msg, route, err := RecvMessage(l.requests, true)
	if err != nil {
		return err
	}

	// Mux the request correctly
	switch msg.method {
	case MethodGet:
		return l.onGet(msg, route)
	case MethodPut:
		return l.onPut(msg, route)
	default:
		return fmt.Errorf("unknown request method %s", msg.method)
	}
}

// Handle snapshots to bring a replica up to date.
func (l *Leader) onSnapshots() error {
	// Read the message off the wire
	msg, route, err := RecvMessage(l.snapshots, true)
	if err != nil {
		return err
	}

	// Ensure the message type is correct
	if msg.method != MethodSnapshot {
		return fmt.Errorf("bad request, cannot recv %s on snapshots", msg.method)
	}

	// Send every entry in the store to the client
	keys := 0
	for _, val := range l.store {
		keys++
		val.Send(l.snapshots, route)
	}

	// Send finished with sequence number
	reply := &Message{
		method:   MethodTerm,
		sequence: l.sequence,
		key:      "",
		body:     nil,
	}
	reply.Send(l.snapshots, route)
	info("sent %d keys on state snapshot %d", keys, l.sequence)
	return nil
}

// Handle a Put request from a client
func (l *Leader) onPut(msg *Message, route []byte) error {
	// Store the message and increment the state sequence
	l.sequence++
	msg.sequence = l.sequence

	// Publish the message to all replicas
	if err := msg.Send(l.updates, nil); err != nil {
		return err
	}

	// Store the state locally
	l.store[msg.key] = msg
	info("published state %d updated %s=%s", l.sequence, msg.key, msg.body)

	// Respond to the client
	return msg.Send(l.requests, route)
}
