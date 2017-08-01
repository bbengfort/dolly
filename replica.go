package dolly

import (
	"fmt"
	"time"

	zmq "github.com/pebbe/zmq4"
)

// Replica defines a peer on the network that can respond to Get requests
// and synchronizes state by subscribing to the leader.
type Replica struct {
	PID       uint16 `json:"pid"`       // the precedence id of the peer
	Name      string `json:"name"`      // unique name of the peer
	Addr      string `json:"address"`   // the network address of the peer
	Host      string `json:"host"`      // the hostname of the peer
	IPAddr    string `json:"ipaddr"`    // the ip address of the peer
	Updates   uint16 `json:"updates"`   // the port the replica publishes updates on
	Snapshots uint16 `json:"snapshots"` // the port the replica fetches snapshots on
	Requests  uint16 `json:"requests"`  // the port the replica handles requests on

	store     map[string]*Message // the key/value store representing state
	sequence  uint64              // the order of states as applied
	context   *zmq.Context        // the zmq context to create sockets with
	updates   *zmq.Socket         // socket to bind PUB/SUB on
	snapshots *zmq.Socket         // socket to bind ROUTER/DEALER on
	requests  *zmq.Socket         // socket to bind ROUTER on for clients
}

// Serve requests and subscribe to the leader to get updates.
func (r *Replica) Serve(leader *Replica, ctx *zmq.Context, echan chan<- error) {
	var err error

	// Initialize the store and save state
	r.context = ctx
	r.store = make(map[string]*Message)

	// Connect to the leader
	if err = r.Connect(leader); err != nil {
		echan <- err
		return
	}

	// Bind the requests handler
	if err = r.Bind(); err != nil {
		echan <- err
		return
	}

	// Send snapshot request to get up to date with the leader
	if err = r.Snapshot(); err != nil {
		echan <- err
		return
	}

	// Create a poller to handle updates and requests
	poller := zmq.NewPoller()
	poller.Add(r.updates, zmq.POLLIN)
	poller.Add(r.requests, zmq.POLLIN)

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
			if item.Socket == r.requests {
				if err := r.onRequests(); err != nil {
					echan <- err
					return
				}
			}

			// Handle Updates
			if item.Socket == r.updates {
				if err := r.onUpdates(); err != nil {
					echan <- err
					return
				}
			}

		}
	}
}

// Connect the sockets to the leader.
func (r *Replica) Connect(leader *Replica) (err error) {
	// Create the snapshots socket
	if r.snapshots, err = r.context.NewSocket(zmq.DEALER); err != nil {
		return err
	}
	if err = r.snapshots.SetLinger(0); err != nil {
		return err
	}
	endpoint := fmt.Sprintf("tcp://%s:%d", leader.Addr, leader.Snapshots)
	if err = r.snapshots.Connect(endpoint); err != nil {
		return err
	}
	info("connected snapshots DEALER socket to %s", endpoint)

	// Create the updates socket
	if r.updates, err = r.context.NewSocket(zmq.SUB); err != nil {
		return err
	}
	if err = r.updates.SetLinger(0); err != nil {
		return err
	}
	if err = r.updates.SetSubscribe(""); err != nil {
		return err
	}
	endpoint = fmt.Sprintf("tcp://%s:%d", leader.Addr, leader.Updates)
	if err = r.updates.Connect(endpoint); err != nil {
		return err
	}
	info("connected updates SUB socket to %s", endpoint)

	return nil
}

// Bind the requests endpoint
func (r *Replica) Bind() (err error) {
	// Create the requests socket
	if r.requests, err = r.context.NewSocket(zmq.ROUTER); err != nil {
		return err
	}
	endpoint := fmt.Sprintf("tcp://*:%d", r.Requests)
	if err = r.requests.Bind(endpoint); err != nil {
		return err
	}
	info("bound requests ROUTER socket to %s", endpoint)

	return nil
}

// Snapshot sends a snapshot request to the server to become up to date.
func (r *Replica) Snapshot() (err error) {
	req := &Message{
		method:   MethodSnapshot,
		sequence: r.sequence,
		key:      "",
		body:     nil,
	}

	// Send the snapshot request
	if err := req.Send(r.snapshots, nil); err != nil {
		return err
	}

	// Handle all the snapshots being sent back
	keys := 0
	for {
		msg, _, err := RecvMessage(r.snapshots, false)
		if err != nil {
			return err
		}

		// If this is the terminate message then collect sequence
		if msg.method == MethodTerm {
			r.sequence = msg.sequence
			info("received %d keys and up to date with snapshot %d", keys, r.sequence)
			return nil
		}

		// Otherwise handle the update
		keys++
		r.store[msg.key] = msg
	}
}

// Handle a request from a client.
func (r *Replica) onRequests() error {
	// Get the message from the socket
	msg, route, err := RecvMessage(r.requests, true)
	if err != nil {
		return err
	}

	// Mux the request correctly
	switch msg.method {
	case MethodGet:
		return r.onGet(msg, route)
	case MethodPut:
		return r.onPut(msg, route)
	default:
		return fmt.Errorf("unknown request method %s", msg.method)
	}
}

// Handle an update from the leader.
func (r *Replica) onUpdates() error {
	msg, _, err := RecvMessage(r.updates, false)
	if err != nil {
		return err
	}

	// Discard out of sequence messages
	// TODO: handle catchup better
	if msg.sequence > r.sequence {
		r.sequence = msg.sequence
		r.store[msg.key] = msg
		info("received update to state %d %s=%s", msg.sequence, msg.key, msg.body)
	}

	return nil
}

// Handle a Get request from a client
func (r *Replica) onGet(msg *Message, route []byte) error {
	var rep *Message
	var ok bool

	// Just send the local state back
	rep, ok = r.store[msg.key]
	if !ok {
		rep = &Message{
			method:   MethodError,
			sequence: r.sequence,
			key:      msg.key,
			body:     []byte("key not found"),
		}
	}

	// Send the message back
	return rep.Send(r.requests, route)
}

// Handle a Put request from a client
func (r *Replica) onPut(msg *Message, route []byte) error {
	rep := &Message{
		method:   MethodError,
		sequence: r.sequence,
		key:      msg.key,
		body:     []byte(fmt.Sprintf("not the leader cannot put value")),
	}

	return rep.Send(r.requests, route)
}
