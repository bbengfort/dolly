package dolly

import (
	"fmt"
	"time"

	zmq "github.com/pebbe/zmq4"
)

// Client connects to the a replica and makes requests.
type Client struct {
	replica *Replica
	context *zmq.Context
	socket  *zmq.Socket
}

// Connect all sockets from the client to the leader.
func (c *Client) Connect() (err error) {
	if c.context, err = zmq.NewContext(); err != nil {
		return err
	}

	if c.socket, err = c.context.NewSocket(zmq.DEALER); err != nil {
		return err
	}

	endpoint := fmt.Sprintf("tcp://%s:%d", c.replica.Addr, c.replica.Requests)
	return c.socket.Connect(endpoint)
}

// Close all sockets on the client and stop resonding to requests.
func (c *Client) Close() error {
	return zmq.Term()
}

// Get the value for the specified key
func (c *Client) Get(key string, timeout time.Duration) error {
	msg := &Message{
		method:   MethodGet,
		sequence: 0,
		key:      key,
		body:     nil,
	}

	if err := msg.Send(c.socket, nil); err != nil {
		return err
	}

	rep, _, err := RecvMessage(c.socket, false)
	if err != nil {
		return err
	}

	if rep.method == MethodError {
		fmt.Printf("could not get %s: %s\n", rep.key, string(rep.body))
	} else {
		fmt.Printf("%s = %s (state %d)\n", rep.key, string(rep.body), rep.sequence)
	}

	return nil
}

// Put a value for the specified key.
func (c *Client) Put(key, val string, timeout time.Duration) error {
	msg := &Message{
		method:   MethodPut,
		sequence: 0,
		key:      key,
		body:     []byte(val),
	}

	if err := msg.Send(c.socket, nil); err != nil {
		return err
	}

	rep, _, err := RecvMessage(c.socket, false)
	if err != nil {
		return err
	}

	if rep.method == MethodError {
		fmt.Printf("could not put %s: %s\n", rep.key, string(rep.body))
	} else {
		fmt.Printf("%s set in state %d\n", rep.key, rep.sequence)
	}
	return nil
}
