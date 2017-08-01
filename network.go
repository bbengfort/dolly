package dolly

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	zmq "github.com/pebbe/zmq4"
)

// Initialize the package and random numbers, etc.
func init() {
	// Set the random seed to something different each time.
	rand.Seed(time.Now().Unix())

	// Initialize our debug logging with our prefix
	logger = log.New(os.Stdout, "[dolly] ", log.Lmicroseconds)

}

// Method constants
const (
	MethodGet      = "Get"
	MethodPut      = "Put"
	MethodError    = "Error"
	MethodSnapshot = "Snapshot"
	MethodTerm     = "Terminate"
)

// New creates a Dolly network from the specified peers.json configuration.
func New(peers string) (*Network, error) {

	// Create the network
	network := &Network{
		peers: make(Replicas, 0),
	}

	// Load peers from file
	data, err := ioutil.ReadFile(peers)
	if err != nil {
		return nil, err
	}

	// Load the peers JSON file
	if err = json.Unmarshal(data, &network.peers); err != nil {
		return nil, err
	}

	// Look up the leader for reference
	if network.leader, err = network.peers.Leader(); err != nil {
		return nil, err
	}

	return network, err
}

// Network defines all sockets for the local process.
type Network struct {
	local   *Replica
	leader  *Replica
	peers   Replicas
	context *zmq.Context
}

// Run a replica or leader with the specified name.
func (n *Network) Run(name string) (err error) {
	// Ensure we clean up after ourselves
	defer zmq.Term()

	// Create the context
	if n.context, err = zmq.NewContext(); err != nil {
		return err
	}

	// Look up the local replica
	if n.local, err = n.peers.Get(name); err != nil {
		return err
	}

	// Create the error channel and signal handlers
	echan := make(chan error)
	notify := make(chan os.Signal)
	signal.Notify(notify)

	// Figure out if we're the leader or not
	if n.local == n.leader {
		// Run as leader
		leader := &Leader{*n.local}
		go leader.Serve(n.context, echan)
	} else {
		// Run as replica
		go n.local.Serve(n.leader, n.context, echan)
	}

	// Listen for errors
	select {
	case err := <-echan:
		return err
	case <-notify:
		os.Exit(0)
	}

	return nil
}

// Client returns a client connection for the specified replica.
func (n *Network) Client(name string) (*Client, error) {
	replica, err := n.peers.Get(name)
	if err != nil {
		return nil, err
	}

	return &Client{replica: replica}, nil
}

// Replicas represents a collection of replicas.
type Replicas []*Replica

// Leader returns the replica that has the lowest PID.
func (r Replicas) Leader() (*Replica, error) {
	var err error
	var leader *Replica

	for _, replica := range r {
		if leader == nil {
			leader = replica
		} else if replica.PID == leader.PID {
			return nil, errors.New("conflicting PID for replicas")
		} else if replica.PID < leader.PID {
			leader = replica
		}
	}

	if leader == nil {
		err = errors.New("no replicas configured")
	}
	return leader, err
}

// Get a replica by name, returns nil if not found.
func (r Replicas) Get(name string) (*Replica, error) {
	for _, replica := range r {
		if replica.Name == name {
			return replica, nil
		}
	}

	return nil, fmt.Errorf("could not find replica named %s", name)
}
