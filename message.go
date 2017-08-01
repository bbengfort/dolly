package dolly

import (
	"encoding/binary"

	zmq "github.com/pebbe/zmq4"
)

// RecvMessage off the socket, serializing correctly. If route is true,
// then the message is read with the identity, otherwise it is treated as a
// subscription message.
func RecvMessage(sock *zmq.Socket, route bool) (*Message, []byte, error) {
	parts, err := sock.RecvMessageBytes(0)
	if err != nil {
		return nil, nil, err
	}

	var identity []byte
	if route {
		identity = parts[0]
		parts = parts[1:]
	}

	message := &Message{
		method:   string(parts[0]),
		sequence: binary.LittleEndian.Uint64(parts[1]),
		key:      string(parts[2]),
		body:     parts[3],
	}

	return message, identity, nil
}

// Message represents a message that can be read off the wire.
type Message struct {
	method   string
	sequence uint64
	key      string
	body     []byte
}

// Send the message on the socket
func (m *Message) Send(sock *zmq.Socket, route []byte) error {
	// Convert the sequence into bytes
	seq := make([]byte, 8)
	binary.LittleEndian.PutUint64(seq, m.sequence)

	// If we have the identity, send that first
	if route != nil {
		sock.SendBytes(route, zmq.SNDMORE)
	}

	// Send the message on the wire
	_, err := sock.SendMessageDontwait([]byte(m.method), seq, []byte(m.key), m.body)
	return err
}
