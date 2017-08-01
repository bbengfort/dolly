# Dolly

**ZMQ centralized key/value clone model with PUB/SUB**

This simple network of peers replicates a key/value store using PUB/SUB ZMQ sockets. A client can make a GET request to any replica and receive that replica's current value of the key. Consistency is maintained by serializing all PUT requests through the leader -- which is selected as the replica with the lowest PID value. If the leader goes down, GET requests can still be made, but all PUTs are dropped until the leader comes back online.

## Getting Started

To run the code, first get and install the package:

    $ go get github.com/bbengfort/dolly

Note that this will require the installation of ZMQ, which can be tricky depending on your environment. Then create a `peers.json` file to define the replica network:

```json
[
  {
    "pid": 1,
    "name": "alpha",
    "address": "localhost",
    "host": "apollo",
    "ipaddr": "127.0.0.1",
    "updates": 3264,
    "snapshots": 3265,
    "requests": 3266
  },
  {
    "pid": 2,
    "name": "bravo",
    "address": "localhost",
    "host": "apollo",
    "ipaddr": "127.0.0.1",
    "updates": 3267,
    "snapshots": 3268,
    "requests": 3269
  },
  {
    "pid": 3,
    "name": "charlie",
    "address": "localhost",
    "host": "apollo",
    "ipaddr": "127.0.0.1",
    "updates": 3270,
    "snapshots": 3271,
    "requests": 3272
  }
]
```

Note that each replica requires three ports to bind or connect sockets on.

- `updates`: the port where the leader binds PUB and replicas connect SUB to get key/value updates.
- `snapshots`: the port where the leader binds ROUTER and replicas connect DEALER so that a late replica can catch up with the leader.  
- `requests`: the port where the leader binds PULL and replicas and clients bind PUSH so that the leader can have its state updated. 
