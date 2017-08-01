package main

import (
	"fmt"
	"os"
	"time"

	"github.com/bbengfort/dolly"
	"github.com/joho/godotenv"
	"github.com/pebbe/zmq4"
	"github.com/urfave/cli"
)

//===========================================================================
// Main Method
//===========================================================================

func main() {

	// Load the .env file if it exists
	godotenv.Load()

	// Instantiate the command line application
	app := cli.NewApp()
	app.Name = "dolly"
	app.Version = "0.1"
	app.Usage = "centralized key/value replication"

	// Define commands available to the application
	app.Commands = []cli.Command{
		{
			Name:     "serve",
			Usage:    "run the key/val replicas",
			Category: "server",
			Action:   serve,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "p, peers",
					Usage:  "path to peers configuration",
					Value:  "",
					EnvVar: "PEERS_PATH",
				},
				cli.StringFlag{
					Name:   "n, name",
					Usage:  "name of the replica to initialize",
					Value:  "",
					EnvVar: "ALIA_REPLICA_NAME",
				},
				cli.StringFlag{
					Name:   "u, uptime",
					Usage:  "pass a parsable duration to shut the server down after",
					EnvVar: "ALIA_SERVER_UPTIME",
				},
				cli.UintFlag{
					Name:   "verbosity",
					Usage:  "set log level from 0-4, lower is more verbose",
					Value:  2,
					EnvVar: "ALIA_VERBOSITY",
				},
			},
		},
		{
			Name:     "get",
			Usage:    "get a value for the specified key(s)",
			Category: "client",
			Action:   get,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "p, peers",
					Usage:  "path to peers configuration",
					Value:  "",
					EnvVar: "PEERS_PATH",
				},
				cli.StringFlag{
					Name:   "n, name",
					Usage:  "name of the replica to connect to",
					Value:  "",
					EnvVar: "KILO_LEADER_NAME",
				},
				cli.StringFlag{
					Name:   "t, timeout",
					Usage:  "recv timeout for each message",
					Value:  "2s",
					EnvVar: "KILO_TIMEOUT",
				},
			},
		},
		{
			Name:     "put",
			Usage:    "put a value for the specified key",
			Category: "client",
			Action:   put,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "p, peers",
					Usage:  "path to peers configuration",
					Value:  "",
					EnvVar: "PEERS_PATH",
				},
				cli.StringFlag{
					Name:   "n, name",
					Usage:  "name of the replica to connect to",
					Value:  "",
					EnvVar: "KILO_LEADER_NAME",
				},
				cli.StringFlag{
					Name:   "t, timeout",
					Usage:  "recv timeout for each message",
					Value:  "2s",
					EnvVar: "KILO_TIMEOUT",
				},
			},
		},
	}

	// Run the CLI program
	app.Run(os.Args)
}

//===========================================================================
// Helper Functions
//===========================================================================

func exit(err error) error {
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("fatal error: %s", err), 1)
	}
	return nil
}

//===========================================================================
// Server Commands
//===========================================================================

func serve(c *cli.Context) error {
	// Set the debug log level
	verbose := c.Uint("verbosity")
	dolly.SetLogLevel(uint8(verbose))

	// Create the network
	network, err := dolly.New(c.String("peers"))
	if err != nil {
		return exit(err)
	}

	// If uptime is specified, set a fixed duration for the server to run.
	if uptime := c.String("uptime"); uptime != "" {
		d, err := time.ParseDuration(uptime)
		if err != nil {
			return err
		}

		time.AfterFunc(d, func() {
			zmq4.Term()
			os.Exit(0)
		})
	}

	// Run the network server and broadcast clients
	if err := network.Run(c.String("name")); err != nil {
		return exit(err)
	}
	return nil
}

//===========================================================================
// Client Commands
//===========================================================================

func get(c *cli.Context) error {
	network, err := dolly.New(c.String("peers"))
	if err != nil {
		return exit(err)
	}

	client, err := network.Client(c.String("name"))
	if err != nil {
		return exit(err)
	}

	if err = client.Connect(); err != nil {
		return exit(err)
	}

	var timeout time.Duration
	if timeout, err = time.ParseDuration(c.String("timeout")); err != nil {
		return exit(err)
	}

	for _, key := range c.Args() {
		if err := client.Get(key, timeout); err != nil {
			return exit(err)
		}
	}

	return exit(client.Close())
}

func put(c *cli.Context) error {
	network, err := dolly.New(c.String("peers"))
	if err != nil {
		return exit(err)
	}

	client, err := network.Client(c.String("name"))
	if err != nil {
		return exit(err)
	}

	if err = client.Connect(); err != nil {
		return exit(err)
	}

	var timeout time.Duration
	if timeout, err = time.ParseDuration(c.String("timeout")); err != nil {
		return exit(err)
	}

	args := c.Args()
	if len(args) != 2 {
		return cli.NewExitError("specify the key then the value to put", 1)
	}

	if err := client.Put(args[0], args[1], timeout); err != nil {
		return exit(err)
	}

	return exit(client.Close())
}
