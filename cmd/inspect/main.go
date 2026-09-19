// Command inspect makes the system observable without reading its source.
//
// Three subcommands, each answering a question the code otherwise only
// answers by being read:
//
//	inspect chunks <clip>   What does the gateway DO to this audio?
//	                        Offline — runs internal/audio directly, no
//	                        stack needed. Frame-by-frame VAD decisions,
//	                        where chunks get cut, and why.
//
//	inspect trace <clip>    Where does the TIME go?
//	                        Live — one real session against a running
//	                        gateway, every event stamped and attributed.
//
//	inspect fleet           What is the fleet, right now?
//	                        Live — each worker's identity, capability,
//	                        health and load, straight from the router.
//
// `chunks` is the one to start with: it needs nothing running, and the
// chunking policy is where most of this system's behaviour originates.
package main

import (
	"fmt"
	"os"
)

func usage() {
	fmt.Fprint(os.Stderr, `usage: inspect <command> [args]

  chunks <clip.wav>    offline: frames -> VAD -> chunk cuts, with timings
  trace  [clip.wav]    live: one session end to end, every event stamped
  fleet                live: worker identity, capability, health, load

environment:
  GATEWAY_WS_URL       default ws://localhost:7070/ws
  GATEWAY_DEBUG_URL    default http://localhost:7000

examples:
  inspect chunks corpus/large/dialogue/dialogue_0001.wav
  inspect trace
  GATEWAY_DEBUG_URL=http://localhost:17000 inspect fleet
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "chunks":
		if len(os.Args) < 3 {
			usage()
			os.Exit(2)
		}
		err = cmdChunks(os.Args[2])
	case "trace":
		clip := ""
		if len(os.Args) > 2 {
			clip = os.Args[2]
		}
		err = cmdTrace(clip)
	case "fleet":
		err = cmdFleet()
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "inspect %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
