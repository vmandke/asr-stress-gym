// Command loadgen is the paced synthetic client described in
// docs/build-plan.md "The load generator". It imports internal/wire
// directly so the load generator and the gateway never drift onto two
// different codecs.
//
// Not yet implemented — lands at M7 (docs/implementation-plan.md). This
// stub exists so `go build ./...` and the compose "loadgen" service (which
// starts idle and is driven from the dashboard) both work today.
package main

import "log"

func main() {
	log.Println("loadgen: not yet implemented (M7) — idling")
	select {}
}
