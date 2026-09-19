// Package wire implements the client<->gateway binary frame codec defined in
// docs/PROTOCOL.md. Written at M1, after the protocol doc, not before.
//
// loadgen (cmd/loadgen) imports this package directly so the load generator
// and the gateway can never drift onto two different codecs.
package wire
