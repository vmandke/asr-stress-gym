# Live chaos checks

Run `make live` first, then run:

```bash
make chaos
```

It exercises the deployed stateless Bifrost path, not the retired fixed
`worker-*-2` Compose topology.

1. It creates a zipformer peer through the dashboard fleet API if one is not
   already routeable. It temporarily drains every other worker, opens one
   stream so `worker-zip-1` is its Bifrost primary, re-enables the compatible
   peer, and kills that primary. The stream must complete with zero client
   errors and zero duplicate finals. This proves the peer can take the
   Bifrost fallback request while the KV reference stays in the zip tier.
2. It restarts the gateway with a deliberately small admission ceiling (12 by
   default) and opens 150% of that number. New sessions must receive
   `overloaded`; admitted sessions must complete without errors or duplicate
   finals. The normal capacity settings are restored before the command exits.

The dynamic worker container created by the first check remains running. The
second check deliberately recreates the gateway, whose dynamic-fleet
membership is presently in memory; re-add that worker from the dashboard if
you want it routeable afterwards. All worker drains and injected request
faults are cleared, and the seed worker is restored on exit. `CHAOS_CEILING`
and `CHAOS_SOFT` can override the small admission envelope.
