# Bifrost

Routes **offline** transcription jobs across the whole worker fleet, with a
fallback chain. Partials never come through here, and online finals do not
either unless you ask for it.

| Traffic | Route | Why |
|---|---|---|
| Partials during speech | direct to the pinned worker | stateful, latency-critical; routing it here is *structurally* impossible — see §10 |
| Online finals | direct (opt in with `BIFROST_FINALS=1`) | the pinned worker is holding inference state built from this exact audio. A direct `flush` finalizes it in **9ms**; a stateless Bifrost request discards it and re-transcribes in **265ms** |
| Offline jobs | **Bifrost** | no partials, so no hot state to discard; no latency budget, so retry-with-backoff is *correct*; nothing for the router to pin or prefer |

That middle row is the one worth internalising. build-plan.md argues a
complete utterance is a discrete stateless request, so finals belong here.
True of the **audio**, false of the **worker** — which by then holds
accumulated encoder and predictor context built from exactly that audio.
`/v1/audio/transcriptions` is stateless by construction and throws it away.

## Off by default

```bash
make up                                  # no Bifrost; finals go direct
docker compose --profile bifrost up -d   # Bifrost on :8080, gateway routes finals through it
```

The gateway reads `BIFROST_URL`. Unset means the client is `nil` and every
final takes the direct path — which is also what happens on *any* Bifrost
failure. Enabling it can cost latency; it cannot cost a transcript.

## All five workers, each its own provider

The plan originally named only worker-d. A gateway fronting one provider
makes Bifrost's actual features — fallback, weighted balancing, health
tracking — inert, so every worker is registered instead.

**They have to be separate providers, not separate keys of one provider.**
`base_url` lives in `network_config`, which is per *provider*; keys under a
provider share it. Registering five workers as five keys of `openai` looked
right and silently sent every request to `api.openai.com`, which came back
as a 401 about an invalid API key — the first sign that the config was
being ignored rather than applied. Each worker is therefore its own
provider with `custom_provider_config.base_provider_type: "openai"`.

Route by naming the provider in the model string: `worker-a/whisper-1`.

### Fallbacks are the one feature doing real work

`BIFROST_FALLBACKS` is an ordered chain Bifrost walks when the primary
fails, each provider getting its own retry budget. This covers a class the
gateway deliberately does not: `internal/coord` rebuilds *streaming*
sessions and has no equivalent for a one-shot transcription whose backend
is down.

The published docs only describe `fallbacks` for chat completions, so it
was tested rather than assumed — SIGKILL the primary worker, send a request
naming it with a chain, and a transcript comes back served by the next
provider. It works on `/v1/audio/transcriptions`.

Key rotation and weighted balancing remain inert here and cannot be fixed
locally: there are no keys and no quotas, because these are containers on
one host.

`value` is a placeholder. These are local containers with no auth; Bifrost
requires the field, the workers ignore it, and nothing leaves the host.

### `allow_private_network` is required

Bifrost refuses to connect to private IPs by default — an SSRF guard, and a
sensible one. Every `base_url` here is a compose DNS name resolving to a
172.x address, so without this the request fails with
`connection to private IP 172.19.0.4 is not allowed` before it ever leaves
the proxy. It is set per provider, next to `base_url`.

## Why local, not cloud

Bifrost routes to external providers by default. Pointing it there would
break the hermetic `docker compose up` — no keys, no internet, and no
benchmark number that depends on somebody else's rate limits. Every
`base_url` here is a compose DNS name on the internal network.

The workers expose `POST /v1/audio/transcriptions` (OpenAI-shaped,
multipart) for exactly this. It is stateless by construction: fresh
adapter state, infer, finalize, discard — it never touches the handle→state
map that the streaming path depends on.
