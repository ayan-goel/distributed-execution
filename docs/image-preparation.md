# Pinned image preparation

`DockerRuntime::prepare_image` accepts a validated `ExecutionSpec`. It inspects the
exact admitted image digest and reuses an existing cache entry. Only an explicit
Docker 404 starts a pull; other inspection failures return their bounded category.
The pull uses that same digest as `fromImage`, with no mutable tag, import URL,
user-controlled Docker options, or credentials from the job document.

The preparation operation has a startup-sized timeout and consumes at most 65,536
progress records without retaining or printing registry diagnostics. An error
stream fails preparation. After the stream ends, a second inspection of the exact
digest must succeed. Partial download or stream completion alone cannot authorize
container creation. Progress metadata is decoded by Bollard; the record bound does
not establish a raw-byte cap on an individual trusted daemon response.

This operation creates no containers and grants no execution authority. The agent
supervises it with the assignment's live authority and original phase deadline,
before input staging and container launch. Cancellation acknowledges that no
container was created. Observed preparation errors seal a durable
`RUNTIME_UNAVAILABLE` result, preserving its completion ID on replay. Expired or
fenced authority interrupts preparation and exits through existing reconciliation;
this slice does not establish complete phase-timeout failure classification.
Docker may retain downloaded layers after cancellation; preparation does not prune
the shared image cache. Private registry credential references remain separate work.

The component is verified with isolated Unix-socket Docker fixtures for cache reuse,
exact digest pulls, post-pull verification, daemon errors, stream errors, and a
stalled pull. Real cached-image and missing-image agent gates passed with CLI
submission, dataset staging, live logs, verified output publication/download, and
local cleanup. The cold fixture uses an absent public BusyBox digest without
manually pulling it, and removes only its newly introduced image reference after
container cleanup. This verifies one local Docker engine, not independent Linux
hosts or private registry authentication.

Sources: the pinned [Bollard 0.21.1 image API](https://docs.rs/bollard/0.21.1/bollard/struct.Docker.html#method.create_image)
and [Docker's digest pull contract](https://docs.docker.com/reference/cli/docker/image/pull/#pull-an-image-by-digest-immutable-identifier).
