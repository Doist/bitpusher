# bitpusher

A small UDP→Redis forwarder sidecar: it receives msgpack `{id, ev[]}` packets on a
UDP socket, deduplicates events over a 5-second window, and writes them as bits into
a [bitmapist](https://github.com/Doist/bitmapist) Redis instance (`SETBIT` against
day/week/month keys).

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-l` | `localhost:25800` | UDP address to listen on |
| `-b` | `localhost:6379` | bitmapist (Redis) address |
| `-statsd` | _(empty)_ | DogStatsD address for metrics, e.g. `localhost:8125`. Empty disables metrics. |
| `-project` | _(empty)_ | product this deployment serves, added as a `project:<value>` metric tag, e.g. `todoist`. |

## Metrics

When `-statsd` is set, bitpusher pushes the following metrics to the Datadog agent
over DogStatsD (UDP). The names are **product-agnostic** (`bitpusher.*`); the product
is a tag so one dashboard/monitor can slice across products. Every metric is tagged
`service:bitpusher`, `version:<build>`, `project:<value>` (from `-project`), and
`env:<from the agent>`.

| Metric | Type | Meaning |
|--------|------|---------|
| `bitpusher.events_received` | count | events parsed from incoming UDP packets |
| `bitpusher.events_flushed`  | count | unique events written to bitmapist per 5s flush |
| `bitpusher.events_dropped`  | count | events discarded because the internal queue was full |
| `bitpusher.decode_errors`   | count | UDP packets that failed msgpack decoding |
| `bitpusher.queue_depth`     | gauge | internal queue length, sampled each flush tick |

Metrics are fire-and-forget over UDP: if the agent is unreachable, bitpusher drops
metrics rather than blocking or failing.

Each deployment passes its own `-project` (e.g. `todoist`, `comms`, `automations`), so
the same image and metric names serve every product — slice by the `project` tag in Datadog.
