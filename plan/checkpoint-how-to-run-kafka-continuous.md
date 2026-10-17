# How to run the pipeline (as it stands today)

> Updated 2026-10-11. This documents the **setup that works today** (EventSim → Kafka → peek consumer). The target architecture — MinIO, Kafka Connect, PyFlink, ClickHouse, dbt, dashboard — lives in [`CHECKPOINT.md`](./CHECKPOINT.md). This file grows as each phase lands.

## What works today

- Modernized EventSim to run on **Java 21** and stream into **Kafka 4.3.1** (KRaft mode).
- Fixed EventSim's `--continuous` flag so it streams at real-time pace (the original compared times backwards).
- `producer.py` — a small steady test stream. (EventSim is the official source.)
- `consumer.py` — a **peek tool**: prints events from a topic so we can see what is flowing. It is **not** the pipeline's processor — PyFlink takes that role in phase P2.
- Python venv (`.venv`) with `confluent-kafka`. It will be recreated on **Python 3.12** in phase P0 (PyFlink supports 3.9–3.12 only).
- **Kafka Connect is already installed** with Kafka (`bin/connect-standalone.sh`, `bin/connect-distributed.sh`). Not used yet — it arrives in phase P1 for the MinIO archive.

## How to start it (2 terminals to run, 1 optional to watch)

### 1. Start Kafka (leave it running)
```bash
cd ~/kafka/kafka_2.13-4.3.1
bin/kafka-server-start.sh config/server.properties
```
Kafka's data now lives in `~/kafka/kafka_2.13-4.3.1/data/kraft-combined-logs` (moved out of
`/tmp`), so it survives WSL restarts and never needs re-formatting.

### 2. Start the stream / producer (leave it running)
```bash
~/Spotify-Streaming-Intelligence-Platform/eventsim/bin/eventsim -c examples/example-config.json --continuous --nusers 1800 \
  --start-time "$(date '+%Y-%m-%dT%H:%M:%S')" \
  --end-time 2099-01-01T00:00:00 \
  --kafkaBrokerList localhost:9092 --kafkaTopic spotify-events
```
The launcher works from any directory (it `cd`s into `eventsim/` itself). The
`data/*.gz` files must exist in `eventsim/data/` — they are not committed.
- `--continuous` = real-time pace. Without it, EventSim dumps every event instantly (a burst).
- `--end-time 2099-01-01` = far future, so it never stops on its own → runs until Ctrl+C.
- `--nusers 1800` ≈ 2 events/sec. (Lower it for a slower stream.)

### 3. Peek at the stream (optional, separate terminal)
```bash
cd ~/Spotify-Streaming-Intelligence-Platform
./.venv/bin/python consumer.py
```
This just prints events so you can see them — a debugging window, not part of the pipeline.

To stop: `Ctrl+C` in each terminal.

## How much data does it produce?

The rate depends on `--nusers` and the config's `beta`. With the current config (`beta = 1800`):

| `--nusers` | approx events/sec |
|---|---|
| 500 | ~0.6–1.1 (measured) |
| 1,800 | ~2.2 (measured) |

Rule of thumb: `rate ≈ users × 10.7 / (1800 + beta)` events/sec. It drifts up and down with the
day/night cycle, so treat it as an estimate and measure the topic offset to confirm.

## Notes on realism and randomness

- **The rate is set by `beta`, not hardcoded.** `beta` = the *average* time between a user's sessions. We changed it from the shipped `604800` (7 days — "visit once a week") to **`1800`** (30 min) in `examples/example-config.json`, so a modest user count gives a steady stream.
- **To go faster or slower**, change `beta` or `--nusers`. Lower `beta` = users return sooner = more events. (Set `beta` back to `604800.0` to restore EventSim's shipped behaviour.)
- **User behavior is already random per user.** `beta` is only the average; each user's actual next visit is a random draw, so some return in 20 minutes, some in a few hours.
- **To make it unpredictable every run**, change the seed: `--randomseed $RANDOM`. The example locks the seed to `1`, which makes every run identical.

## What's next (from CHECKPOINT.md)

1. **P0** — move Python to 3.12.
2. **P1** — install MinIO; archive raw events there with a Kafka Connect sink.
3. **P2** — add PyFlink; process events into a second topic.
4. **P3** — install ClickHouse; load processed events there with a Kafka Connect sink.

Until then, the two terminals above (Kafka + EventSim) plus the optional peek are the entire working setup.
