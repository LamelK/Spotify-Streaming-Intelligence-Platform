# Checkpoint: How to run Kafka continuous

## What we've achieved

- Modernized EventSim to run on **Java 21** and stream into **Kafka 4.3.1** (KRaft mode).
- Fixed EventSim's `--continuous` flag so it streams at real-time pace (the original compared times backwards).
- Wrote our own `consumer.py` (reads events) and `producer.py` (steady test stream).
- Set up a Python venv (`.venv`) with `confluent-kafka`.

## How to run the live stream (next 24 hours)

1. Start Kafka (leave it running):
   ```bash
   cd ~/kafka/kafka_2.13-4.3.1
   bin/kafka-server-start.sh config/server.properties
   ```

2. Start the consumer (second terminal):
   ```bash
   cd ~/Spotify-Streaming-Intelligence-Platform
   source .venv/bin/activate
   python consumer.py
   ```

3. Stream events in real time (third terminal). Runs until Ctrl+C:
   ```bash
   cd ~/eventsim
   ./bin/eventsim -c examples/example-config.json --from 0 --to -1 --continuous --nusers 10000 --kafkaBrokerList localhost:9092 --kafkaTopic spotify-events
   ```

## How much data does it produce?

EventSim is realistic, so the stream is sparse: **~2.2 events per day, per user**.

| `--nusers` | events per minute | pace |
|---|---|---|
| 1 (default) | 0.0015 | ~1 event every 11 hours |
| 50 | 0.08 | ~1 event every 13 minutes |
| 600 | 0.9 | ~1 event per minute |
| 10,000 | 15 | ~1 event every 4 seconds |
| 38,000 | 60 | ~1 event per second |

## Notes on realism and randomness

- **The "~2.2 events/day" is not hardcoded.** It comes from `"beta" : 604800` (7 days) in the config — the *average* time between a user's sessions. The example default is tuned for a website (visit once a week), not a music app.
- **To make users come back more often**, lower `beta` (e.g. `21600` = 6 hours → a few sessions a day). Same fake app, just a different setting.
- **User behavior is already random per user.** `beta` is only the average; each user's actual next visit is a random draw, so some return in 2 days, some in 20.
- **To make it unpredictable every run**, change the seed: `--randomseed $RANDOM`. The example locks the seed to `1`, which makes every run identical.
