# Checkpoint — updated 2026-10-11

Resume by saying: **"continue from the checkpoint"**.

## Where we are

**Stage 0 COMPLETE — live streaming works.** EventSim streams in real time into Kafka 4.3.1 (topic `spotify-events`) at ~1–3 events/sec, and our `consumer.py` reads it. Verified end to end.

**2026-10-11 — the architecture changed.** We adopted a new target architecture and **stopped the PostgreSQL path**:

```text
EventSim → Kafka (spotify-events)
   ├─ Kafka Connect object-store sink → RustFS                   (durable raw archive)
   └─ PyFlink → Kafka (spotify-events-processed)
                → Kafka Connect ClickHouse sink → ClickHouse → dbt → dashboard
```

PostgreSQL 18.6 stays installed on the machine but is no longer part of the plan. The raw table was never created, so no real work was lost. Python moves from 3.14 to 3.12 (PyFlink supports 3.9–3.12 only).

**Nothing of the new stack is built yet.** RustFS, Kafka Connect, PyFlink, ClickHouse, dbt and the dashboard are all still to come. The work is planned in phases P0–P7 (see Next steps).

Our Python code: `consumer.py` and `producer.py` — **both written by the assistant (unprompted) in earlier sessions; see the working rule in Decisions.** `consumer.py` will be repurposed as a general "peek at any topic" tool and is no longer a pipeline component. `producer.py` stays as a dev/test stream.

Repo is on GitHub: `github.com/LamelK/Spotify-Streaming-Intelligence-Platform` (SSH-key auth).

The simulator now lives **inside the project** at [`eventsim/`](../eventsim) — a patched, vendored copy. The old clone at `~/eventsim` is a leftover and is no longer used. Kafka stays at `~/kafka/kafka_2.13-4.3.1`.

## Environment (verified on `melak@Lamel-Laptop`, WSL Ubuntu)

| Tool | Version | Note |
|---|---|---|
| Java | 8 (1.8.0_504) and 21 (21.0.12.1) | Java 21 is the default. Flink recommends Java 17; Java 21 is *experimental* for Flink (try 21 first, install 17 only if needed) |
| sbt | 1.11.7 | builds EventSim on Scala 2.12 |
| Kafka | 4.3.1 | installed at `~/kafka/kafka_2.13-4.3.1`, KRaft mode. **Kafka Connect ships with it**: `bin/connect-standalone.sh`, `bin/connect-distributed.sh` |
| **Python (project)** | **3.12.15** | installed with `uv` (Ubuntu 26.04 ships 3.14 only; uv installs into `~/.local/share/uv/python/`). The `.venv` is built on 3.12. Ubuntu's system Python 3.14.4 is untouched |
| **uv** | 0.13.0 | manages Python versions and the venv (`~/.local/bin/uv`) |
| **confluent-kafka** | **2.16.0** | the Kafka client used by `consumer.py` / `producer.py` |
| PostgreSQL | 18.6 | installed natively in WSL; **now unused** (kept, not deleted) |
| Docker | not installed | deliberate decision: not using it — still deferred to the polish stage |

- Project folder: `~/Spotify-Streaming-Intelligence-Platform`
- EventSim clone: `~/eventsim` (outside the project, third-party)
- Kafka home: `~/kafka/kafka_2.13-4.3.1`

## Freshness targets (agreed 2026-10-11)

- **Live page** — an event should appear within **30–60 seconds** of being generated.
- **Analytics page** — **dbt runs every 5 minutes**; the page shows the latest successful run (so at most ~5 min + run time old).

These two targets constrain the ClickHouse sink's flush/buffer settings and Flink's checkpoint interval. They are measured and recorded, never assumed.

## Target architecture (adopted 2026-10-11)

| Component | Plain-English job |
|---|---|
| **EventSim** | Generates fake Spotify events (a song play, a page view). |
| **Kafka** | Carries events and buffers them — the replay buffer. |
| **Kafka Connect** | Moves data between Kafka and other systems through a configured *connector*. Used twice: raw events → RustFS, processed events → ClickHouse. |
| **RustFS** | S3-compatible object store (Apache 2.0, written in Rust); keeps the original events beyond Kafka's retention window. |
| **PyFlink** | Continuous stream processing: validation, enrichment, later windowed metrics. |
| **ClickHouse** | Analytical database; answers the dashboard's queries quickly. |
| **dbt** | Builds tested, documented SQL models for historical analytics. |
| **Dashboard** | One app, two pages: Live (30–60 s) and Analytics (5 min). |

⚠️ This is the **target**, not what is built. See Next steps for the order of work.

## What was changed (EventSim modernization)

Five files changed in `~/eventsim`, plus two new dependencies. Everything else untouched.

1. `build.sbt` — `jollyday` 0.5.1 → 0.5.10; `kafka_2.10` 0.8.2.1 → `kafka-clients` 4.3.1; added `slf4j-nop` 1.7.36; added an assembly merge rule to discard `module-info.class`.
2. `TimeUtilities.scala` — `HolidayManager.getInstance(HolidayCalendar.UNITED_STATES)` (the no-arg call returned null under WSL's `C.UTF-8` locale).
3. `KafkaOutputStream.scala` — old `kafka.producer` API → modern `KafkaProducer` + `ProducerRecord`; added `close()`.
4. `Main.scala` — `metadata.broker.list` → `bootstrap.servers`, added key/value serializers. CLI flags unchanged.
5. `bin/eventsim` — fixed jar path (`scala-2.10` → `scala-2.12`), removed `-XX:+AggressiveOpts`, use `java` from PATH.

## Build / run / verify commands (all verified working)

Build EventSim (only after changing the Scala source; the jar is committed):
```bash
cd ~/Spotify-Streaming-Intelligence-Platform/eventsim
sbt assembly
cp target/scala-2.12/eventsim-assembly-1.0.jar dist/
```

Run to a file / stdout (no Kafka):
```bash
~/Spotify-Streaming-Intelligence-Platform/eventsim/bin/eventsim -c examples/example-config.json --from 1 --to 0 --nusers 2
```

Start Kafka (single node, KRaft; first time only format with `bin/kafka-storage.sh format -t <uuid> -c config/server.properties --standalone`):
```bash
cd ~/kafka/kafka_2.13-4.3.1
bin/kafka-server-start.sh config/server.properties
```
Kafka's data lives in `~/kafka/kafka_2.13-4.3.1/data/kraft-combined-logs` (moved out of `/tmp`), so it survives WSL restarts and never needs re-formatting.

Create the topic (only needed once):
```bash
cd ~/kafka/kafka_2.13-4.3.1
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic spotify-events --partitions 1 --replication-factor 1
```

Stream into Kafka — continuous / real-time (runs until Ctrl+C):
```bash
~/Spotify-Streaming-Intelligence-Platform/eventsim/bin/eventsim -c examples/example-config.json --continuous --nusers 1800 \
  --start-time "$(date '+%Y-%m-%dT%H:%M:%S')" \
  --end-time 2099-01-01T00:00:00 \
  --kafkaBrokerList localhost:9092 --kafkaTopic spotify-events
```
Why each flag:
- `--continuous` = real-time pacing. Without it, EventSim dumps all events as a burst.
- `--end-time 2099-01-01` = far future, so it never finishes on its own → runs until you press Ctrl+C.
- Rate ≈ `users × 10.7 / (1800 + beta)` events/sec. Current config (1800 users, beta=1800) → ~1–3/sec intended.
- **This is a daily average, not a steady rate.** Measured 2026-10-11: **16 events in 30 s ≈ 0.5/sec** during a quiet part of the simulated day. The day/night curve (`Damping=0.09375`) means the moment-to-moment rate drifts well below and above the average. Always measure, don't assume.

Measure the real rate (two counts, 30 s apart):
```bash
cd ~/kafka/kafka_2.13-4.3.1
bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic spotify-events
sleep 30
bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic spotify-events
```

See where a consumer group has read up to (LAG = backlog left):
```bash
cd ~/kafka/kafka_2.13-4.3.1
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group python-consumer-1
```

Check the broker is up and list topics:
```bash
cd ~/kafka/kafka_2.13-4.3.1
bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | head -5
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**Why a new consumer shows a burst of events, then slows down:** the consumer's settings say `auto.offset.reset: earliest`, so if the group has no saved position it reads from the **oldest** event in the topic. Everything accumulated in the topic during the pause is read as fast as possible (the burst), then it reaches "now" and reads at the producer's live rate (the slow drip). Kafka keeps the events, so nothing is lost while a consumer is stopped.

Peek at the stream with our own Python consumer:
```bash
cd ~/Spotify-Streaming-Intelligence-Platform
./.venv/bin/python consumer.py
```

Check the topic's offsets (message count / rate):
```bash
cd ~/kafka/kafka_2.13-4.3.1
bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic spotify-events
```

## Decisions and breakthroughs

### Current decisions (2026-10-11 architecture)

- **📌 Project decision — target architecture adopted (2026-10-11):** `EventSim → Kafka → {Connect → MinIO archive} + {PyFlink → processed topic → Connect → ClickHouse} → dbt → dashboard`. Reason: as a portfolio project this demonstrates the full data-engineering path — transport, stream processing, durable raw storage, an analytical database, modelling, and a dashboard. The earlier decision deferred this as "too big"; it is now chosen **deliberately**, and built in phases (P0–P7) so each piece is understood before the next. *(The archive target changed from MinIO to RustFS on 2026-10-17 — see the decision below.)*
- **📌 Project decision — freshness targets:** Live page 30–60 s; Analytics page refreshed by a 5-minute dbt run. These constrain sink buffering and checkpoint intervals (see P2/P3).
- **📌 Project decision — PostgreSQL leaves the plan (REVISED 2026-10-11).** Supersedes the 2026-10-04 decisions "warehouse = PostgreSQL", "ELT into PostgreSQL", "schema naming raw/staging/analytics", "one raw table", and "cleaning lives in dbt staging". Reason: ClickHouse replaces its role; keeping both would mean two databases doing one job. The PostgreSQL install stays on the machine, unused — no uninstall needed.
- **📌 Project decision — the streaming engine is PyFlink (REVISED 2026-10-11).** This reverses the 2026-10-04 decision to defer Flink/Bytewax/Quix Streams and use only our own Python consumer. Reason: we now want engine-level concepts (event-time windows, watermarks, checkpoints) in the project. Kept from before: this is the decision most likely to be revisited if PyFlink proves too heavy.
- **📌 Project decision — Python moves 3.14 → 3.12.** PyFlink supports 3.9–3.12 (`apache-flink` 2.2.1 ships `cp312` wheels, verified 2026-10-11). One environment for everything (consumer, PyFlink, dbt, dashboard).
- **📌 Project decision — the raw archive uses RustFS (2026-10-17).** RustFS is an Apache-2.0, S3-compatible object store written in Rust, published as a drop-in MinIO replacement. Reason: the target architecture needs an S3-compatible target for the Kafka Connect object-store sink, and MinIO's open-source edition is archived and no longer downloadable (`dl.min.io` → "410 Gone"). RustFS keeps both the S3 skill and the existing connector path, so P1 does not have to be redesigned. **Status: chosen, not yet installed or tested** — install method, version pin, single-node mode and the exact S3 endpoint must be verified at P1. Rejected: (a) plain files on disk — drops the S3 skill and the Connect sink; (b) Garage / SeaweedFS — viable, but further from being a MinIO drop-in; (c) skipping the archive — removes a planned component.
- **📌 Project decision — installs stay native in WSL; Docker still deferred.** Each new service (RustFS, ClickHouse) is installed like Kafka/PostgreSQL. Docker remains a polish-stage option.
- **📌 Project decision — Flink pinned to 2.2.x, local mode.** The Kafka connectors exist as `flink-connector-kafka` / `flink-sql-connector-kafka` **5.0.0-2.2**; there is **no connector for Flink 2.3** (checked 2026-10-11). Local mode = `pip install apache-flink`, Flink runs inside the job process; the same job can later run on a real cluster.
- **📌 Project decision — Kafka Connect runs in standalone mode** (one worker, config files) — enough for a single machine; distributed mode is a later option.
- **📌 Project decision — one processed topic first.** `spotify-events-processed`. Route events into more topics only when there is a clear reason (avoid a topic per event variation).
- **📌 Project decision — exactly-once is never claimed, only tested.** We test restart, duplicate and recovery behaviour and record what we actually observe.

### Standing decisions (still true from earlier stages)

- **📌 Project decision — Java 21 is the baseline** for EventSim (Java 8 stays installed, unused).
- **📌 Project decision — Kafka 4.3.1, KRaft mode (no ZooKeeper).**
- **📌 Project decision — EventSim is vendored in the repo (2026-10-17).** The patched copy lives in [`eventsim/`](../eventsim), with the built jar committed in `eventsim/dist/` (32 MB) and the 71 MB of `data/*.gz` excluded by `.gitignore`. Reason: one clone should hold everything needed to run, and nobody should have to patch a 2015 codebase to reproduce the stream. Rejected: a GitHub fork (a second repo to maintain) and a patch file only (fragile, and still needs an upstream clone). Consequence: **the repo copy is now the source of truth**; the old `~/eventsim` clone is unused. The `.gz` files are mandatory — EventSim crashes without them — so a setup step must fetch them.
- **📌 Project decision — keep EventSim's CLI and event shape unchanged.** Only the Kafka delivery layer and the minimal Java 21 fixes changed.
- **📌 Project decision — Kafka's data lives outside `/tmp`** (`~/kafka/kafka_2.13-4.3.1/data/kraft-combined-logs`). Reason: `/tmp` is wiped on WSL restart, which previously deleted Kafka's `meta.properties` and the topic.
- **📌 Project decision — the stream runs in EventSim `--continuous` mode** (real-time pacing), not a burst. Config: `beta` 604800 → 1800, run with `--nusers 1800`.
- **📌 Project decision — EventSim data is structurally messy, not clean.** Ragged schema (`artist`/`song`/`length` only on `NextSong`), empty-string `userId` + missing user fields for Guest/Logged Out, 404/307 statuses. Good raw material for validation and dbt tests.
- **📌 Working rule — Melak writes the project code; the assistant guides.** `consumer.py` and `producer.py` were created by the assistant (unprompted) in earlier sessions, which undermined his #1 goal (*understanding*). From now on: the assistant explains and reviews; Melak types.

### Breakthroughs

- **🎉 Breakthrough — Phase 0 done (2026-10-11): the project runs on Python 3.12, verified end to end.** Installed `uv` 0.13.0, installed Python 3.12.15 into the user's home folder (Ubuntu 26.04 only ships 3.14, and 3.12 is not in its repos), rebuilt `.venv` on 3.12, reinstalled `confluent-kafka` 2.16.0, and confirmed live events print from Kafka (`spotify-events`, offsets 27019 → 27035 over 30 s). Also: `plan/CHECKPOINT.md` rewritten for the new architecture, the how-to-run doc refreshed, the root `AGENTS.md` stack rules corrected, and the stale duplicate `plan/AGENTS.md` deleted.
- **🎉 Breakthrough — EventSim builds and runs on Java 21.** Streams real Spotify-shaped events into Kafka 4.3.1 (a 50-user / 14-day run streamed 1,566 events).
- **🎉 Breakthrough — the streaming pipeline runs live end to end:** EventSim → Kafka (`spotify-events`) → `consumer.py`, running until stopped.
- **🎉 Breakthrough — PostgreSQL 18.6 installed, database + login role created, raw schema designed** (now unused — kept for the record).

## Open questions (not yet answered)

- **Raw-archive tool (BLOCKER for P1)** — ✅ answered 2026-10-17: **RustFS** (Apache 2.0, S3-compatible, MinIO drop-in). MinIO itself is out: its open-source edition is archived and `dl.min.io` returns "410 Gone". Still to verify at P1: install method, version pin, single-node mode, S3 endpoint and credentials file for Kafka Connect. The archive is a side branch — no downstream component reads from it — so it does not block PyFlink, ClickHouse, dbt or the dashboard.
- **How to install Python 3.12** — ✅ answered: `uv` 0.13.0, Python 3.12.15.
- **Dashboard tool** — Streamlit recommended (Python, two pages, fastest path); alternatives: Grafana, Metabase. Decide in P6.
- **Where our modified EventSim will live / how it is owned** — ✅ answered 2026-10-17: **vendored inside the repo** at [`eventsim/`](../eventsim) (source + built jar; `data/*.gz` downloaded, not committed). See [`eventsim/PATCHES.md`](../eventsim/PATCHES.md).
- **`producer.py`** — keep as a dev/test smoke stream (current plan), or delete later.

## Next steps, in order

**Resume by phase.** Each phase ends with a working, tested slice, an updated checkpoint, and a git push.

- **P0 — Replace the plans + move to Python 3.12. ✅ DONE 2026-10-11.** (1) `CHECKPOINT.md` replaced. (2) how-to-run doc refreshed. (3) root `AGENTS.md` corrected; `plan/AGENTS.md` deleted. (4) Python 3.12.15 installed via `uv`, `.venv` rebuilt, `confluent-kafka` 2.16.0 installed, live events verified. (5) pushed to GitHub.
- **P1 — Raw archive. ▶️ UNBLOCKED 2026-10-17 — tool chosen: RustFS.** Next: verify how RustFS installs and runs single-node on WSL, create a bucket, add the Kafka Connect object-store sink that writes raw events there, and confirm objects appear. (MinIO is out: its open-source edition is archived; `dl.min.io` returns "410 Gone".) The archive is a **side branch** — nothing downstream reads from it, so it never blocks the analytics path. The old stray `~/minio/` folder (only a 410 error page) was deleted on 2026-10-17.
- **P2 — First PyFlink job.** Install `apache-flink==2.2.1`; add connector jars `5.0.0-2.2`; write job v1 (read `spotify-events`, validate, write `spotify-events-processed`); enable checkpointing (small interval, to protect the 30–60 s target); malformed + restart tests.
- **P3 — ClickHouse + sink connector + recovery runs.** Install ClickHouse, create the processed-events table, add the official ClickHouse sink connector, tune its flush/buffering for the live target, verify rows, then duplicate and full-stack restart tests.
- **P4 — Windowing exercise.** Events per minute (v1), then top 5 songs per 5 minutes (v2) → `spotify-metrics` → ClickHouse.
- **P5 — dbt on ClickHouse.** Staging models, first marts, tests + docs, scheduled every 5 minutes.
- **P6 — Dashboard.** One app, two pages (Live: 30 s refresh; Analytics: dbt marts); measure both freshness numbers; update the README.
- **P7 — Robustness + consolidation.** Schema-change policy, minimal monitoring, retention docs, final checkpoint/README pass.

## Working rules for this project

The teaching and working rules live in [`AGENTS.md`](../AGENTS.md) at the repo root (VS Code auto-loads that one): simple English, define jargon before using it, why before how, one step at a time, ask before adding new technology, and label **📌 project decision** / **🎉 breakthrough**.
