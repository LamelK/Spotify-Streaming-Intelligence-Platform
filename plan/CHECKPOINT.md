# Checkpoint — updated 2026-09-26

Resume by saying: **"continue from the checkpoint"**.

## Where we are

**Stage 0 COMPLETE.** EventSim (the fake Spotify data simulator) builds and runs on Java 21 and streams real-time into Kafka 4.3.1 (single topic `spotify-events`).

Our own Python code exists: `consumer.py` (reads events from Kafka) and `producer.py` (a small dev/test stream — the official source of events is EventSim). Both use `confluent-kafka`.

The third-party simulator lives outside the project at `~/eventsim`; Kafka at `~/kafka/kafka_2.13-4.3.1`.

## Environment (verified on `melak@Lamel-Laptop`, WSL Ubuntu)

| Tool | Version | Note |
|---|---|---|
| Java | 8 (1.8.0_504) and 21 (21.0.12.1) | Java 21 is the default and what we build/run with |
| sbt | 1.11.7 | builds EventSim on Scala 2.12 |
| Kafka | 4.3.1 | installed at `~/kafka/kafka_2.13-4.3.1`, KRaft mode |
| Python | 3.14.4 | for our own Kafka producer/consumer later |
| Docker | not installed | deliberate decision: not using it |

- Project folder: `~/Spotify-Streaming-Intelligence-Platform`
- EventSim clone: `~/eventsim` (outside the project, third-party)
- Kafka home: `~/kafka/kafka_2.13-4.3.1`

## What was changed (EventSim modernization)

Five files changed in `~/eventsim`, plus two new dependencies. Everything else untouched.

1. `build.sbt`
   - `jollyday` `0.5.1` → `0.5.10` (old version cannot load holiday XML under Java 21).
   - `kafka_2.10` `0.8.2.1` → `kafka-clients` `4.3.1`.
   - added `slf4j-nop` `1.7.36` (silences the Kafka client's "no logger binding" warning).
   - added an assembly merge rule to discard `module-info.class` (jollyday 0.5.10 pulls in JAXB jars that each ship a conflicting one).
2. `TimeUtilities.scala`
   - `HolidayManager.getInstance()` → `HolidayManager.getInstance(HolidayCalendar.UNITED_STATES)`.
   - Reason: the no-arg call picks a country from the system locale; WSL's locale is `C.UTF-8` (no country), so it returned null. The code comment already said "US only".
3. `KafkaOutputStream.scala`
   - old `kafka.producer` API → modern `KafkaProducer` + `ProducerRecord`.
   - `flush()` sends one record per event; added `close()` to flush and close the producer.
4. `Main.scala`
   - `metadata.broker.list` → `bootstrap.servers`, added `key.serializer` / `value.serializer`.
   - CLI options `--kafkaBrokerList` and `--kafkaTopic` unchanged.
5. `bin/eventsim`
   - fixed jar path (`scala-2.10` → `scala-2.12`), removed the removed flag `-XX:+AggressiveOpts`, use `java` from PATH.

## Build / run / verify commands (all verified working)

Build:
```bash
cd ~/eventsim
sbt assembly
```

Run to a file / stdout (no Kafka):
```bash
cd ~/eventsim
./bin/eventsim -c examples/example-config.json --from 1 --to 0 --nusers 2
```

Start Kafka (single node, KRaft, first time only format with `bin/kafka-storage.sh format -t <uuid> -c config/server.properties --standalone`):
```bash
cd ~/kafka/kafka_2.13-4.3.1
bin/kafka-server-start.sh config/server.properties
```

Create a topic:
```bash
cd ~/kafka/kafka_2.13-4.3.1
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic spotify-events --partitions 1 --replication-factor 1
```

Stream into Kafka:
```bash
cd ~/eventsim
./bin/eventsim -c examples/example-config.json --from 14 --to 0 --nusers 50 --kafkaBrokerList localhost:9092 --kafkaTopic spotify-events
```

Consume to verify:
```bash
cd ~/kafka/kafka_2.13-4.3.1
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic spotify-events --from-beginning --max-messages 5
```

## Decisions and breakthroughs

- **📌 Project decision — Java 21 is the baseline.** Java 8 stays installed but is not needed for the current build; we build and run with Java 21.
- **📌 Project decision — Kafka 4.3.1, KRaft mode (no ZooKeeper).** Kafka 4.x removed ZooKeeper; single combined broker+controller node.
- **📌 Project decision — keep EventSim's CLI and event shape unchanged.** Only the Kafka delivery layer and the minimal Java 21 compatibility fixes changed.
- **🎉 Breakthrough — EventSim builds and runs on Java 21.**
- **🎉 Breakthrough — EventSim streams real Spotify-shaped events into Kafka 4.3.1.** Verified with the console consumer; a 50-user / 14-day run streamed 1,566 events into the topic.

## Target architecture and decisions (agreed 2026-09-26, updated 2026-10-03)

> ⚠️ **Nothing here is final.** Every architecture decision below can be changed. We record the choice *and the reason* so we can revisit it deliberately — not to lock ourselves in. When a decision is "deferred" or "open", that is the plan saying: decide later, on purpose.

- **📌 Project decision — target architecture:** `EventSim → Kafka → Python consumer (confluent-kafka) → PostgreSQL → dbt → (later) Airflow`. Local-first, free, and recruiter-reproducible via Docker Compose.
- **📌 Project decision — warehouse = PostgreSQL** (local, in Docker). Cloud alternatives rejected for now: Snowflake (30-day trial only), AWS (credit card + expiring free tier). BigQuery kept as an *optional later* dbt target for a cloud badge.
- **📌 Project decision — transformation/modelling = dbt** (dbt-core + dbt-postgres). PySpark deferred to a separate project (our data is far too small to justify Spark). Fabric (needs an org email) and Databricks (extra platform) rejected.
- **📌 Project decision — orchestration = Airflow** (local, Docker) instead of cloud orchestrators (AWS Step Functions / Azure Data Factory), for portability and a transferable skill.
- **📌 Project decision — EventSim data is structurally messy, not clean.** Evidence: ragged schema (`artist`/`song`/`length` only on `NextSong` events), empty-string `userId` + missing user fields for Guest/Logged Out, and 404/307 error statuses. This gives the cleaning + dbt-testing stage real work with no injection needed. (Optional later: inject duplicates / malformed JSON / late events.)
- **📌 Project decision — split into 4 streams in our Python consumer** (`auth_events`, `listen_events`, `page_view_events`, `status_change_events`). EventSim stays untouched (one `spotify-events` raw topic); `consumer.py` routes each event by its `page` field. Rejected the earlier plan (change EventSim's output): it's Scala work, and Python is the part we build and can explain anyway.
- **📌 Project decision — the streaming processing layer is our own Python consumer, not a dedicated engine.** We considered Flink / PyFlink / Bytewax / Quix Streams and **deferred them all**. Reason: our data is tiny (~15 events/min at 10k users), a plain Python consumer is fully explainable, and we are still Kafka beginners — adding a JVM cluster now would be the "install everything → get confused" trap. This is the one decision most likely to change later (see Open questions).
- **📌 Project decision — ELT, not ETL.** Load events raw into PostgreSQL first (whole event in a `jsonb` column), then clean and model inside dbt. Reason: we keep the raw copy, and cleaning becomes re-runnable, testable SQL. The Python consumer does only light work: parse, drop broken events, route by `page`.
- **📌 Project decision — cleaning is split; the real cleaning lives in dbt's staging layer.** `consumer.py` validates and routes; dbt staging (`stg_*`) casts types, handles missing fields, and dedupes. This mirrors real pipelines: streaming for ingestion, batch for modeling.
- **📌 Project decision — raw tables are created by our own `CREATE TABLE` SQL; dbt tables are created by dbt itself.** We never hand-write DDL for dbt models — dbt generates the tables from our `SELECT` models when we run `dbt run`.

## Open questions (not yet answered)

- **Optional Stage B — a dedicated streaming engine. Deferred, and explicitly open to change.** Options: Flink (PyFlink), Bytewax, or Quix Streams — or none. We would only add one if we want to learn engine-level concepts (watermarks, checkpoints, event-time windows). Bytewax and Quix Streams are pure Python (no JVM); PyFlink still needs a JVM cluster. **Power BI** is also deferred (it cannot live in the Docker Compose repo; later it can be a screenshot in the README). Revisit after Stage A works end-to-end.
- **How we will decide Stage B — the "honest middle path".** (1) Build Stage A with Python first — a fast, working pipeline. (2) Then try one small exercise that only an engine does well: *"top 5 songs per 5-minute window, updated live."* (3) At that point the value of windows/watermarks is *felt*, not argued — we decide with real experience, not a debate. If it clicks, add Bytewax / Quix Streams (or Flink) as Stage B; if not, Python was the right call and we can explain why.
- **Where our modified EventSim will live / how it is owned** — vendored copy inside the project, or a GitHub fork. Undecided. (The changes currently live in the local `~/eventsim` clone.)
- **`plan/AGENTS.md` is not auto-loaded** by VS Code (it only auto-loads `AGENTS.md` from the *workspace root*). Unanswered.
- **`producer.py` — keep as a dev/test smoke stream, or delete.** Undecided.
- **Docker timing** — recommended at the PostgreSQL stage (step 2), not before.

## Next steps, in order

1. **Watch one event flow live** — Kafka → EventSim → `consumer.py` — to cement the producer/broker/consumer mental model.
2. **Streaming ETL** — `consumer.py` → validate + route, load **raw** into PostgreSQL (`jsonb` columns); introduce Docker Compose here.
3. **dbt on PostgreSQL** — staging → star schema (`dim_users`, `dim_songs`, `dim_artists`, `fact_listens`), with tests and docs.
4. **Airflow** — schedule the consumer and the dbt runs.
5. **README + Docker Compose polish** — one-command reproducibility for a recruiter.
6. *(optional)* **BigQuery** as a second dbt target — the cloud badge.

## Working rules for this project

The teaching and working rules live in [`AGENTS.md`](./AGENTS.md) in this same folder: simple English, define jargon before using it, why before how, one step at a time, no new technology without asking, and label **📌 project decision** / **🎉 breakthrough**.
