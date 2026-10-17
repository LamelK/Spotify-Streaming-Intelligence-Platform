# EventSim — vendored copy

This folder is a copy of [Interana's EventSim](https://github.com/interana/eventsim)
(MIT licensed, see [LICENSE.txt](./LICENSE.txt)), pinned to upstream commit
`04645e4`. We keep it inside this repository so the whole pipeline can be built
from one clone.

EventSim is the fake Spotify event generator: it simulates users listening to
songs and writes one JSON event per action.

## Why it is vendored

The upstream code uses Kafka APIs from 2015 and does not compile or connect
today. The changes below were made so it runs on Java 21 with Kafka 4.3.1.
Upstream EventSim is **not** a dependency of this project any more — the copy in
this folder is the source of truth.

## Changes from upstream

| File | Change | Why |
|---|---|---|
| `build.sbt` | `jollyday` 0.5.1 → 0.5.10; `kafka_2.10` 0.8.2.1 → `kafka-clients` 4.3.1; added `slf4j-nop` 1.7.36; added an assembly merge rule for `module-info.class` | The old Kafka client library is not compatible with Kafka 4.x, and Java 9+ jars ship conflicting `module-info.class` files that break the flat jar build |
| `src/main/scala/com/interana/eventsim/KafkaOutputStream.scala` | Old `kafka.producer` API replaced with `KafkaProducer` + `ProducerRecord`; added `close()` | The old producer API was removed from Kafka |
| `src/main/scala/com/interana/eventsim/Main.scala` | `metadata.broker.list` → `bootstrap.servers`; added key/value serializers | Same reason; `bootstrap.servers` is the modern setting |
| `src/main/scala/com/interana/eventsim/TimeUtilities.scala` | `HolidayManager.getInstance(HolidayCalendar.UNITED_STATES)` instead of the no-arg call | The no-arg call returned `null` under a `C.UTF-8` locale, which crashed the job |
| `examples/example-config.json` | `beta` 604800 → 1800 | `beta` is the average seconds between a user's sessions. 604800 (one week) produced almost no events; 1800 (30 minutes) gives a steady stream |
| `bin/eventsim` | Jar path `target/scala-2.10/...` → `dist/...`; resolves its own directory; `-Xmx8G` → `-Xmx1G` | The old path was wrong and depended on the current directory. 8 GB of heap fails on smaller machines; our runs use 1800 users, which needs far less |
| `Dockerfile` | **Not vendored** | Upstream's Dockerfile uses a Java 8 runtime and builds Scala 2.13, both incompatible with our Java 21 / Scala 2.12 build. Docker is deferred (see `plan/CHECKPOINT.md`) |

## What is committed and what is downloaded

Committed:

- the Scala source, configs, examples and launcher
- `dist/eventsim-assembly-1.0.jar` — the jar built from this exact source

Not committed (`.gitignore`d):

- `data/*.gz` — ~71 MB of reference data. EventSim loads these at startup
  (`Loading song file...`), so `setup.sh` downloads them from the pinned
  upstream commit. Without them the job crashes with
  `ExceptionInInitializerError`.
- `target/` — sbt build output

## Rebuilding the jar (optional)

Only needed if you change the Scala source. Requires Java 21 and sbt:

```bash
cd eventsim
sbt assembly
cp target/scala-2.12/eventsim-assembly-1.0.jar dist/
```

The jar committed in `dist/` was built from the source in this folder with
`sbt assembly`.
