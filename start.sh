#!/usr/bin/env bash
# Start the whole pipeline: Kafka, EventSim and a peek consumer.
# Ctrl+C stops everything this script started.

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

KAFKA_VERSION="4.3.1"
KAFKA_SCALA="2.13"
KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}}"
TOPIC="spotify-events"
NUSERS="${NUSERS:-1800}"
RUN_DIR="$PROJECT_DIR/.run"

log() { printf '\n[start] %s\n' "$*"; }
die() { printf '\n[start] ERROR: %s\n' "$*" >&2; exit 1; }

[ -x "$PROJECT_DIR/.venv/bin/python" ] || die "No .venv found. Run ./setup.sh first."
[ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ] || die "Kafka not found at $KAFKA_HOME. Run ./setup.sh first."
[ -s "$PROJECT_DIR/eventsim/dist/eventsim-assembly-1.0.jar" ] || die "EventSim jar missing. Run ./setup.sh first."
[ -s "$PROJECT_DIR/eventsim/data/songs_analysis.txt.gz" ] || die "EventSim data missing. Run ./setup.sh first."

# EventSim's launcher calls `java` from PATH; prefer the JDK setup.sh may have fetched.
if [ -x "$PROJECT_DIR/.tools/jdk-21/bin/java" ]; then
    export JAVA_HOME="$PROJECT_DIR/.tools/jdk-21"
    export PATH="$JAVA_HOME/bin:$PATH"
fi

mkdir -p "$RUN_DIR"

STARTED_KAFKA=0
KAFKA_PID=""
EVENTSIM_PID=""
CONSUMER_PID=""

cleanup() {
    code="${1:-0}"
    trap - EXIT INT TERM
    if [ -n "$CONSUMER_PID" ] && kill -0 "$CONSUMER_PID" 2>/dev/null; then
        # SIGTERM, not SIGINT: bash sets SIGINT to "ignored" for background children.
        kill "$CONSUMER_PID" 2>/dev/null || true
        wait "$CONSUMER_PID" 2>/dev/null || true
    fi
    if [ -n "$EVENTSIM_PID" ] && kill -0 "$EVENTSIM_PID" 2>/dev/null; then
        printf '\n[start] Stopping EventSim...\n'
        kill "$EVENTSIM_PID" 2>/dev/null || true
        wait "$EVENTSIM_PID" 2>/dev/null || true
    fi
    if [ "$STARTED_KAFKA" -eq 1 ] && [ -n "$KAFKA_PID" ] && kill -0 "$KAFKA_PID" 2>/dev/null; then
        printf '[start] Stopping Kafka...\n'
        kill "$KAFKA_PID" 2>/dev/null || true
        wait "$KAFKA_PID" 2>/dev/null || true
    fi
    exit "$code"
}
trap 'cleanup 0' EXIT
trap 'cleanup 130' INT TERM

broker_up() {
    "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 >/dev/null 2>&1
}

if broker_up; then
    log "Kafka is already running — leaving it alone"
else
    log "Starting Kafka (log: .run/kafka.log)"
    "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/server.properties" \
        > "$RUN_DIR/kafka.log" 2>&1 &
    KAFKA_PID=$!
    STARTED_KAFKA=1
    for _ in $(seq 1 60); do
        broker_up && break
        kill -0 "$KAFKA_PID" 2>/dev/null || die "Kafka exited. See .run/kafka.log"
        sleep 1
    done
    broker_up || die "Kafka did not become ready within 60s. See .run/kafka.log"
    log "Kafka is ready"
fi

log "Ensuring topic '$TOPIC' exists"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1

log "Starting EventSim with $NUSERS users (log: .run/eventsim.log)"
"$PROJECT_DIR/eventsim/bin/eventsim" -c examples/example-config.json --continuous --nusers "$NUSERS" \
    --start-time "$(date '+%Y-%m-%dT%H:%M:%S')" \
    --end-time 2099-01-01T00:00:00 \
    --kafkaBrokerList localhost:9092 --kafkaTopic "$TOPIC" \
    > "$RUN_DIR/eventsim.log" 2>&1 &
EVENTSIM_PID=$!

sleep 3
if ! kill -0 "$EVENTSIM_PID" 2>/dev/null; then
    die "EventSim exited immediately. Last lines of .run/eventsim.log:

$(tail -n 20 "$RUN_DIR/eventsim.log")"
fi

log "EventSim is loading ~385k songs; the first events appear in about a minute"
log "Peeking at the stream — press Ctrl+C to stop everything"
"$PROJECT_DIR/.venv/bin/python" -u consumer.py &
CONSUMER_PID=$!
wait "$CONSUMER_PID" || true
