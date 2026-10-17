#!/usr/bin/env bash
# One-time setup for the Spotify Streaming Intelligence Platform.
#
# It fetches what the repo does not contain and prepares Kafka and Python.
# Safe to re-run: every step checks whether it is already done.
# No sudo is needed and nothing is installed system-wide.

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

KAFKA_VERSION="4.3.1"
KAFKA_SCALA="2.13"
KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}}"
KAFKA_TARBALL_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz"
KAFKA_LOG_DIRS="$KAFKA_HOME/data/kraft-combined-logs"

TOOLS_DIR="$PROJECT_DIR/.tools"
JDK_DIR="$TOOLS_DIR/jdk-21"
JDK_URL="https://api.adoptium.net/v3/binary/latest/21/ga/linux/x64/jdk/hotspot/normal/eclipse"

EVENTSIM_DIR="$PROJECT_DIR/eventsim"
EVENTSIM_COMMIT="04645e4"
DATA_URL_BASE="https://raw.githubusercontent.com/interana/eventsim/${EVENTSIM_COMMIT}/data"

log() { printf '\n[setup] %s\n' "$*"; }
ok()  { printf '[setup]   ok   %s\n' "$*"; }
die() { printf '\n[setup] ERROR: %s\n' "$*" >&2; exit 1; }

# --------------------------------------------------------------------------
# 0. Platform
# --------------------------------------------------------------------------
[ "$(uname -s)" = "Linux" ] || die "This project runs on Linux (including WSL). Detected: $(uname -s)."

# --------------------------------------------------------------------------
# 1. Java 21 or newer
# --------------------------------------------------------------------------
java_major() {
    "$1" -version 2>&1 | awk -F'"' '/version/ {print $2}' | awk -F. '{print ($1 == "1") ? $2 : $1}'
}

find_java() {
    if [ -x "$JDK_DIR/bin/java" ]; then
        echo "$JDK_DIR/bin/java"
    elif command -v java >/dev/null 2>&1; then
        command -v java
    fi
}

JAVA="$(find_java || true)"
if [ -n "$JAVA" ] && [ "$(java_major "$JAVA")" -ge 21 ]; then
    ok "Java $(java_major "$JAVA") at $JAVA"
else
    log "Java 21+ not found — downloading Temurin 21 into .tools/ (about 65 MB)"
    mkdir -p "$TOOLS_DIR"
    tmp_jdk="$(mktemp /tmp/jdk.XXXXXX.tar.gz)"
    curl -fL --progress-bar "$JDK_URL" -o "$tmp_jdk"
    rm -rf "$JDK_DIR"
    mkdir -p "$JDK_DIR"
    tar -xzf "$tmp_jdk" -C "$JDK_DIR" --strip-components=1
    rm -f "$tmp_jdk"
    JAVA="$JDK_DIR/bin/java"
    [ -x "$JAVA" ] || die "Java download succeeded but $JAVA is missing."
    ok "Java $(java_major "$JAVA") installed at $JDK_DIR"
fi

# --------------------------------------------------------------------------
# 2. Kafka
# --------------------------------------------------------------------------
if [ ! -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]; then
    log "Kafka not found at $KAFKA_HOME — downloading Kafka $KAFKA_VERSION (about 120 MB)"
    mkdir -p "$(dirname "$KAFKA_HOME")"
    tmp_kafka="$(mktemp /tmp/kafka.XXXXXX.tgz)"
    curl -fL --progress-bar "$KAFKA_TARBALL_URL" -o "$tmp_kafka"
    tar -xzf "$tmp_kafka" -C "$(dirname "$KAFKA_HOME")"
    rm -f "$tmp_kafka"
    [ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ] || die "Kafka extraction failed."
    ok "Kafka installed at $KAFKA_HOME"
else
    ok "Kafka found at $KAFKA_HOME"
fi

# Keep Kafka's data inside its own folder. A fresh download points at /tmp,
# which WSL wipes on restart and which deletes the topic with it.
if ! grep -qx "log.dirs=$KAFKA_LOG_DIRS" "$KAFKA_HOME/config/server.properties"; then
    log "Pointing Kafka's log.dirs at $KAFKA_LOG_DIRS"
    awk -v new="$KAFKA_LOG_DIRS" 'BEGIN{done=0}
        /^log\.dirs=/ {print "log.dirs=" new; done=1; next}
        {print}
        END{if(!done) print "log.dirs=" new}' \
        "$KAFKA_HOME/config/server.properties" > "$KAFKA_HOME/config/server.properties.tmp"
    mv "$KAFKA_HOME/config/server.properties.tmp" "$KAFKA_HOME/config/server.properties"
fi

# Kafka needs a one-time storage format before its first start.
if [ ! -f "$KAFKA_LOG_DIRS/meta.properties" ]; then
    log "Formatting Kafka storage (first run only)"
    KAFKA_UUID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
    "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$KAFKA_UUID" \
        -c "$KAFKA_HOME/config/server.properties" --standalone
    ok "Kafka storage formatted"
else
    ok "Kafka storage already formatted"
fi

# --------------------------------------------------------------------------
# 3. EventSim input data (too large for git, required at runtime)
# --------------------------------------------------------------------------
mkdir -p "$EVENTSIM_DIR/data"
for name in songs_analysis.txt.gz listen_counts.txt.gz; do
    target="$EVENTSIM_DIR/data/$name"
    if [ -s "$target" ]; then
        ok "eventsim/data/$name present"
        continue
    fi
    log "Downloading eventsim/data/$name"
    curl -fL --progress-bar "$DATA_URL_BASE/$name" -o "$target.part"
    mv "$target.part" "$target"
    ok "eventsim/data/$name downloaded"
done

# --------------------------------------------------------------------------
# 4. Python environment
# --------------------------------------------------------------------------
PY_VERSION="$((.venv/bin/python -c 'import sys; print("%d.%d" % sys.version_info[:2])') 2>/dev/null || true)"
if [ "$PY_VERSION" != "3.12" ]; then
    log "Creating .venv on Python 3.12"
    rm -rf .venv
    if command -v uv >/dev/null 2>&1; then
        uv venv --python 3.12 .venv
    elif command -v python3.12 >/dev/null 2>&1; then
        python3.12 -m venv .venv
    else
        die "Python 3.12 is required. Install 'uv' (https://docs.astral.sh/uv/) and re-run."
    fi
fi
ok ".venv runs Python $PY_VERSION"

log "Installing requirements.txt"
if command -v uv >/dev/null 2>&1; then
    uv pip install --quiet --python .venv/bin/python -r requirements.txt
else
    .venv/bin/python -m pip install --quiet -r requirements.txt
fi
.venv/bin/python -c "import confluent_kafka" || die "confluent-kafka did not install correctly."
ok "confluent-kafka installed"

# --------------------------------------------------------------------------
# Done
# --------------------------------------------------------------------------
printf '\n[setup] Setup complete.\n'
printf '[setup] Next: ./start.sh\n'
