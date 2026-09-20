#!/bin/sh
# build-installer.sh — assemble the self-extracting simple-chat.sh installer.
#
# Builds static linux/amd64 + linux/arm64 binaries from app/, base64-embeds
# both into one POSIX-sh script that extracts the right one, prepares .env /
# accounts.json, and execs the gateway.
#
# Final script layout (order matters — payloads are assigned before the
# runtime logic references them):
#   1. shebang + documentation header          (HEAD heredoc)
#   2. BIN_AMD64_B64 / BIN_ARM64_B64 assignments (base64 payloads)
#   3. runtime logic                           (BODY heredoc)
#
# Usage: scripts/build-installer.sh [output-path]
#   default output: dist/simple-chat.sh
# Run from anywhere; paths resolve from the script location.
# Requires: go, base64 (GNU or busybox).
set -eu

OUT="${1:-dist/simple-chat.sh}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
APP="$ROOT/app"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "==> building linux/amd64"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -C "$APP" -trimpath -ldflags="-s -w" -o "$WORK/simple-chat-amd64-linux" .
echo "==> building linux/arm64"
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -C "$APP" -trimpath -ldflags="-s -w" -o "$WORK/simple-chat-arm64-linux" .

echo "==> assembling $OUT"
mkdir -p "$(dirname "$OUT")"

# --- part 1: shebang + docs ---
cat > "$OUT" <<'HEADER'
#!/bin/sh
# simple-chat self-extracting installer
# built by scripts/build-installer.sh from public source — contains no
# credentials, no config, no accounts: just the static binary (amd64 +
# arm64) and bootstrap logic.
#
# What it does:
#   1. extracts ./simple-chat (matching your CPU arch, mode 0700)
#   2. creates a template ./.env if missing, then exits — fill it in and
#      re-run (all vars optional; you need at least one account, pushed
#      via POST /admin/accounts or seeded into accounts.json)
#   3. creates ./accounts.json (empty, 0600) if missing
#   4. execs ./simple-chat in the foreground (Ctrl-C stops it; use the
#      docker-compose.yml alternative at the bottom of this file or a
#      systemd unit for service management)
#
# Re-running the script is the upgrade path: the binary is overwritten
# with the embedded one, existing .env and accounts.json are kept.
HEADER

# --- part 2: base64 payloads ---
printf 'BIN_AMD64_B64="' >> "$OUT"
base64 -w0 "$WORK/simple-chat-amd64-linux" >> "$OUT"
printf '"\nBIN_ARM64_B64="' >> "$OUT"
base64 -w0 "$WORK/simple-chat-arm64-linux" >> "$OUT"
printf '"\n' >> "$OUT"

# --- part 3: runtime logic ---
cat >> "$OUT" <<'BODY'
set -eu

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

case "$(uname -m)" in
    x86_64)        PAYLOAD="$BIN_AMD64_B64" ;;
    aarch64|arm64) PAYLOAD="$BIN_ARM64_B64" ;;
    *) echo "unsupported architecture: $(uname -m) (installer ships x86_64 and aarch64)" >&2; exit 1 ;;
esac

echo "==> extracting simple-chat ($(uname -m))"
printf '%s' "$PAYLOAD" | base64 -d > ./simple-chat
chmod 0700 ./simple-chat

if [ ! -f ./.env ]; then
    cat > ./.env <<'ENVTEMPLATE'
# simple-chat configuration. Every var is optional.
# This file must stay shell-sourceable: KEY=value lines, # comments, no spaces around =.

# API key required on /v1/* and /admin/* routes (unset = open access).
# Generate one: openssl rand -hex 24
#DS_API_KEY=

# Listen address.
DS_ADDR=:8080

# Account store. Default (below) is the local JSON file; for a managed
# Upstash Redis instead, set DS_REDIS_HOST + DS_REDIS_TOKEN and comment
# DS_ACCOUNTS out. A full rediss:// connection string in DS_REDIS_HOST is
# used as-is (token then ignored).
#DS_ACCOUNTS=./accounts.json
#DS_REDIS_HOST=https://mydb.upstash.io
#DS_REDIS_TOKEN=

# See README.md for the full env surface (in-flight caps, prompt caps,
# session cleanup/purge tuning).
ENVTEMPLATE
    chmod 0600 ./.env
    echo "==> created ./.env template — fill it in, then re-run this script"
    exit 0
fi

# Export everything in .env for the binary.
set -a
. ./.env
set +a

if [ ! -f ./accounts.json ]; then
    # Placeholder account: the gateway deliberately refuses to start with
    # an empty pool (fail-fast contract), and the admin API lives behind
    # the running server — so first boot needs one row to come up at all.
    # This one is obviously fake; add real accounts via POST /admin/accounts
    # (or edit accounts.json) and remove the placeholder.
    cat > ./accounts.json <<'SEED'
{"accounts": [{"mobile": "13800000000", "email": "", "password": "replace-me"}]}
SEED
    chmod 0600 ./accounts.json
    echo "==> created ./accounts.json with a placeholder account (13800000000/replace-me) — add real ones via POST /admin/accounts, then delete the placeholder"
fi

echo "==> starting simple-chat on ${DS_ADDR:-:8080}"
exec ./simple-chat

# --- docker-compose.yml alternative (extract manually if wanted) ---
# cut everything between the two COMPOSE markers into docker-compose.yml,
# then: docker compose up -d
# --- COMPOSE BEGIN ---
#services:
#  simple-chat:
#    build: .
#    ports:
#      - "9879:8080"
#    volumes:
#      - ./accounts.json:/app/accounts.json
#    env_file:
#      - ./.env
#    restart: unless-stopped
# --- COMPOSE END ---
BODY

chmod 0755 "$OUT"
echo "done: $OUT ($(wc -c < "$OUT") bytes)"
