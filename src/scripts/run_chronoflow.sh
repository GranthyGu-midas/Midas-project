#!/bin/sh
# Foreground owner of ChronoFlow. Other exchange processes remain independent.
set -eu

launcher_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
bundle="$launcher_root/data/replay/hyperliquid_btc_eth_20261123T135500Z_5m_v1"
config="$launcher_root/src/config/exchange.conf"
build_dir="$launcher_root/build/bin"
log_root="$launcher_root/build/logs/chronoflow"
speed=1
bootstrap_batch=1024
spin_us=50
max_lateness_us=0
recovery_seconds=300
color=auto
verify=false

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'HELP'
Usage: run_chronoflow.sh [options]
  --bundle PATH           Normalized input bundle (default: prepared BTC/ETH five-minute bundle)
  --config PATH           Exchange configuration (default: src/config/exchange.conf)
  --build-dir PATH        Directory containing market_chronoflow (default: build/bin)
  --log-dir PATH          Parent session directory (default: build/logs/chronoflow)
  --speed N               Historical replay speed (default: 1)
  --bootstrap-batch N     Orders between initialization barriers (default: 1024)
  --spin-us N             Busy-wait tail, 0..1000 us (default: 50)
  --max-lateness-us N     Stop above this scheduling delay; 0 reports only (default: 0)
  --recovery-seconds N    Maximum wait for one recovery episode (default: 300)
  --color MODE            auto, always, or never (default: auto; respects NO_COLOR)
  --verify-only           Validate the bundle and configuration without connecting
  --help                 Show this help

Keep this terminal open. Ctrl-C stops only ChronoFlow and flushes its reports.
Green: ready/resumed/complete. Cyan: progress and recovery positions.
Yellow: waiting/stopped. Red: paused input, unavailable engine, or fatal error.
Each session retains timestamped plain service.log, service.pid and run reports.
Restart this command with the same bundle/config and recovered engine data to resume.
HELP
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --help)
      usage
      exit 0
      ;;
    --verify-only)
      verify=true
      shift
      ;;
    --bundle|--config|--build-dir|--log-dir|--speed|--bootstrap-batch|--spin-us|--max-lateness-us|--recovery-seconds|--color)
      [ "$#" -ge 2 ] || fail "Missing value for $1"
      case "$1" in
        --bundle) bundle=$2 ;;
        --config) config=$2 ;;
        --build-dir) build_dir=$2 ;;
        --log-dir) log_root=$2 ;;
        --speed) speed=$2 ;;
        --bootstrap-batch) bootstrap_batch=$2 ;;
        --spin-us) spin_us=$2 ;;
        --max-lateness-us) max_lateness_us=$2 ;;
        --recovery-seconds) recovery_seconds=$2 ;;
        --color) color=$2 ;;
      esac
      shift 2
      ;;
    *)
      fail "Unknown option: $1 (see --help)"
      ;;
  esac
done

[ -r "$bundle/manifest.json" ] || fail "Cannot read bundle manifest: $bundle/manifest.json"
[ -r "$config" ] || fail "Cannot read configuration: $config"
[ -x "$build_dir/market_chronoflow" ] || fail "Missing market_chronoflow; run src/scripts/build.sh first"
case "$color" in
  always) use_color=1 ;;
  never) use_color=0 ;;
  auto)
    use_color=0
    if [ -t 1 ] && [ -z "${NO_COLOR+x}" ] && [ "${TERM:-dumb}" != dumb ]; then
      use_color=1
    fi
    ;;
  *) fail "Color must be auto, always, or never" ;;
esac

umask 077
mkdir -p "$log_root"
run_dir=$(mktemp -d "$log_root/run.XXXXXX")
fifo="$run_dir/output.fifo"
mkfifo "$fifo"
child_pid=
logger_pid=

cleanup() {
  trap '' INT TERM
  if [ -n "$child_pid" ]; then
    kill -TERM "$child_pid" 2>/dev/null || true
    wait "$child_pid" 2>/dev/null || true
  fi
  if [ -n "$logger_pid" ]; then
    wait "$logger_pid" 2>/dev/null || true
  fi
  rm -f -- "$fifo"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

printf 'COMPONENT=ChronoFlow\nBUNDLE=%s\nCONFIG=%s\nLOG_DIR=%s\n' "$bundle" "$config" "$run_dir"
printf '%s\n' 'Ctrl-C stops ChronoFlow; matching engines, Router and Gateway keep running.'

# The sole logger drains to EOF before exit, including the final recovery/report
# lines. ANSI escapes are applied only to terminal output, never to service.log.
awk -v logfile="$run_dir/service.log" -v colored="$use_color" '
  {
    level = "INFO"
    shade = "36"
    if ($0 ~ /CHRONOFLOW_ERROR|CHRONOFLOW_PAUSED|matching_engine_unavailable/) {
      level = "ERROR"
      shade = "31"
    } else if ($0 ~ /CHRONOFLOW_RECOVERY_WAIT|CHRONOFLOW_STOPPED/) {
      level = "WARN"
      shade = "33"
    } else if ($0 ~ /CHRONOFLOW_.*READY|CHRONOFLOW_REPLAY_STARTED|CHRONOFLOW_RESUMED|CHRONOFLOW_COMPLETE/) {
      shade = "32"
    }
    line = strftime("%Y-%m-%dT%H:%M:%S%z") " [" level "] " $0
    print line >> logfile
    fflush(logfile)
    if (colored)
      printf "\033[%sm%s\033[0m\n", shade, line
    else
      print line
    fflush()
  }
' < "$fifo" &
logger_pid=$!

set -- "$build_dir/market_chronoflow" --bundle "$bundle" --config "$config" \
  --output "$run_dir" --speed "$speed" --bootstrap-batch "$bootstrap_batch" \
  --spin-us "$spin_us" --max-lateness-us "$max_lateness_us" --recovery-seconds "$recovery_seconds"
if [ "$verify" = true ]; then
  set -- "$@" --verify-only
fi
"$@" > "$fifo" 2>&1 &
child_pid=$!
printf '%s\n' "$child_pid" > "$run_dir/service.pid"
printf 'PROCESS_RUNNING component=chronoflow pid=%s\n' "$child_pid"

set +e
wait "$child_pid"
result=$?
child_pid=
wait "$logger_pid"
logger_result=$?
logger_pid=
set -e
if [ "$logger_result" -ne 0 ] && [ "$result" -eq 0 ]; then
  result=$logger_result
fi
printf 'CHRONOFLOW_EXIT code=%s log=%s/service.log\n' "$result" "$run_dir"
exit "$result"
