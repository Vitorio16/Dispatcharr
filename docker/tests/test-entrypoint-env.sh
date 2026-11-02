#!/bin/bash
# Exercise the real entrypoint propagation block without booting services or
# writing /etc. A clean-environment child models su - discarding container env.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(mktemp -d)"
trap 'rm -rf "$TEST_DIR"' EXIT
export TEST_DIR

# Extract only the existing environment writer; redirect its system paths into
# the disposable test directory. No replacement propagation logic lives here.
tr -d '\r' < "$SCRIPT_DIR/../entrypoint.sh" | awk '
    /^variables=\(/ { copying=1 }
    copying { print }
    copying && /^chmod \+x \/etc\/profile.d\/dispatcharr.sh/ { exit }
' | sed -e 's|/etc/profile.d/dispatcharr.sh|"${TEST_DIR}/profile.sh"|g' \
        -e 's|/etc/environment|"${TEST_DIR}/environment"|g' > "$TEST_DIR/propagate.sh"
test -s "$TEST_DIR/propagate.sh"

names=(HOT_STANDBY_ENABLED HOT_STANDBY_WARMUP_TIMEOUT HOT_STANDBY_IDLE_TIMEOUT
       HOT_STANDBY_COOLDOWN HOT_STANDBY_SEARCH_INTERVAL)
values=(true 7 9 23 2)

for index in "${!names[@]}"; do
    export "${names[index]}=${values[index]}"
done
touch "$TEST_DIR/environment"
bash "$TEST_DIR/propagate.sh" > "$TEST_DIR/output"

# Each file must independently carry all five values after inherited env is
# erased. Use absolute bash because env -i also discards PATH.
BASH_BIN="$(command -v bash)"
for generated in profile.sh environment; do
    env -i "$BASH_BIN" --noprofile --norc -c '
        source "$1"
        test "$HOT_STANDBY_ENABLED" = true
        test "$HOT_STANDBY_WARMUP_TIMEOUT" = 7
        test "$HOT_STANDBY_IDLE_TIMEOUT" = 9
        test "$HOT_STANDBY_COOLDOWN" = 23
        test "$HOT_STANDBY_SEARCH_INTERVAL" = 2
    ' bash "$TEST_DIR/$generated"
done
echo 'PASS: all five settings survive a cleared child environment via either file'

# Restart with changed values: replace rather than append stale assignments.
export HOT_STANDBY_ENABLED=false HOT_STANDBY_COOLDOWN=31
bash "$TEST_DIR/propagate.sh" > "$TEST_DIR/output"
test "$(grep -c '^HOT_STANDBY_COOLDOWN=' "$TEST_DIR/environment")" = 1
grep -qx "HOT_STANDBY_COOLDOWN='31'" "$TEST_DIR/environment"
grep -qx "export HOT_STANDBY_ENABLED='false'" "$TEST_DIR/profile.sh"
echo 'PASS: explicit false and changed settings propagate on restart'

# Fresh startup without any optional standby settings stays quiet and lets
# config.py apply its defaults.
for name in "${names[@]}"; do unset "$name"; done
: > "$TEST_DIR/environment"
bash "$TEST_DIR/propagate.sh" > "$TEST_DIR/output"
! grep -q HOT_STANDBY "$TEST_DIR/profile.sh" "$TEST_DIR/environment" "$TEST_DIR/output"
echo 'PASS: unset standby settings remain absent without warnings'
