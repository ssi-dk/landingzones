#!/bin/sh
set -eu

log_file="output/log/transfers.log"
latest_log_file="output/log/transfers.log.latest"
mini_log_file="output/log/transfers.log.mini"
flock_file="output/flock/landingzones.lock"
run_log="$(mktemp "${TMPDIR:-/tmp}/landingzones.transfer_001.rsync.XXXXXX")"
cleanup_log="$(mktemp "${TMPDIR:-/tmp}/landingzones.transfer_001.cleanup.XXXXXX")"
promote_log="$(mktemp "${TMPDIR:-/tmp}/landingzones.transfer_001.promote.XXXXXX")"

cleanup() {
    rm -f "$run_log" "$cleanup_log" "$promote_log"
}
trap cleanup EXIT HUP INT TERM

log_status() {
    printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S%z')" "$1" >> "$mini_log_file"
}

exec 9>"$flock_file"
if ! /usr/bin/flock -n 9; then
    exit 0
fi

: >"$run_log"
: >"$promote_log"
find "input" -mindepth 1 -maxdepth 1 -type d -print | while IFS= read -r source_dir; do
    [ -n "$source_dir" ] || continue
    dir_name=$(basename "$source_dir")
    log_status "$dir_name initiated"
    mkdir -p "output/.staging/$dir_name" >>"$promote_log" 2>&1
    rsync -av --remove-source-files "$source_dir/" "output/.staging/$dir_name/" >>"$run_log" 2>&1
    if [ -d "output/$dir_name" ]; then find "output/.staging/$dir_name" -mindepth 1 -maxdepth 1 -exec mv {} "output/$dir_name"/ \; && rmdir "output/.staging/$dir_name" 2>/dev/null || true; else mv "output/.staging/$dir_name" "output/$dir_name"; fi; rmdir "output/.staging" 2>/dev/null || true >>"$promote_log" 2>&1
    log_status "$dir_name completed"
done
if [ -s "$run_log" ]; then
    cat "$run_log" >> "$log_file"
fi
if [ -s "$promote_log" ]; then
    cat "$promote_log" >> "$log_file"
fi

find "input" -mindepth 1 -type d -empty -delete >"$cleanup_log" 2>&1
if [ -s "$cleanup_log" ]; then
    cat "$cleanup_log" >> "$log_file"
fi

if sed '/^sending incremental file list$/d; /^sent .* bytes .*$/d; /^total size is .*$/d; /^$/d' "$run_log" | grep -q .; then
    cat "$run_log" > "$latest_log_file"
    if [ -s "$promote_log" ]; then
        cat "$promote_log" >> "$latest_log_file"
    fi
    if [ -s "$cleanup_log" ]; then
        cat "$cleanup_log" >> "$latest_log_file"
    fi
fi
