#!/bin/sh
set -eu

log_file="output/log/transfers.log"
latest_log_file="output/log/transfers.log.latest"
mini_log_file="output/log/transfers.log.mini"
flock_file="output/flock/landingzones.lock"
common_status_log_file="output/log/Landing_Zone_localhost.transfers.tsv"
common_status_lock_file="output/flock/Landing_Zone_localhost.transfers.lock"
transfer_identifier="transfer_001"
run_log="$(mktemp "${TMPDIR:-/tmp}/landingzones.transfer_001.rsync.XXXXXX")"
cleanup_log="$(mktemp "${TMPDIR:-/tmp}/landingzones.transfer_001.cleanup.XXXXXX")"
promote_log="$(mktemp "${TMPDIR:-/tmp}/landingzones.transfer_001.promote.XXXXXX")"
current_run=""
current_run_source=""
current_run_destination=""
current_run_completed=0

cleanup() {
    rm -f "$run_log" "$cleanup_log" "$promote_log"
}
debug_enabled() {
    [ -t 1 ] || [ "${LZ_DEBUG_CLI:-0}" = "1" ]
}

log_status() {
    printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S%z')" "$1" >> "$mini_log_file"
}

sanitize_tsv_field() {
    printf '%s' "$1" | tr '\t\r\n' '   '
}

append_common_status() {
    event_status="$1"
    event_directory="${2:-}"
    event_source="${3:-}"
    event_destination="${4:-}"
    event_timestamp="$(date '+%Y-%m-%d %H:%M:%S%z')"
    (
        exec 8>>"$common_status_lock_file"
        /usr/bin/flock 8
        if [ ! -s "$common_status_log_file" ]; then
            printf 'datetime\tidentifier\tdirectory\tsource\tdestination\tstatus\n' >> "$common_status_log_file"
        fi
        printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$(sanitize_tsv_field "$event_timestamp")" \
            "$(sanitize_tsv_field "$transfer_identifier")" \
            "$(sanitize_tsv_field "$event_directory")" \
            "$(sanitize_tsv_field "$event_source")" \
            "$(sanitize_tsv_field "$event_destination")" \
            "$(sanitize_tsv_field "$event_status")" >> "$common_status_log_file"
    ) || debug "unable to append common status row"
}

debug() {
    if debug_enabled; then
        printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S%z')" "$1" >&2
    fi
}

dump_debug_log() {
    label="$1"
    path="$2"
    if debug_enabled && [ -s "$path" ]; then
        debug "$label follows"
        cat "$path" >&2
    fi
}

on_exit() {
    status=$?
    if [ "$status" -ne 0 ]; then
        if [ -n "$current_run" ] && [ "$current_run_completed" -eq 0 ]; then
            log_status "$current_run error"
            append_common_status "error" "$current_run" "$current_run_source" "$current_run_destination"
        else
            append_common_status "error" "" "input" "output"
        fi
        debug "script failed with exit code $status"
        dump_debug_log "run log" "$run_log"
        dump_debug_log "promote log" "$promote_log"
        dump_debug_log "cleanup log" "$cleanup_log"
    fi
    cleanup
    exit "$status"
}
trap on_exit EXIT HUP INT TERM

mkdir -p "$(dirname "$log_file")" "$(dirname "$latest_log_file")" "$(dirname "$mini_log_file")" "$(dirname "$flock_file")" "$(dirname "$common_status_log_file")" "$(dirname "$common_status_lock_file")"
debug "using lock file $flock_file"

exec 9>"$flock_file"
if ! /usr/bin/flock -n 9; then
    debug "lock busy, exiting"
    exit 0
fi

if ! [ -d "input" ]; then
    log_status "source directory missing: input"
    append_common_status "error" "" "input" "output"
    printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S%z')" "source directory missing: input" >> "$log_file"
    debug "source directory missing: input"
    exit 0
fi

: >"$run_log"
: >"$promote_log"
find "input" -mindepth 1 -maxdepth 1 -type d ! -name ".*" -print | while IFS= read -r source_dir; do
    [ -n "$source_dir" ] || continue
    dir_name=$(basename "$source_dir")
    current_run="$dir_name"
    current_run_source="$source_dir"
    current_run_destination="output/$dir_name"
    current_run_completed=0
    log_status "$dir_name initiated"
    append_common_status "initiated" "$dir_name" "$current_run_source" "$current_run_destination"
    debug "$dir_name initiated"
    mkdir -p "output/.staging/$dir_name" </dev/null >>"$promote_log" 2>&1
    rsync -av --remove-source-files "$source_dir/" "output/.staging/$dir_name/" </dev/null >>"$run_log" 2>&1
    if [ -d "output/$dir_name" ]; then find "output/.staging/$dir_name" -mindepth 1 -maxdepth 1 ! -name ".staging" -exec mv {} "output/$dir_name"/ \; && rmdir "output/.staging/$dir_name" 2>/dev/null || true; else mv "output/.staging/$dir_name" "output/$dir_name"; fi; rmdir "output/.staging" 2>/dev/null || true </dev/null >>"$promote_log" 2>&1
    log_status "$dir_name completed"
    append_common_status "completed" "$dir_name" "$current_run_source" "$current_run_destination"
    debug "$dir_name completed"
    current_run_completed=1
    current_run=""
    current_run_source=""
    current_run_destination=""
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
