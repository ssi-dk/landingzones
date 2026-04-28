#!/bin/sh
set -eu

usage() {
    cat <<'EOF'
Usage:
  lz_run_validation.sh preflight --fixture-dir DIR --entry-dir DIR [--next-hop DEST] [--next-hop-port PORT]
  lz_run_validation.sh run --fixture-dir DIR --entry-dir DIR --flow-group NAME --producer NAME [--token TOKEN]

Options:
  --fixture-dir DIR     Local fixture directory copied into the validation run folder
  --entry-dir DIR       Local visible entry directory watched by the transfer hop
  --next-hop DEST       Immediate next hop to validate, either local path or remote host:path
  --next-hop-port PORT  Optional SSH port for a remote --next-hop target
  --flow-group NAME     Flow label used in the generated LZTEST folder name
  --producer NAME       Producer label used in the generated LZTEST folder name
  --token TOKEN         Optional suffix token; defaults to the first uuidgen field
EOF
}

print_status() {
    level="$1"
    message="$2"
    printf '%s %s\n' "$level" "$message"
}

sanitize_component() {
    printf '%s' "$1" | tr -c 'A-Za-z0-9_-' '_' | tr '[:lower:]' '[:upper:]'
}

split_remote_path() {
    value="$1"
    case "$value" in
        *:*)
            remote_part="${value%%:*}"
            path_part="${value#*:}"
            case "$remote_part" in
                */*)
                    return 1
                    ;;
                *)
                    REMOTE_TARGET="$remote_part"
                    REMOTE_PATH="$path_part"
                    return 0
                    ;;
            esac
            ;;
        *)
            return 1
            ;;
    esac
}

check_local_dir() {
    path="$1"
    label="$2"
    if [ ! -d "$path" ]; then
        print_status "ERROR" "$label missing: $path"
        return 1
    fi
    if [ ! -w "$path" ]; then
        print_status "ERROR" "$label not writable: $path"
        return 1
    fi
    print_status "OK" "$label ready: $path"
}

remote_ssh() {
    remote_target="$1"
    remote_port="$2"
    shift 2
    remote_command=""
    for remote_arg in "$@"; do
        quoted_remote_arg=$(printf '%s' "$remote_arg" | sed "s/'/'\\\\''/g")
        if [ -n "$remote_command" ]; then
            remote_command="$remote_command "
        fi
        remote_command="${remote_command}'${quoted_remote_arg}'"
    done
    if [ -n "$remote_port" ]; then
        ssh -p "$remote_port" -o BatchMode=yes -o ConnectTimeout=10 "$remote_target" "$remote_command"
    else
        ssh -o BatchMode=yes -o ConnectTimeout=10 "$remote_target" "$remote_command"
    fi
}

check_remote_dir() {
    remote_target="$1"
    remote_port="$2"
    remote_path="$3"
    if ! remote_ssh "$remote_target" "$remote_port" printf '%s\n' 'SSH_OK' >/dev/null 2>&1; then
        print_status "ERROR" "Immediate next-hop SSH failed: $remote_target"
        return 1
    fi
    if ! remote_ssh "$remote_target" "$remote_port" sh -c '[ -d "$1" ] && [ -w "$1" ]' sh "$remote_path" >/dev/null 2>&1; then
        print_status "ERROR" "Immediate next-hop directory unavailable: $remote_target:$remote_path"
        return 1
    fi
    print_status "OK" "Immediate next-hop ready: $remote_target:$remote_path"
}

require_tool() {
    tool_name="$1"
    if ! command -v "$tool_name" >/dev/null 2>&1; then
        print_status "ERROR" "Missing required tool: $tool_name"
        return 1
    fi
    print_status "OK" "Tool available: $tool_name"
}

build_validation_name() {
    flow_group_safe="$(sanitize_component "$FLOW_GROUP")"
    producer_safe="$(sanitize_component "$PRODUCER")"
    timestamp_utc="$(date -u '+%Y%m%dT%H%M%SZ')"
    token_value="$TOKEN"
    if [ -z "$token_value" ]; then
        token_value="$(uuidgen | cut -d- -f1 | tr '[:lower:]' '[:upper:]')"
    fi
    token_safe="$(sanitize_component "$token_value")"
    printf 'LZTEST_%s_%s_%s_%s\n' "$flow_group_safe" "$producer_safe" "$timestamp_utc" "$token_safe"
}

run_preflight() {
    require_tool uuidgen || return 1
    require_tool cp || return 1
    [ -n "$FIXTURE_DIR" ] || { print_status "ERROR" "--fixture-dir is required"; return 1; }
    [ -n "$ENTRY_DIR" ] || { print_status "ERROR" "--entry-dir is required"; return 1; }
    check_local_dir "$FIXTURE_DIR" "Fixture directory" || return 1
    check_local_dir "$ENTRY_DIR" "Entry directory" || return 1

    if [ -n "$NEXT_HOP" ]; then
        if split_remote_path "$NEXT_HOP"; then
            require_tool ssh || return 1
            check_remote_dir "$REMOTE_TARGET" "$NEXT_HOP_PORT" "$REMOTE_PATH" || return 1
        else
            check_local_dir "$NEXT_HOP" "Immediate next-hop directory" || return 1
        fi
    fi
    print_status "OK" "Preflight passed"
}

run_validation() {
    require_tool uuidgen || return 1
    require_tool cp || return 1
    [ -n "$FIXTURE_DIR" ] || { print_status "ERROR" "--fixture-dir is required"; return 1; }
    [ -n "$ENTRY_DIR" ] || { print_status "ERROR" "--entry-dir is required"; return 1; }
    [ -n "$FLOW_GROUP" ] || { print_status "ERROR" "--flow-group is required"; return 1; }
    [ -n "$PRODUCER" ] || { print_status "ERROR" "--producer is required"; return 1; }
    check_local_dir "$FIXTURE_DIR" "Fixture directory" || return 1
    check_local_dir "$ENTRY_DIR" "Entry directory" || return 1

    validation_name="$(build_validation_name)"
    target_dir="$ENTRY_DIR/$validation_name"
    if [ -e "$target_dir" ]; then
        print_status "ERROR" "Validation target already exists: $target_dir"
        return 1
    fi

    mkdir -p "$target_dir"
    cp -R "$FIXTURE_DIR"/. "$target_dir"/
    {
        printf 'validation_name\t%s\n' "$validation_name"
        printf 'flow_group\t%s\n' "$FLOW_GROUP"
        printf 'producer\t%s\n' "$PRODUCER"
        printf 'created_at_utc\t%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    } > "$target_dir/lz_validation.marker"

    print_status "OK" "Validation run injected: $target_dir"
}

COMMAND=""
FIXTURE_DIR=""
ENTRY_DIR=""
NEXT_HOP=""
NEXT_HOP_PORT=""
FLOW_GROUP=""
PRODUCER=""
TOKEN=""

if [ "$#" -eq 0 ]; then
    usage
    exit 1
fi

case "$1" in
    --help|-h)
        usage
        exit 0
        ;;
esac

COMMAND="$1"
shift

while [ "$#" -gt 0 ]; do
    case "$1" in
        --fixture-dir)
            FIXTURE_DIR="$2"
            shift 2
            ;;
        --entry-dir)
            ENTRY_DIR="$2"
            shift 2
            ;;
        --next-hop)
            NEXT_HOP="$2"
            shift 2
            ;;
        --next-hop-port)
            NEXT_HOP_PORT="$2"
            shift 2
            ;;
        --flow-group)
            FLOW_GROUP="$2"
            shift 2
            ;;
        --producer)
            PRODUCER="$2"
            shift 2
            ;;
        --token)
            TOKEN="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            print_status "ERROR" "Unknown argument: $1"
            usage
            exit 1
            ;;
    esac
done

case "$COMMAND" in
    preflight)
        run_preflight
        ;;
    run)
        run_validation
        ;;
    *)
        print_status "ERROR" "Unknown command: $COMMAND"
        usage
        exit 1
        ;;
esac
