#!/usr/bin/env bash
# HOST-1 OS overhead benchmark harness for headless Linux (design 0114, llama.cpp b12810).
# Strictly bash 3.2 compatible; emits no host names, user names, or IP addresses.

set -eu

STATE="state2"
CPU_DIR="/tmp/host1/bin/ubuntu-x64"
VULKAN_DIR="/tmp/host1/bin/ubuntu-vulkan-x64"
CUDA_DIR="/tmp/host1/bin/ubuntu-cuda-13.5-x64"
MODEL_06B="/tmp/host1/models/Qwen3-0.6B-Q8_0.gguf"
MODEL_8B="/tmp/host1/models/Qwen3-8B-Q4_K_M.gguf"
OUT_DIR=""
REPS=5
PROMPT_TOKENS=512
GEN_TOKENS=128
TUNED=0
CPU_MASK=""
PRIO=2
DRY_RUN=0

usage() {
    cat <<'EOF'
Usage: run_host1.sh [options]
  --state <state2|state3|custom>   State label (default: state2)
  --cpu-dir <dir>                  Directory holding CPU llama-bench
  --vulkan-dir <dir>               Directory holding Vulkan llama-bench
  --cuda-dir <dir>                 Directory holding CUDA llama-bench + cudart libs
  --model-06b <path>               Path to Qwen3-0.6B-Q8_0.gguf
  --model-8b <path>                Path to Qwen3-8B-Q4_K_M.gguf
  --out-dir <dir>                  Output folder (default: /tmp/host1-results/<ts>-<state>)
  --reps <n>                       llama-bench repetitions (default: 5)
  --prompt <n>                     Prompt tokens -p (default: 512)
  --gen <n>                        Generated tokens -n (default: 128)
  --tuned                          Enable CPU mask, strict affinity, priority, and -lm none/mlock variants
  --cpu-mask <hex>                 Override auto-detected physical-core hex mask (with --tuned)
  --prio <n>                       llama-bench --prio value when --tuned (default: 2)
  --dry-run                        Write machine_facts.json only without running llama-bench
  -h, --help                       Show this help
EOF
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --state) STATE="$2"; shift 2 ;;
        --cpu-dir) CPU_DIR="$2"; shift 2 ;;
        --vulkan-dir) VULKAN_DIR="$2"; shift 2 ;;
        --cuda-dir) CUDA_DIR="$2"; shift 2 ;;
        --model-06b) MODEL_06B="$2"; shift 2 ;;
        --model-8b) MODEL_8B="$2"; shift 2 ;;
        --out-dir) OUT_DIR="$2"; shift 2 ;;
        --reps) REPS="$2"; shift 2 ;;
        --prompt) PROMPT_TOKENS="$2"; shift 2 ;;
        --gen) GEN_TOKENS="$2"; shift 2 ;;
        --tuned) TUNED=1; shift ;;
        --cpu-mask) CPU_MASK="$2"; shift 2 ;;
        --prio) PRIO="$2"; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

UTC_NOW="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
if [ -z "$OUT_DIR" ]; then
    STAMP="$(date -u '+%Y%m%dT%H%M%SZ')"
    OUT_DIR="/tmp/host1-results/${STAMP}-${STATE}"
fi
mkdir -p "$OUT_DIR"

json_escape() {
    printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

read_first_existing() {
    for p in "$@"; do
        if [ -r "$p" ]; then
            tr '\n' ' ' < "$p" | sed 's/[[:space:]]*$//'
            return 0
        fi
    done
    printf 'unknown'
}

detect_os() {
    if [ -r /etc/os-release ]; then
        pretty="$(awk -F= '/^PRETTY_NAME=/{gsub(/^"|"$/, "", $2); print $2; exit}' /etc/os-release)"
        printf '%s (%s)' "${pretty:-Linux}" "$(uname -sr)"
    else
        uname -sr
    fi
}

detect_cpu_model() {
    if [ -r /proc/cpuinfo ]; then
        m="$(awk -F: '/model name/{sub(/^[ \t]+/, "", $2); print $2; exit}' /proc/cpuinfo)"
        if [ -n "$m" ]; then
            printf '%s' "$m"
            return 0
        fi
    fi
    printf '%s' "unknown"
}

detect_logical_cores() {
    if command -v nproc >/dev/null 2>&1; then
        nproc
    elif [ -r /proc/cpuinfo ]; then
        awk '/^processor[ \t]*:/{c++} END{print (c>0?c:1)}' /proc/cpuinfo
    else
        printf '1'
    fi
}

detect_physical_cores() {
    if [ -r /proc/cpuinfo ]; then
        phys="$(awk -F: '
            /^processor[ \t]*:/ { proc=$2+0; has_p=1 }
            /^physical id[ \t]*:/ { pkg=$2+0 }
            /^core id[ \t]*:/ { core=$2+0; key=pkg ":" core; if (!(key in seen)) { seen[key]=1; cnt++ } }
            END { if (cnt>0) print cnt }
        ' /proc/cpuinfo)"
        if [ -n "$phys" ]; then
            printf '%s' "$phys"
            return 0
        fi
    fi
    detect_logical_cores
}

compute_phys_core_mask() {
    if [ ! -r /proc/cpuinfo ]; then
        printf ''
        return 0
    fi
    awk -F: '
        function finish_proc(   key, idx) {
            if (!have_proc) return
            key = pkg ":" core
            if (!(key in seen)) {
                seen[key] = 1
                pick[proc] = 1
                if (proc > maxp) maxp = proc
            }
            have_proc = 0
        }
        /^[ \t]*$/ { finish_proc(); next }
        /^processor[ \t]*:/ { finish_proc(); proc=$2+0; pkg=0; core=proc; have_proc=1 }
        /^physical id[ \t]*:/ { pkg=$2+0 }
        /^core id[ \t]*:/ { core=$2+0 }
        END {
            finish_proc()
            if (maxp < 0) exit
            nibbles = int(maxp / 4) + 1
            hex = "0123456789abcdef"
            out = ""
            for (n = nibbles - 1; n >= 0; n--) {
                v = 0
                for (b = 0; b < 4; b++) {
                    bit = n * 4 + b
                    if (bit in pick) {
                        p2 = 1
                        for (k = 0; k < b; k++) p2 = p2 * 2
                        v += p2
                    }
                }
                c = substr(hex, v + 1, 1)
                if (out != "" || c != "0") out = out c
            }
            if (out == "") out = "0"
            printf "0x%s", out
        }
    ' /proc/cpuinfo
}

detect_ram_mib() {
    if [ -r /proc/meminfo ]; then
        awk '/^MemTotal:/{printf "%.1f", $2/1024; exit}' /proc/meminfo
    else
        printf '0'
    fi
}

query_nvidia_smi() {
    if command -v nvidia-smi >/dev/null 2>&1; then
        nvidia-smi --query-gpu=name,driver_version,memory.total,memory.free --format=csv,noheader,nounits 2>/dev/null | head -n 1 || true
    fi
}

file_sha256_or_null() {
    if [ -r "$1" ]; then
        if command -v sha256sum >/dev/null 2>&1; then
            sha256sum "$1" | awk '{print "\"" $1 "\""}'
            return 0
        elif command -v shasum >/dev/null 2>&1; then
            shasum -a 256 "$1" | awk '{print "\"" $1 "\""}'
            return 0
        fi
    fi
    printf 'null'
}

scrub_json_file() {
    f="$1"
    if [ ! -f "$f" ]; then
        return 0
    fi
    if command -v python3 >/dev/null 2>&1; then
        python3 - "$f" <<'PYEOF'
import json, os, sys
p = sys.argv[1]
with open(p, "r", encoding="utf-8") as fh:
    data = json.load(fh)
def scrub(obj):
    if isinstance(obj, list):
        for item in obj:
            scrub(item)
    elif isinstance(obj, dict):
        if "model_filename" in obj and isinstance(obj["model_filename"], str):
            obj["model_filename"] = os.path.basename(obj["model_filename"])
scrub(data)
with open(p, "w", encoding="utf-8") as fh:
    json.dump(data, fh, indent=2)
PYEOF
    else
        sed -i.bak 's|"model_filename":[[:space:]]*"[^"]*/\([^"/]*\)"|"model_filename": "\1"|g' "$f"
        rm -f "${f}.bak"
    fi
}

OS_DESC="$(detect_os)"
CPU_MODEL="$(detect_cpu_model)"
PHYS_CORES="$(detect_physical_cores)"
LOG_CORES="$(detect_logical_cores)"
RAM_MIB="$(detect_ram_mib)"
GOVERNOR="$(read_first_existing /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor /sys/devices/system/cpu/cpufreq/policy0/scaling_governor)"
THP_ENABLED="$(read_first_existing /sys/kernel/mm/transparent_hugepage/enabled)"
THP_DEFRAG="$(read_first_existing /sys/kernel/mm/transparent_hugepage/defrag)"

if [ "$TUNED" -eq 1 ] && [ -z "$CPU_MASK" ]; then
    CPU_MASK="$(compute_phys_core_mask)"
fi

# Thread sweep: 1,4,8,<physical cores>, deduplicated & capped at logical cores
THREAD_SWEEP=""
for t in 1 4 8 "$PHYS_CORES"; do
    if [ "$t" -gt 0 ] 2>/dev/null && [ "$t" -le "$LOG_CORES" ] 2>/dev/null; then
        case ",${THREAD_SWEEP}," in
            *,"${t}",*) ;;
            *)
                if [ -z "$THREAD_SWEEP" ]; then THREAD_SWEEP="$t"; else THREAD_SWEEP="${THREAD_SWEEP},${t}"; fi
                ;;
        esac
    fi
done
THREAD_SWEEP="$(printf '%s' "$THREAD_SWEEP" | tr ',' '\n' | sort -n | tr '\n' ',' | sed 's/,$//')"

GPU_LINE="$(query_nvidia_smi)"
GPU_NAME="null"
GPU_DRIVER="null"
VRAM_TOTAL="null"
VRAM_FREE_BEFORE="null"
if [ -n "$GPU_LINE" ]; then
    GPU_NAME="\"$(json_escape "$(printf '%s' "$GPU_LINE" | awk -F, '{gsub(/^[ \t]+|[ \t]+$/, "", $1); print $1}')")\""
    GPU_DRIVER="\"$(json_escape "$(printf '%s' "$GPU_LINE" | awk -F, '{gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}')")\""
    VRAM_TOTAL="$(printf '%s' "$GPU_LINE" | awk -F, '{gsub(/[^0-9]/, "", $3); print ($3!=""?$3:"null")}')"
    VRAM_FREE_BEFORE="$(printf '%s' "$GPU_LINE" | awk -F, '{gsub(/[^0-9]/, "", $4); print ($4!=""?$4:"null")}')"
fi

MODEL06B_NAME="$(basename "$MODEL_06B")"
MODEL8B_NAME="$(basename "$MODEL_8B")"
MODEL06B_SHA="$(file_sha256_or_null "$MODEL_06B")"
MODEL8B_SHA="$(file_sha256_or_null "$MODEL_8B")"

TUNED_CPU_FLAGS=""
TUNED_GPU_FLAGS=""
if [ "$TUNED" -eq 1 ]; then
    if [ -n "$CPU_MASK" ]; then
        TUNED_CPU_FLAGS="-C $CPU_MASK --cpu-strict 1 --prio $PRIO"
    else
        TUNED_CPU_FLAGS="--cpu-strict 1 --prio $PRIO"
    fi
    TUNED_GPU_FLAGS="--prio $PRIO"
fi

PLANNED_CPU="llama-bench -m $MODEL06B_NAME -ngl 0 -t $THREAD_SWEEP -p $PROMPT_TOKENS -n $GEN_TOKENS -r $REPS -o json${TUNED_CPU_FLAGS:+ $TUNED_CPU_FLAGS}"
PLANNED_CUDA="llama-bench -m $MODEL8B_NAME -ngl 99 -p $PROMPT_TOKENS -n $GEN_TOKENS -r $REPS -o json${TUNED_GPU_FLAGS:+ $TUNED_GPU_FLAGS}"
PLANNED_VULKAN="llama-bench -m $MODEL8B_NAME -ngl 99 -p $PROMPT_TOKENS -n $GEN_TOKENS -r $REPS -o json${TUNED_GPU_FLAGS:+ $TUNED_GPU_FLAGS}"

VRAM_FREE_CUDA="null"
VRAM_FREE_VULKAN="null"

write_machine_facts() {
    cat > "${OUT_DIR}/machine_facts.json" <<EOF
{
  "utc_timestamp": "$(json_escape "$UTC_NOW")",
  "state": "$(json_escape "$STATE")",
  "tuned": $([ "$TUNED" -eq 1 ] && printf 'true' || printf 'false'),
  "dry_run": $([ "$DRY_RUN" -eq 1 ] && printf 'true' || printf 'false'),
  "engine_tag": "b12810",
  "os": "$(json_escape "$OS_DESC")",
  "cpu_model": "$(json_escape "$CPU_MODEL")",
  "physical_cores": ${PHYS_CORES},
  "logical_processors": ${LOG_CORES},
  "ram_mib": ${RAM_MIB},
  "gpu_name": ${GPU_NAME},
  "gpu_driver_version": ${GPU_DRIVER},
  "vram_total_mib": ${VRAM_TOTAL},
  "vram_free_before_load_mib": ${VRAM_FREE_BEFORE},
  "vram_free_before_cuda_mib": ${VRAM_FREE_CUDA},
  "vram_free_before_vulkan_mib": ${VRAM_FREE_VULKAN},
  "cpu_governor": "$(json_escape "$GOVERNOR")",
  "thp_enabled": "$(json_escape "$THP_ENABLED")",
  "thp_defrag": "$(json_escape "$THP_DEFRAG")",
  "cpu_mask": "$(json_escape "$CPU_MASK")",
  "thread_sweep": "$(json_escape "$THREAD_SWEEP")",
  "models": {
    "qwen3_0_6b": {
      "filename": "$(json_escape "$MODEL06B_NAME")",
      "sha256": ${MODEL06B_SHA}
    },
    "qwen3_8b": {
      "filename": "$(json_escape "$MODEL8B_NAME")",
      "sha256": ${MODEL8B_SHA}
    }
  },
  "planned_commands": {
    "cpu": "$(json_escape "$PLANNED_CPU")",
    "gpu_cuda": "$(json_escape "$PLANNED_CUDA")",
    "gpu_vulkan": "$(json_escape "$PLANNED_VULKAN")"
  }
}
EOF
}

if [ "$DRY_RUN" -eq 1 ]; then
    write_machine_facts
    printf 'HOST-1 dry run complete.\n'
    printf 'STATE=%s CPU=%s CORES=%s/%s\n' "$STATE" "$CPU_MODEL" "$PHYS_CORES" "$LOG_CORES"
    printf 'OUT_DIR=%s\n' "$OUT_DIR"
    exit 0
fi

find_bench_bin() {
    dir="$1"
    if [ -x "$dir/llama-bench" ]; then
        printf '%s' "$dir/llama-bench"
        return 0
    fi
    found="$(find "$dir" -maxdepth 3 -type f -name 'llama-bench' 2>/dev/null | head -n 1 || true)"
    if [ -n "$found" ]; then
        printf '%s' "$found"
        return 0
    fi
    printf 'llama-bench not found under %s\n' "$dir" >&2
    exit 1
}

if [ ! -r "$MODEL_06B" ] || [ ! -r "$MODEL_8B" ]; then
    echo "Missing model file(s): expected $(basename "$MODEL_06B") and $(basename "$MODEL_8B")" >&2
    exit 1
fi

CPU_BIN="$(find_bench_bin "$CPU_DIR")"
CUDA_BIN="$(find_bench_bin "$CUDA_DIR")"
VULKAN_BIN="$(find_bench_bin "$VULKAN_DIR")"

run_bench() {
    bin="$1"
    out_json="$2"
    shift 2
    bindir="$(dirname "$bin")"
    err_log="${out_json}.stderr.log"
    tmp_json="${out_json}.tmp"
    (
        export LD_LIBRARY_PATH="${bindir}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
        exec "$bin" "$@"
    ) > "$tmp_json" 2> "$err_log"
    mv "$tmp_json" "$out_json"
    scrub_json_file "$out_json"
}

run_cpu_suite() {
    set -- -m "$MODEL_06B" -ngl 0 -t "$THREAD_SWEEP" -p "$PROMPT_TOKENS" -n "$GEN_TOKENS" -r "$REPS" -o json
    if [ "$TUNED" -eq 1 ]; then
        if [ -n "$CPU_MASK" ]; then
            set -- "$@" -C "$CPU_MASK"
        fi
        set -- "$@" --cpu-strict 1 --prio "$PRIO"
    fi
    echo "[CPU] Running default load mode ($MODEL06B_NAME, threads=$THREAD_SWEEP)..."
    run_bench "$CPU_BIN" "$OUT_DIR/cpu_qwen3_0.6b_q8_0.json" "$@"

    if [ "$TUNED" -eq 1 ]; then
        echo "[CPU] Running tuned -lm none (no-mmap equivalent in b12810)..."
        run_bench "$CPU_BIN" "$OUT_DIR/cpu_qwen3_0.6b_q8_0_nommap.json" "$@" -lm none

        echo "[CPU] Running tuned -lm mlock (optional; depends on RLIMIT_MEMLOCK)..."
        if ! run_bench "$CPU_BIN" "$OUT_DIR/cpu_qwen3_0.6b_q8_0_mlock.json" "$@" -lm mlock; then
            printf 'mlock run failed (check ulimit -l / CAP_IPC_LOCK); see %s\n' \
                "cpu_qwen3_0.6b_q8_0_mlock.json.stderr.log" > "$OUT_DIR/cpu_mlock_error.txt"
            rm -f "$OUT_DIR/cpu_qwen3_0.6b_q8_0_mlock.json.tmp" "$OUT_DIR/cpu_qwen3_0.6b_q8_0_mlock.json"
        fi
    fi
}

run_gpu_suite() {
    backend_name="$1"
    bin_path="$2"
    stem="$3"
    set -- -m "$MODEL_8B" -ngl 99 -p "$PROMPT_TOKENS" -n "$GEN_TOKENS" -r "$REPS" -o json
    if [ "$TUNED" -eq 1 ]; then
        set -- "$@" --prio "$PRIO"
    fi
    echo "[$backend_name] Running default load mode ($MODEL8B_NAME, ngl=99)..."
    run_bench "$bin_path" "$OUT_DIR/${stem}.json" "$@"
    if [ "$TUNED" -eq 1 ]; then
        echo "[$backend_name] Running tuned -lm none..."
        run_bench "$bin_path" "$OUT_DIR/${stem}_nommap.json" "$@" -lm none
    fi
}

run_cpu_suite

g_cuda="$(query_nvidia_smi)"
if [ -n "$g_cuda" ]; then
    VRAM_FREE_CUDA="$(printf '%s' "$g_cuda" | awk -F, '{gsub(/[^0-9]/, "", $4); print ($4!=""?$4:"null")}')"
fi
run_gpu_suite "CUDA" "$CUDA_BIN" "gpu_cuda_qwen3_8b_q4_k_m"

g_vulk="$(query_nvidia_smi)"
if [ -n "$g_vulk" ]; then
    VRAM_FREE_VULKAN="$(printf '%s' "$g_vulk" | awk -F, '{gsub(/[^0-9]/, "", $4); print ($4!=""?$4:"null")}')"
fi
run_gpu_suite "Vulkan" "$VULKAN_BIN" "gpu_vulkan_qwen3_8b_q4_k_m"

write_machine_facts
printf 'HOST-1 Linux run finished (%s).\n' "$STATE"
printf 'OUT_DIR=%s\n' "$OUT_DIR"
