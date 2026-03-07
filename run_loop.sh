#!/bin/bash
# Auto-restarting wrapper for London Nervous System
# Restarts when the daemon signals code was changed (via .restart sentinel)
# Stop with Ctrl+C twice (once stops python, twice stops the loop)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"
[ -f "${REPO_ROOT}/venv/bin/activate" ] && source "${REPO_ROOT}/venv/bin/activate"

RESTART_FILE="${REPO_ROOT}/london/data/.restart"
LOG_FILE="${REPO_ROOT}/london/data/logs/loop.log"
MIN_UPTIME=10  # seconds — if system runs less than this, don't auto-restart

# Clean up any stale restart file
rm -f "$RESTART_FILE"

mkdir -p "${REPO_ROOT}/london/data/logs"

echo "=== London Nervous System loop starting at $(date) ===" | tee -a "$LOG_FILE"
echo "Will auto-restart when code changes are detected." | tee -a "$LOG_FILE"
echo "Ctrl+C once = stop current run. Twice = exit loop." | tee -a "$LOG_FILE"
echo ""

KEEP_RUNNING=true
CONSECUTIVE_FAST_CRASHES=0

trap 'echo ""; echo "Loop interrupted. Exiting."; KEEP_RUNNING=false' INT

while $KEEP_RUNNING; do
    echo "=== Starting London Nervous System at $(date) ===" | tee -a "$LOG_FILE"

    START_TIME=$(date +%s)

    # Run the system
    python3 -m london.run
    EXIT_CODE=$?

    END_TIME=$(date +%s)
    UPTIME=$((END_TIME - START_TIME))

    echo "=== Exited with code $EXIT_CODE after ${UPTIME}s at $(date) ===" | tee -a "$LOG_FILE"

    # Check if restart was requested via sentinel file
    if [ -f "$RESTART_FILE" ]; then
        REASON=$(cat "$RESTART_FILE" 2>/dev/null || echo "code changed")
        rm -f "$RESTART_FILE"

        # Safety: if system ran less than MIN_UPTIME seconds, don't restart
        # This prevents crash loops from broken code
        if [ $UPTIME -lt $MIN_UPTIME ]; then
            CONSECUTIVE_FAST_CRASHES=$((CONSECUTIVE_FAST_CRASHES + 1))
            echo "WARNING: System ran only ${UPTIME}s (crash #${CONSECUTIVE_FAST_CRASHES}). Reason: $REASON" | tee -a "$LOG_FILE"

            if [ $CONSECUTIVE_FAST_CRASHES -ge 3 ]; then
                echo "ABORT: 3 consecutive fast crashes. Likely broken code. Stopping." | tee -a "$LOG_FILE"
                echo "Fix the code manually and restart." | tee -a "$LOG_FILE"
                break
            fi

            echo "Retrying in 10 seconds..." | tee -a "$LOG_FILE"
            sleep 10
            continue
        fi

        CONSECUTIVE_FAST_CRASHES=0
        echo "Restart requested: $REASON" | tee -a "$LOG_FILE"
        echo "Restarting in 3 seconds..." | tee -a "$LOG_FILE"
        sleep 3
        continue
    fi

    # Any other exit = stop the loop
    echo "System stopped normally. Not restarting." | tee -a "$LOG_FILE"
    break
done

echo "=== Loop exited at $(date) ===" | tee -a "$LOG_FILE"
