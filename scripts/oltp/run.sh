#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

case "${1:-historical}" in
    historical|hist|h)
        cd "$SCRIPT_DIR/generators/historical"
        exec python historical_generator.py
        ;;
    weekly|week|w)
        cd "$SCRIPT_DIR/generators/weekly"
        exec python weekly_generator.py
        ;;
    *)
        echo "Usage: $0 {historical|weekly}"
        exit 1
        ;;
esac