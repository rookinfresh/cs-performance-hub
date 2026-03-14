#!/bin/bash
# Refresh CS Perf Hub data from Snowflake and update the committed snapshot.
#
# Usage:
#   bash scripts/sync-snowflake.sh
#
# Then commit and push to deploy:
#   git add public/cs_perf_data.json
#   git commit -m "chore: refresh CS perf data"
#   git push

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV="$HOME/Documents/Development/tools/mcp-snowflake-server/venv"
EXPORT_SCRIPT="$HOME/Documents/Development/sigma-mcp/export_cs_perf_data.py"
OUTPUT="$PROJECT_DIR/public/cs_perf_data.json"

echo "Activating venv..."
source "$VENV/bin/activate"

echo "Querying Snowflake..."
OUTPUT_PATH="$OUTPUT" python "$EXPORT_SCRIPT"

echo ""
echo "✓  public/cs_perf_data.json updated"
echo ""
echo "Next steps:"
echo "  git add public/cs_perf_data.json"
echo "  git commit -m 'chore: refresh CS perf data'"
echo "  git push"
