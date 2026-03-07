#!/data/data/com.termux/files/usr/bin/bash
# ============================================================
#  Quick launcher — use this for manual one-tap runs
#  You can add this to Termux:Widget for a home screen button
# ============================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Activate venv
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
else
    echo "❌ Run setup.sh first!"
    exit 1
fi

# Parse arg: default = full
MODE="${1:-full}"

echo ""
echo "🛍️  Fashion Scraper — Mode: $MODE"
echo "─────────────────────────────────"

python main.py --mode "$MODE"
EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Completed successfully"
else
    echo "⚠️  Completed with errors (check logs/fashion_scraper.log)"
fi
echo ""
