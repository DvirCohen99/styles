#!/data/data/com.termux/files/usr/bin/bash
# ============================================================
#  Termux:Boot startup script
#  Automatically starts cron daemon when phone boots.
#  Place this file in ~/.termux/boot/ (symlink created by setup.sh)
# ============================================================

# Wait for network (critical!)
sleep 45

SCRAPER_DIR="$HOME/styles"
LOG_FILE="$SCRAPER_DIR/logs/boot.log"

echo "$(date): Termux:Boot triggered" >> "$LOG_FILE"

cd "$SCRAPER_DIR" || exit 1

# Start cron daemon
if ! pgrep -x crond > /dev/null; then
    crond
    echo "$(date): crond started" >> "$LOG_FILE"
else
    echo "$(date): crond already running" >> "$LOG_FILE"
fi

# Optional: Acquire wake lock to prevent CPU sleep during scraping
# Requires Termux:API app
# termux-wake-lock 2>/dev/null || true

echo "$(date): Boot script complete" >> "$LOG_FILE"
