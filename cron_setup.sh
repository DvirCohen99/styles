#!/data/data/com.termux/files/usr/bin/bash
# ============================================================
#  Configure cron schedule (run after setup.sh if needed)
#  Customize the time by editing CRON_TIME below.
# ============================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Default: every day at 02:00 AM (phone usually charging)
# Format: minute hour day month weekday
# Examples:
#   "0 2 * * *"    = every day at 2 AM
#   "0 */12 * * *" = every 12 hours
#   "0 2 */2 * *"  = every 2 days at 2 AM
CRON_TIME="${1:-0 2 * * *}"

CRON_CMD="cd $SCRIPT_DIR && source venv/bin/activate && python main.py --mode auto >> $SCRIPT_DIR/logs/cron.log 2>&1"

echo "Setting cron: [$CRON_TIME] $CRON_CMD"

(crontab -l 2>/dev/null | grep -v "fashion-scraper\|main.py" ; echo "$CRON_TIME $CRON_CMD  # fashion-scraper") | crontab -

echo "✅ Cron updated. Verify with: crontab -l"
crontab -l | grep "fashion-scraper"

# Ensure crond is running
if ! pgrep -x crond > /dev/null; then
    crond
    echo "✅ crond started"
else
    echo "✅ crond already running"
fi
