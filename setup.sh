#!/data/data/com.termux/files/usr/bin/bash
# ============================================================
#  Fashion Scraper - Termux Setup Script
#  רץ פעם אחת בלבד לאחר התקנת Termux
# ============================================================
set -e

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC}  $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
error()   { echo -e "${RED}[ERR]${NC}  $1"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║   🛍️  Fashion Auto-Scraper — Termux Setup        ║"
echo "║   Android Automated Fashion Data Pipeline        ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ── 1. Update pkg & install system deps ──────────────────
info "Updating Termux packages..."
pkg update -y -o Dpkg::Options::="--force-confnew" 2>/dev/null || warn "pkg update had warnings"
pkg install -y python libxml2 libxslt openssl-dev clang make 2>/dev/null

# ── 2. Install cronie for scheduling ─────────────────────
info "Installing cronie (cron daemon)..."
pkg install -y cronie 2>/dev/null && success "cronie installed"

# ── 3. Install Termux:Boot app hint ──────────────────────
echo ""
warn "════════════════════════════════════════════════"
warn " IMPORTANT: Install 'Termux:Boot' from F-Droid"
warn " (NOT Play Store) for automatic startup!"
warn " https://f-droid.org/packages/com.termux.boot"
warn "════════════════════════════════════════════════"
echo ""

# ── 4. Python virtual environment ────────────────────────
info "Creating Python virtual environment..."
python -m venv venv 2>/dev/null || python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip -q

info "Installing Python dependencies (this takes a few minutes)..."
pip install -r requirements.txt -q
success "Dependencies installed"

# ── 5. Config template ───────────────────────────────────
if [ ! -f "config.env" ]; then
    info "Creating config.env template..."
    cat > config.env << 'ENVEOF'
# ════════════════════════════════════════════════
#  Fashion Scraper Configuration
#  מלא את הפרטים שלך בקובץ זה
# ════════════════════════════════════════════════

# Gemini API Key (חינמי - https://aistudio.google.com/app/apikey)
GEMINI_API_KEY=YOUR_GEMINI_API_KEY_HERE

# Firebase Project ID (https://console.firebase.google.com)
FIREBASE_PROJECT_ID=YOUR_PROJECT_ID_HERE

# Path to Firebase service account JSON
# הורד מ: Firebase Console → Project Settings → Service Accounts → Generate new private key
FIREBASE_CREDENTIALS_PATH=/data/data/com.termux/files/home/styles/config/firebase_credentials.json

# Scraping settings
REQUEST_DELAY_MIN=3
REQUEST_DELAY_MAX=8
MAX_PRODUCTS_PER_SITE=50
MAX_RETRIES=3

# AI settings
AI_BATCH_SIZE=5
AI_LANGUAGE=hebrew

# Schedule (cron format) - Default: 2:00 AM every night
CRON_SCHEDULE=0 2 * * *

# Sites to scrape (comma separated, no spaces)
# Options: renoir,renuar,castro,zara,next,fox,shein,lidor
ACTIVE_SITES=renoir,renuar,castro,zara,next,fox
ENVEOF
    success "config.env created — fill in your API keys!"
fi

# ── 6. Create Firebase credentials placeholder ──────────
mkdir -p config
if [ ! -f "config/firebase_credentials.json" ]; then
    cat > config/firebase_credentials.json << 'JSONEOF'
{
  "PLACEHOLDER": "Replace this file with your Firebase service account JSON",
  "Instructions": [
    "1. Go to https://console.firebase.google.com",
    "2. Select your project",
    "3. Project Settings → Service Accounts",
    "4. Click 'Generate new private key'",
    "5. Save the downloaded JSON as config/firebase_credentials.json"
  ]
}
JSONEOF
    warn "Replace config/firebase_credentials.json with your real Firebase credentials!"
fi

# ── 7. Setup Termux:Boot auto-start ──────────────────────
info "Configuring Termux:Boot auto-start..."
BOOT_DIR="$HOME/.termux/boot"
mkdir -p "$BOOT_DIR"
cat > "$BOOT_DIR/start-fashion-scraper.sh" << BOOTEOF
#!/data/data/com.termux/files/usr/bin/bash
# Termux:Boot - runs on phone boot
sleep 30  # wait for network
cd $SCRIPT_DIR
source venv/bin/activate
crond  # start cron daemon
echo "\$(date): Cron started on boot" >> logs/boot.log
BOOTEOF
chmod +x "$BOOT_DIR/start-fashion-scraper.sh"
success "Termux:Boot configured"

# ── 8. Setup cron job ────────────────────────────────────
info "Setting up cron schedule..."
CRON_CMD="cd $SCRIPT_DIR && source venv/bin/activate && python main.py --mode auto >> logs/cron.log 2>&1"
# Remove old entries and add fresh
(crontab -l 2>/dev/null | grep -v "fashion-scraper\|main.py" ; echo "0 2 * * * $CRON_CMD  # fashion-scraper") | crontab -
success "Cron job set for 02:00 AM daily"

# ── 9. Start cron daemon now ──────────────────────────────
crond 2>/dev/null || true

# ── 10. Create logs dir ───────────────────────────────────
mkdir -p logs data

# ── 11. Final instructions ────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║              ✅ SETUP COMPLETE!                      ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  NEXT STEPS (required before first run):             ║"
echo "║                                                      ║"
echo "║  1. Edit config.env:                                 ║"
echo "║     nano config.env                                  ║"
echo "║     → Add GEMINI_API_KEY                             ║"
echo "║     → Add FIREBASE_PROJECT_ID                        ║"
echo "║                                                      ║"
echo "║  2. Add Firebase credentials:                        ║"
echo "║     Replace config/firebase_credentials.json         ║"
echo "║     with your real service account JSON              ║"
echo "║                                                      ║"
echo "║  3. Test run:                                        ║"
echo "║     python main.py --mode test                       ║"
echo "║                                                      ║"
echo "║  4. Full run:                                        ║"
echo "║     python main.py --mode full                       ║"
echo "║                                                      ║"
echo "║  5. Check dashboard:                                 ║"
echo "║     python dashboard.py                              ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
success "Fashion Scraper is ready! Cron runs daily at 02:00 AM 🎉"
