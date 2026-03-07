# 🛍️ Fashion Auto-Scraper — Android + Termux

> **Set & Forget** — מאגר מוצרי אופנה אוטומטי לגמרי ב-Firestore.
> רץ בלילה כשהטלפון טוען, ללא מגע ידני אחרי ההגדרה הראשונית.

---

## 🏗️ ארכיטקטורה

```
Termux (Android)
  ├── crond  ──► 02:00 בלילה בכל יום
  │              └── main.py
  │                   ├── scrapers/ (8 אתרים)
  │                   ├── Gemini Flash AI  ──► תיאורים + תגיות
  │                   └── Firestore  ──► upsert אוטומטי
  └── Termux:Boot  ──► מפעיל crond בכל אתחול טלפון
```

**אתרים נתמכים:** Renoir · Renuar · Castro · Zara IL · Next IL · Fox · Shein IL · Lidor

---

## ⚡ התקנה מהירה (20 דקות)

### שלב 1: הכן את Termux

```bash
# התקן מ-F-Droid (לא Play Store!):
# • Termux
# • Termux:Boot
# • (אופציונלי) Termux:Widget

# פתח Termux ורוץ:
pkg update -y
pkg install -y git
```

### שלב 2: שכפל את הפרויקט

```bash
cd ~
git clone <URL_של_הפרויקט> styles
cd styles
```

### שלב 3: קבל API Keys (חינמי)

#### Gemini API Key:
1. כנס ל-[aistudio.google.com/app/apikey](https://aistudio.google.com/app/apikey)
2. לחץ "Create API key"
3. העתק את המפתח

#### Firebase Service Account:
1. כנס ל-[console.firebase.google.com](https://console.firebase.google.com)
2. צור פרויקט חדש (או השתמש בקיים)
3. הפעל **Firestore Database** (mode: production)
4. Project Settings → Service Accounts → **Generate new private key**
5. הורד את JSON

### שלב 4: הגדר config

```bash
nano config.env
```

מלא:
```env
GEMINI_API_KEY=AIza...  # המפתח שלך
FIREBASE_PROJECT_ID=my-project-123  # שם הפרויקט
ACTIVE_SITES=renoir,renuar,castro,zara,next,fox
MAX_PRODUCTS_PER_SITE=30
```

העלה את Firebase credentials JSON לטלפון:
```bash
# העתק את הקובץ לנתיב:
cp /sdcard/Download/my-project-firebase-adminsdk-xxx.json config/firebase_credentials.json
```

### שלב 5: הרץ setup

```bash
bash setup.sh
```

### שלב 6: בדיקה ראשונה

```bash
source venv/bin/activate
python main.py --mode test
```

אם הכל עובד — תראה מוצר אחד עם תיאור AI ב-Firestore! ✅

### שלב 7: הפעלה מלאה

```bash
python main.py --mode full
```

---

## 🤖 אוטומציה מלאה

### Termux:Boot (אוטומטי באתחול):
```bash
mkdir -p ~/.termux/boot
cp .termux-boot/start-scraper.sh ~/.termux/boot/
chmod +x ~/.termux/boot/start-scraper.sh
```

### Cron (מתוזמן לילי):
```bash
# בדוק שה-cron פעיל:
crontab -l

# שנה שעה אם רוצה (ברירת מחדל: 02:00):
bash cron_setup.sh "0 3 * * *"  # 03:00 במקום

# הפעל crond עכשיו:
crond
```

### Dashboard:
```bash
python dashboard.py
```

---

## 📊 שדות Firestore לכל מוצר

| שדה | תיאור |
|-----|--------|
| `product_id` | MD5 hash ייחודי |
| `site` | שם האתר |
| `name` | שם המוצר |
| `original_url` | קישור מקורי |
| `scrape_date` | תאריך סריקה אחרון |
| `first_seen_date` | תאריך גילוי ראשון |
| `description_short` | תיאור גולמי מהאתר |
| `description_ai_expanded` | תיאור שיווקי AI (200-400 מילה, עברית) |
| `tags` | מערך 10-20 תגיות (עברית+אנגלית) |
| `colors_available` | צבעים זמינים |
| `sizes_available` | מידות זמינות |
| `price` | מחיר נוכחי (ILS) |
| `original_price` | מחיר מקורי |
| `discount_percentage` | אחוז הנחה |
| `is_on_sale` | האם במכירה |
| `images` | מערך URLs של תמונות (3-6) |
| `category` | קטגוריה ראשית |
| `sub_category` | תת-קטגוריה |

---

## 🆓 מגבלות Free Tier

| שירות | מגבלה חינמית | שימוש שלנו |
|--------|-------------|------------|
| Gemini Flash | 1,500 req/day, 15 RPM | ~30-50 req/ריצה |
| Firestore writes | 20k/day | ~200/ריצה |
| Firestore reads | 50k/day | ~500/ריצה |

---

## 🔧 פקודות שימושיות

```bash
python main.py --mode full          # הפעלה מלאה
python main.py --mode site zara     # אתר ספציפי
python main.py --mode test          # בדיקה (3 מוצרים)
python dashboard.py                 # סטטוס
tail -f logs/fashion_scraper.log    # לוגים בזמן אמת
tail -f logs/cron.log               # לוג cron
```

---

## 📁 מבנה הפרויקט

```
styles/
├── main.py              # נקודת כניסה ראשית
├── config.py            # הגדרות מרכזיות
├── config.env           # API Keys שלך (לא ב-git)
├── dashboard.py         # ממשק סטטוס
├── run.sh               # הפעלה חד-לחיצה
├── setup.sh             # התקנה ראשונית
├── cron_setup.sh        # הגדרת תזמון
├── requirements.txt
├── scrapers/
│   ├── base.py          # Base class
│   ├── renoir.py        # Renoir (Shopify HTML)
│   ├── renuar.py        # Renuar (Shopify JSON API)
│   ├── zara.py          # Zara (internal REST API)
│   ├── castro.py        # Castro
│   ├── next_il.py       # Next IL + JSON-LD
│   ├── fox.py           # Fox Fashion
│   ├── shein.py         # Shein IL API
│   ├── lidor.py         # Lidor
│   └── registry.py
├── ai/
│   └── processor.py     # Gemini AI (batched)
├── db/
│   └── firestore.py     # Firebase upsert
├── utils/
│   ├── logger.py
│   └── rate_limiter.py
└── config/
    └── firebase_credentials.json   # Firebase SA key
```

---

## 🚨 טיפים

1. **חשמל בלילה** — חבר טלפון למטען. Cron רץ ב-02:00.
2. **Termux:Boot** — מ-**F-Droid בלבד**, לא Play Store.
3. **Doze Mode** — הוסף Termux ל-"Don't optimize battery" בהגדרות.
4. **Rate limiting** — ברירת מחדל: 3-8 שניות בין בקשות. אל תשנה מתחת ל-2 שניות.
