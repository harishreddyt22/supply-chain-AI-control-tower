#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════
# Supply Chain AI — Local Startup Script
# ═══════════════════════════════════════════════════════
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  🚀  Supply Chain AI — PostgreSQL Control Tower"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Virtual environment
if [ ! -d "venv" ]; then
  echo "📦 Creating virtual environment..."
  python3 -m venv venv
fi
source venv/bin/activate

# Install deps
echo "📥 Installing dependencies..."
pip install -r requirements.txt -q

# Create directories
mkdir -p data/uploads data/csv

echo ""
echo "📋 SETUP CHECKLIST"
echo "  1. Fill in .env with your PostgreSQL credentials"
echo "  2. Run: python setup_db.py   (loads 50L+ records into DB)"
echo "  3. Then this script starts the app"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  🌐  Dashboard:        http://localhost:5000/dashboard"
echo "  ⤡  Route Optimizer:  http://localhost:5000/routing"
echo "  ◎  Analytics:        http://localhost:5000/analytics"
echo "  ⇅  Data Ingestion:   http://localhost:5000/ingestion"
echo "  🗄  pgAdmin:          http://localhost:5050"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

python backend/app.py
