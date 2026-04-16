#!/usr/bin/env python3
"""
setup_db.py — One-time database setup script
Run this BEFORE starting the app:
  python setup_db.py

Steps:
  1. Connects to PostgreSQL using .env credentials
  2. Creates tables (shipments_master, route_events) with indexes
  3. Bulk-loads both CSV files into the tables
  4. Prints row counts and table sizes
"""

import os, sys, time, logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Add backend to path
sys.path.insert(0, os.path.dirname(__file__))

# Load .env
def load_env():
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_path):
        with open(env_path, encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    os.environ.setdefault(k.strip(), v.strip())
        logger.info(f"Loaded .env from {env_path}")
    else:
        logger.warning(".env not found — using environment variables directly")

load_env()

# ── Check psycopg2 ──────────────────────────────────────
try:
    import psycopg2
    logger.info("psycopg2 available ✓")
except ImportError:
    logger.error("psycopg2 not installed!")
    logger.error("Run: pip install psycopg2-binary")
    sys.exit(1)

from backend.db.database import DatabaseManager, setup_schema, DBConfig
from backend.etl.loader import CSVLoader, ETLPipelineRunner

CSV_DIR = os.path.join(os.path.dirname(__file__), 'data', 'csv')


def print_banner():
    print("\n" + "═" * 60)
    print("  Supply Chain AI — Database Setup")
    print("═" * 60)
    print(f"  Host    : {DBConfig.HOST}:{DBConfig.PORT}")
    print(f"  Database: {DBConfig.NAME}")
    print(f"  User    : {DBConfig.USER}")
    print(f"  CSV Dir : {CSV_DIR}")
    print("═" * 60 + "\n")


def check_csvs():
    csvs = {
        'shipments_master.csv': os.path.join(CSV_DIR, 'shipments_master.csv'),
        'route_events.csv':     os.path.join(CSV_DIR, 'route_events.csv'),
    }
    all_ok = True
    for name, path in csvs.items():
        if os.path.exists(path):
            size_mb = os.path.getsize(path) / 1024 / 1024
            logger.info(f"  ✓ {name} ({size_mb:.0f} MB)")
        else:
            logger.warning(f"  ✗ {name} NOT FOUND at {path}")
            all_ok = False
    return all_ok


def main():
    print_banner()

    # 1. Check CSV files
    logger.info("Checking CSV files...")
    csv_ok = check_csvs()
    if not csv_ok:
        logger.warning("Some CSV files missing. Only found files will be loaded.")

    # 2. Test DB connection
    logger.info(f"\nConnecting to PostgreSQL at {DBConfig.HOST}:{DBConfig.PORT}...")
    if not DatabaseManager.is_available():
        logger.error("Cannot connect to PostgreSQL!")
        logger.error("Check: host, port, dbname, user, password in .env")
        sys.exit(1)
    logger.info("Connection successful ✓")

    # 3. Setup schema
    logger.info("\nCreating tables and indexes...")
    if not setup_schema():
        logger.error("Schema setup failed!")
        sys.exit(1)
    logger.info("Tables created ✓")

    # 4. Load CSVs
    loader = CSVLoader(DatabaseManager)
    runner = ETLPipelineRunner(DatabaseManager, loader)

    logger.info("\nStarting bulk CSV load (this may take several minutes for 50L records)...")
    logger.info("Progress is logged every 10 chunks...\n")

    total_start = time.time()
    results = runner.run_full_pipeline(CSV_DIR)
    total_elapsed = time.time() - total_start

    # 5. Print results
    print("\n" + "═" * 60)
    print("  LOAD RESULTS")
    print("═" * 60)
    for key, val in results.items():
        if isinstance(val, dict):
            print(f"\n  [{key}]")
            for k, v in val.items():
                if isinstance(v, dict):
                    print(f"    {k}:")
                    for kk, vv in v.items():
                        print(f"      {kk}: {vv}")
                else:
                    print(f"    {k}: {v}")
        else:
            print(f"  {key}: {val}")

    print(f"\n  Total time: {total_elapsed:.1f} seconds ({total_elapsed/60:.1f} minutes)")
    print("═" * 60)
    print("\n✅ Database setup complete!")
    print("   Now run:  python backend/app.py")
    print("   Or:       .\\run_local.ps1\n")


if __name__ == '__main__':
    main()
