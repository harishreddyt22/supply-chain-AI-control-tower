"""
ETL Bulk Loader
Loads shipments_master.csv and route_events.csv into PostgreSQL
Uses COPY command (fastest method) with chunk fallback
"""

import os
import io
import csv
import time
import logging
import pandas as pd
from typing import Dict

logger = logging.getLogger(__name__)

CHUNK_SIZE = int(os.environ.get('ETL_CHUNK_SIZE', 10000))


class CSVLoader:
    """Loads CSV files into PostgreSQL tables using COPY or batch INSERT"""

    def __init__(self, db_manager):
        self.db = db_manager

    def load_csv_to_table(self, csv_path: str, table: str,
                          skip_errors: bool = True) -> Dict:
        """
        Main entry: loads CSV into PostgreSQL table.
        Tries COPY first (fastest), falls back to chunk INSERT.
        """
        if not os.path.exists(csv_path):
            return {'status': 'error', 'message': f'File not found: {csv_path}'}

        file_size_mb = os.path.getsize(csv_path) / 1024 / 1024
        logger.info(f"Loading {csv_path} ({file_size_mb:.1f} MB) → {table}")

        start = time.time()
        try:
            result = self._copy_load(csv_path, table)
        except Exception as e:
            logger.warning(f"COPY failed ({e}), falling back to chunk INSERT")
            result = self._chunk_insert(csv_path, table, skip_errors)

        result['duration_sec'] = round(time.time() - start, 1)
        result['file_size_mb'] = round(file_size_mb, 1)
        return result

    def _copy_load(self, csv_path: str, table: str) -> Dict:
        """
        PostgreSQL COPY — fastest bulk load method.
        Requires the DB server to access the file path (local connections).
        """
        sql = f"""
            COPY {table}
            FROM '{os.path.abspath(csv_path)}'
            WITH (FORMAT csv, HEADER true, NULL '', DELIMITER ',')
        """
        from backend.db.database import DatabaseManager
        with DatabaseManager.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows_loaded = cur.rowcount

        return {
            'status': 'success',
            'method': 'COPY',
            'table': table,
            'rows_loaded': rows_loaded,
        }

    def _chunk_insert(self, csv_path: str, table: str, skip_errors: bool) -> Dict:
        """
        Chunk-based INSERT fallback using pandas + psycopg2 execute_batch.
        Works regardless of DB server file access.
        """
        from backend.db.database import DatabaseManager
        import psycopg2.extras

        total_loaded = 0
        total_errors = 0
        chunk_num = 0

        for chunk in pd.read_csv(csv_path, chunksize=CHUNK_SIZE,
                                  low_memory=False, on_bad_lines='skip'):
            chunk_num += 1
            chunk = chunk.where(pd.notnull(chunk), None)  # NaN → None

            try:
                cols = ','.join(chunk.columns)
                placeholders = ','.join(['%s'] * len(chunk.columns))
                sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

                data = [tuple(row) for row in chunk.itertuples(index=False, name=None)]

                with DatabaseManager.get_conn() as conn:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_batch(cur, sql, data, page_size=500)

                total_loaded += len(chunk)
                if chunk_num % 10 == 0:
                    logger.info(f"  Chunk {chunk_num}: {total_loaded:,} rows loaded so far")

            except Exception as e:
                total_errors += len(chunk)
                if not skip_errors:
                    raise
                logger.warning(f"Chunk {chunk_num} error (skipped): {e}")

        return {
            'status': 'success' if total_errors == 0 else 'partial',
            'method': 'chunk_insert',
            'table': table,
            'rows_loaded': total_loaded,
            'rows_errored': total_errors,
            'chunks': chunk_num,
        }

    def get_upload_progress(self, table: str) -> Dict:
        """Check how many rows are in the table so far"""
        from backend.db.database import DatabaseManager
        try:
            rows = DatabaseManager.execute_query(f"SELECT COUNT(*) AS cnt FROM {table}")
            return {'table': table, 'rows_loaded': rows[0]['cnt']}
        except Exception:
            return {'table': table, 'rows_loaded': 0}


class ETLPipelineRunner:
    """Orchestrates the full ETL pipeline: schema → load → validate"""

    def __init__(self, db_manager, loader: CSVLoader):
        self.db = db_manager
        self.loader = loader

    def run_full_pipeline(self, csv_dir: str) -> Dict:
        from backend.db.database import setup_schema

        results = {}

        # 1. Setup schema
        logger.info("Step 1: Setting up PostgreSQL schema...")
        schema_ok = setup_schema()
        results['schema_setup'] = 'success' if schema_ok else 'failed'

        if not schema_ok:
            return results

        # 2. Load shipments_master.csv
        ship_csv = os.path.join(csv_dir, 'shipments_master.csv')
        if os.path.exists(ship_csv):
            logger.info("Step 2: Loading shipments_master.csv...")
            results['shipments_master'] = self.loader.load_csv_to_table(
                ship_csv, 'shipments_master'
            )
        else:
            results['shipments_master'] = {'status': 'skipped', 'reason': 'File not found'}

        # 3. Load route_events.csv
        evt_csv = os.path.join(csv_dir, 'route_events.csv')
        if os.path.exists(evt_csv):
            logger.info("Step 3: Loading route_events.csv...")
            results['route_events'] = self.loader.load_csv_to_table(
                evt_csv, 'route_events'
            )
        else:
            results['route_events'] = {'status': 'skipped', 'reason': 'File not found'}

        # 4. Validate
        logger.info("Step 4: Validating row counts...")
        results['validation'] = {
            'shipments_master': self.db.get_table_stats('shipments_master'),
            'route_events': self.db.get_table_stats('route_events'),
        }

        results['pipeline_status'] = 'completed'
        return results

    def get_pipeline_status(self) -> Dict:
        """Return current status of all ETL sources"""
        sources = [
            {'source': 'gps_iot',        'name': 'GPS/IoT Tracker',      'frequency': '5s',       'table': 'route_events'},
            {'source': 'weather_api',    'name': 'Weather API',           'frequency': '15min',    'table': None},
            {'source': 'traffic_api',    'name': 'Traffic Data API',      'frequency': '10min',    'table': None},
            {'source': 'port_congestion','name': 'Port Congestion Feed',  'frequency': '30min',    'table': None},
            {'source': 'warehouse_logs', 'name': 'Warehouse ERP Logs',    'frequency': '1hr',      'table': None},
            {'source': 'historical_db',  'name': 'Historical Delivery DB','frequency': 'daily',    'table': 'shipments_master'},
            {'source': 'customs_api',    'name': 'Customs Clearance API', 'frequency': '20min',    'table': None},
            {'source': 'carrier_feed',   'name': 'Carrier Status Feed',   'frequency': '5min',     'table': None},
            {'source': 'kafka_stream',   'name': 'Apache Kafka Stream',   'frequency': 'realtime', 'table': 'route_events'},
        ]

        db_ok = self.db.is_available()
        result = []
        for s in sources:
            entry = dict(s)
            entry['db_connected'] = db_ok
            if db_ok and s.get('table'):
                stats = self.db.get_table_stats(s['table'])
                entry['row_count'] = stats.get('row_count', 0)
                entry['table_size'] = stats.get('size', 'N/A')
                entry['status'] = 'healthy' if stats.get('row_count', 0) > 0 else 'empty'
            else:
                entry['row_count'] = 0
                entry['status'] = 'db_unavailable' if not db_ok else 'no_table'
            result.append(entry)

        return result
