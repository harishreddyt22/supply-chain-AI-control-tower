"""
Database Module — PostgreSQL connection via psycopg2
Reads .env credentials, provides connection pool and query helpers
"""

import os
import time
import logging
from contextlib import contextmanager
from typing import List, Dict, Any, Optional

# Load .env manually (no external lib needed)
def _load_env():
    env_path = os.path.join(os.path.dirname(__file__), '../../.env')
    env_path = os.path.abspath(env_path)
    if os.path.exists(env_path):
        with open(env_path, encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    os.environ.setdefault(k.strip(), v.strip())

_load_env()

logger = logging.getLogger(__name__)

# ── Try importing psycopg2 ─────────────────────────────
try:
    import psycopg2
    import psycopg2.pool
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    logger.warning("psycopg2 not installed. DB features disabled. Install: pip install psycopg2-binary")


class DBConfig:
    HOST     = os.environ.get('DB_HOST', 'localhost')
    PORT     = int(os.environ.get('DB_PORT', 5432))
    NAME     = os.environ.get('DB_NAME', 'supply_chain_db')
    USER     = os.environ.get('DB_USER', 'postgres')
    PASSWORD = os.environ.get('DB_PASSWORD', '')
    SCHEMA   = os.environ.get('DB_SCHEMA', 'public')
    POOL_SIZE    = int(os.environ.get('DB_POOL_SIZE', 10))
    MAX_OVERFLOW = int(os.environ.get('DB_MAX_OVERFLOW', 20))

    @classmethod
    def dsn(cls):
        return (f"host={cls.HOST} port={cls.PORT} dbname={cls.NAME} "
                f"user={cls.USER} password={cls.PASSWORD}")


class DatabaseManager:
    """
    PostgreSQL connection pool manager.
    Provides query execution, streaming, and bulk-load helpers.
    """
    _pool: Optional[Any] = None

    @classmethod
    def init_pool(cls):
        if not PSYCOPG2_AVAILABLE:
            return False
        try:
            cls._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=DBConfig.POOL_SIZE + DBConfig.MAX_OVERFLOW,
                dsn=DBConfig.dsn()
            )
            logger.info(f"DB pool initialised → {DBConfig.HOST}:{DBConfig.PORT}/{DBConfig.NAME}")
            return True
        except Exception as e:
            logger.error(f"DB pool init failed: {e}")
            cls._pool = None
            return False

    @classmethod
    @contextmanager
    def get_conn(cls):
        if cls._pool is None:
            cls.init_pool()
        if cls._pool is None:
            raise RuntimeError("DB not available")
        conn = cls._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cls._pool.putconn(conn)

    @classmethod
    def execute_query(cls, sql: str, params=None, fetch=True) -> List[Dict]:
        """Run a query and return list of dicts"""
        with cls.get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                if fetch:
                    return [dict(r) for r in cur.fetchall()]
                return []

    @classmethod
    def execute_many(cls, sql: str, data: List[tuple]):
        """Batch insert"""
        with cls.get_conn() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql, data, page_size=1000)

    @classmethod
    def is_available(cls) -> bool:
        if not PSYCOPG2_AVAILABLE:
            return False
        try:
            with cls.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return True
        except Exception:
            return False

    @classmethod
    def table_exists(cls, table: str) -> bool:
        sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
        """
        try:
            rows = cls.execute_query(sql, (DBConfig.SCHEMA, table))
            return rows[0]['exists'] if rows else False
        except Exception:
            return False

    @classmethod
    def get_table_stats(cls, table: str) -> Dict:
        try:
            count_sql = f"SELECT COUNT(*) AS cnt FROM {DBConfig.SCHEMA}.{table}"
            rows = cls.execute_query(count_sql)
            count = rows[0]['cnt'] if rows else 0

            size_sql = """
                SELECT pg_size_pretty(pg_total_relation_size(%s)) AS size
            """
            size_rows = cls.execute_query(size_sql, (f"{DBConfig.SCHEMA}.{table}",))
            size = size_rows[0]['size'] if size_rows else 'N/A'
            return {'table': table, 'row_count': count, 'size': size}
        except Exception as e:
            return {'table': table, 'row_count': 0, 'size': 'N/A', 'error': str(e)}


# ── Schema setup SQL ───────────────────────────────────

CREATE_SHIPMENTS_SQL = """
CREATE TABLE IF NOT EXISTS shipments_master (
    shipment_id         VARCHAR(20) PRIMARY KEY,
    carrier             VARCHAR(50),
    origin_port         VARCHAR(60),
    destination_port    VARCHAR(60),
    transport_mode      VARCHAR(10),
    status              VARCHAR(30),
    goods_category      VARCHAR(40),
    currency            VARCHAR(5),
    created_date        DATE,
    eta_date            DATE,
    transit_hours       NUMERIC(8,1),
    distance_km         NUMERIC(10,1),
    weight_kg           NUMERIC(12,1),
    volume_m3           NUMERIC(10,2),
    value_usd           NUMERIC(16,2),
    freight_cost_usd    NUMERIC(14,2),
    num_containers      SMALLINT,
    num_stops           SMALLINT,
    delay_hours         NUMERIC(8,1),
    risk_score          NUMERIC(6,4),
    weather_severity    NUMERIC(6,4),
    port_congestion     NUMERIC(6,4),
    temperature_c       NUMERIC(5,1),
    priority_level      SMALLINT,
    insurance_required  BOOLEAN,
    ingested_at         TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ship_carrier   ON shipments_master(carrier);
CREATE INDEX IF NOT EXISTS idx_ship_status    ON shipments_master(status);
CREATE INDEX IF NOT EXISTS idx_ship_origin    ON shipments_master(origin_port);
CREATE INDEX IF NOT EXISTS idx_ship_dest      ON shipments_master(destination_port);
CREATE INDEX IF NOT EXISTS idx_ship_created   ON shipments_master(created_date);
CREATE INDEX IF NOT EXISTS idx_ship_risk      ON shipments_master(risk_score);
CREATE INDEX IF NOT EXISTS idx_ship_mode      ON shipments_master(transport_mode);
"""

CREATE_EVENTS_SQL = """
CREATE TABLE IF NOT EXISTS route_events (
    event_id            VARCHAR(20) PRIMARY KEY,
    shipment_id         VARCHAR(20),
    event_type          VARCHAR(40),
    event_timestamp     TIMESTAMP,
    location_name       VARCHAR(60),
    latitude            NUMERIC(10,6),
    longitude           NUMERIC(10,6),
    speed_knots         NUMERIC(6,1),
    heading_deg         NUMERIC(6,1),
    temperature_c       NUMERIC(5,1),
    humidity_pct        NUMERIC(5,1),
    shock_g             NUMERIC(5,2),
    delay_added_hours   NUMERIC(7,1),
    risk_score_delta    NUMERIC(7,4),
    port_wait_hours     NUMERIC(7,1),
    fuel_consumed_lt    NUMERIC(8,2),
    co2_kg              NUMERIC(8,2),
    sensor_type         VARCHAR(20),
    signal_quality      VARCHAR(15),
    anomaly_flag        BOOLEAN,
    alert_sent          BOOLEAN,
    ingested_at         TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_evt_shipment   ON route_events(shipment_id);
CREATE INDEX IF NOT EXISTS idx_evt_timestamp  ON route_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_evt_type       ON route_events(event_type);
CREATE INDEX IF NOT EXISTS idx_evt_location   ON route_events(location_name);
CREATE INDEX IF NOT EXISTS idx_evt_anomaly    ON route_events(anomaly_flag) WHERE anomaly_flag = TRUE;
"""


def setup_schema():
    """Create tables and indexes in PostgreSQL"""
    try:
        with DatabaseManager.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_SHIPMENTS_SQL)
                cur.execute(CREATE_EVENTS_SQL)
        logger.info("Schema setup complete.")
        return True
    except Exception as e:
        logger.error(f"Schema setup failed: {e}")
        return False
