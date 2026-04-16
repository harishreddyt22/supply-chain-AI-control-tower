"""
Live Stream Module
Reads REAL records from PostgreSQL — 5 records every 10 seconds
Streams via WebSocket to dashboard (no random generation)
"""

import os
import json
import time
import logging
import threading
from datetime import datetime
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# Stream config from .env
BATCH_SIZE   = int(os.environ.get('STREAM_BATCH_SIZE', 5))
INTERVAL_SEC = int(os.environ.get('STREAM_INTERVAL_SECONDS', 10))
STREAM_TABLE = os.environ.get('STREAM_TABLE', 'route_events')
ORDER_COL    = os.environ.get('STREAM_ORDER_COL', 'event_timestamp')
STATE_FILE   = os.path.abspath(os.environ.get('STREAM_STATE_FILE', './data/stream_state.json'))


class LiveStreamReader:
    """
    Reads real rows from PostgreSQL in sequential batches.
    Every INTERVAL_SEC seconds, fetches BATCH_SIZE rows starting
    from the last-seen offset (cursor-based pagination).
    Wraps around when end of table is reached.
    """

    def __init__(self, db_manager, socketio_emit: Callable):
        self.db = db_manager
        self.emit = socketio_emit
        self._active = False
        self._thread: Optional[threading.Thread] = None
        self._offset = 0          # current cursor position
        self._total_rows = None   # cached total count
        self._db_available = False
        self._processed_records = 0
        self._last_batch_at = None
        self._state_lock = threading.Lock()
        self._load_state()

    def _load_state(self):
        try:
            if not os.path.exists(STATE_FILE):
                return
            with open(STATE_FILE, encoding='utf-8') as f:
                state = json.load(f)
            self._offset = int(state.get('offset', 0) or 0)
            self._processed_records = int(state.get('processed_records', 0) or 0)
            self._last_batch_at = state.get('last_batch_at')
            logger.info(f"Loaded stream state: offset={self._offset}, processed={self._processed_records}")
        except Exception as e:
            logger.warning(f"Could not load stream state: {e}")

    def _save_state(self):
        try:
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
            state = {
                'offset': self._offset,
                'processed_records': self._processed_records,
                'last_batch_at': self._last_batch_at,
                'table': STREAM_TABLE,
                'batch_size': BATCH_SIZE,
                'interval_sec': INTERVAL_SEC,
            }
            with self._state_lock:
                with open(STATE_FILE, 'w', encoding='utf-8') as f:
                    json.dump(state, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not save stream state: {e}")

    def _get_total_rows(self) -> int:
        try:
            rows = self.db.execute_query(
                f"SELECT COUNT(*) AS cnt FROM {STREAM_TABLE}"
            )
            return rows[0]['cnt'] if rows else 0
        except Exception as e:
            logger.warning(f"Cannot count {STREAM_TABLE}: {e}")
            return 0

    def _fetch_batch(self) -> list:
        """Fetch BATCH_SIZE rows from DB at current offset"""
        sql = f"""
            SELECT *
            FROM   {STREAM_TABLE}
            ORDER  BY {ORDER_COL}
            LIMIT  {BATCH_SIZE}
            OFFSET {self._offset}
        """
        try:
            rows = self.db.execute_query(sql)
            return rows
        except Exception as e:
            logger.error(f"Stream fetch error: {e}")
            return []

    def _serialize(self, row: dict) -> dict:
        """Make row JSON-serialisable"""
        out = {}
        for k, v in row.items():
            if hasattr(v, 'isoformat'):
                out[k] = v.isoformat()
            elif v is None:
                out[k] = None
            else:
                out[k] = v
        return out

    def _stream_loop(self):
        """Main loop: fetch → emit → wait → repeat"""
        logger.info(f"Stream started: table={STREAM_TABLE}, batch={BATCH_SIZE}, interval={INTERVAL_SEC}s")

        # Check DB availability first
        self._db_available = self.db.is_available()
        if not self._db_available:
            logger.warning("DB not available. Stream will emit status messages only.")

        # Cache total rows
        if self._db_available:
            self._total_rows = self._get_total_rows()
            logger.info(f"Total rows in {STREAM_TABLE}: {self._total_rows:,}")

        tick = 0
        while self._active:
            tick += 1
            batch_start = time.time()

            if self._db_available and self._total_rows and self._total_rows > 0:
                # Wrap around when we've reached end of table
                if self._offset >= self._total_rows:
                    self._offset = 0
                    logger.info("Stream wrapped around to beginning of table")

                rows = self._fetch_batch()

                if rows:
                    current_offset = self._offset
                    payload = {
                        'tick': tick,
                        'table': STREAM_TABLE,
                        'offset': current_offset,
                        'batch_size': len(rows),
                        'total_rows': self._total_rows,
                        'timestamp': datetime.now().isoformat(),
                        'records': [self._serialize(r) for r in rows],
                        'processed_records': self._processed_records + len(rows),
                        'source': 'postgresql',
                    }
                    self.emit('live_stream_batch', payload)
                    self._offset += len(rows)
                    self._processed_records += len(rows)
                    self._last_batch_at = payload['timestamp']
                    self._save_state()
                    logger.debug(f"Tick {tick}: emitted {len(rows)} rows, offset={self._offset}")
                else:
                    self._offset = 0  # Reset if no rows returned
                    self._save_state()

            else:
                # DB not available — emit status so UI can show it
                self.emit('stream_status', {
                    'tick': tick,
                    'status': 'db_unavailable',
                    'message': 'PostgreSQL not connected. Check .env credentials and run setup_db.py',
                    'timestamp': datetime.now().isoformat(),
                })

            # Emit stream health every 5 ticks
            if tick % 5 == 0:
                self.emit('stream_health', {
                    'tick': tick,
                    'offset': self._offset,
                    'total_rows': self._total_rows or 0,
                    'table': STREAM_TABLE,
                    'batch_size': BATCH_SIZE,
                    'interval_sec': INTERVAL_SEC,
                    'db_connected': self._db_available,
                    'timestamp': datetime.now().isoformat(),
                })

            # Sleep for remaining interval
            elapsed = time.time() - batch_start
            sleep_for = max(0, INTERVAL_SEC - elapsed)
            time.sleep(sleep_for)

        logger.info("Stream stopped.")

    def start(self):
        if self._active:
            return
        self._active = True
        self._thread = threading.Thread(target=self._stream_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._active = False
        if self._thread:
            self._thread.join(timeout=INTERVAL_SEC + 2)
        self._save_state()
        logger.info("Stream thread stopped.")

    @property
    def is_running(self) -> bool:
        return self._active

    def get_status(self) -> dict:
        return {
            'running': self._active,
            'offset': self._offset,
            'total_rows': self._total_rows or 0,
            'table': STREAM_TABLE,
            'batch_size': BATCH_SIZE,
            'interval_sec': INTERVAL_SEC,
            'db_connected': self._db_available,
            'processed_records': self._processed_records,
            'last_batch_at': self._last_batch_at,
        }
