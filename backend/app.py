"""
Supply Chain AI — Main Application (PostgreSQL-backed)
Real records from DB streamed every 10 seconds (5 records/batch)
"""

import os, sys, logging
from datetime import datetime

# Load .env first
def _load_env():
    env_path = os.path.join(os.path.dirname(__file__), '../.env')
    env_path = os.path.abspath(env_path)
    if os.path.exists(env_path):
        with open(env_path, encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    os.environ.setdefault(k.strip(), v.strip())
_load_env()

from flask import Flask, render_template, jsonify, request, send_from_directory
from flask_socketio import SocketIO, emit
from flask_cors import CORS

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from backend.db.database import DatabaseManager, setup_schema
from backend.ml.disruption_model import DisruptionPredictor
from backend.ml.time_series import TimeSeriesForecaster
from backend.routing.optimizer import RouteOptimizer
from backend.ingestion.data_processor import DataProcessor
from backend.ingestion.live_stream import LiveStreamReader
from backend.etl.loader import CSVLoader, ETLPipelineRunner

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__,
            template_folder='../frontend/templates',
            static_folder='../frontend/static')
app.config['SECRET_KEY']       = os.environ.get('SECRET_KEY', 'dev-secret')
app.config['UPLOAD_FOLDER']    = os.environ.get('UPLOAD_FOLDER', './data/uploads')
app.config['MAX_CONTENT_LENGTH'] = int(os.environ.get('MAX_UPLOAD_MB', 100)) * 1024 * 1024

CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# ── Init DB ──────────────────────────────────────────────
DatabaseManager.init_pool()
logger.info(f"PostgreSQL available: {DatabaseManager.is_available()}")


def is_db_available():
    return DatabaseManager.is_available()

# ── Init components ────────────────────────────────────
predictor  = DisruptionPredictor()
forecaster = TimeSeriesForecaster(db_manager=DatabaseManager)
optimizer  = RouteOptimizer(db_manager=DatabaseManager)
processor  = DataProcessor(db_manager=DatabaseManager)
loader     = CSVLoader(DatabaseManager)
etl_runner = ETLPipelineRunner(DatabaseManager, loader)

# ── Live stream (real DB rows) ─────────────────────────
def _emit(event, data):
    socketio.emit(event, data)

stream_reader = LiveStreamReader(
    db_manager=DatabaseManager,
    socketio_emit=_emit
)


# ══════════════════════════════════════════════════════
# PAGES
# ══════════════════════════════════════════════════════

@app.route('/')
def index(): return render_template('index.html')

@app.route('/dashboard')
def dashboard(): return render_template('dashboard.html')

@app.route('/routing')
def routing_page(): return render_template('routing.html')

@app.route('/analytics')
def analytics(): return render_template('analytics.html')

@app.route('/ingestion')
def ingestion(): return render_template('ingestion.html')


# ══════════════════════════════════════════════════════
# API — Shipments & KPIs
# ══════════════════════════════════════════════════════

@app.route('/api/shipments')
def get_shipments():
    limit = request.args.get('limit', 50, type=int)
    rows = processor.get_recent_shipments(limit=limit)
    if not rows:
        return jsonify({'status': 'ok', 'data': [], 'db_connected': is_db_available(),
                        'message': 'PostgreSQL not connected. Run setup_db.py first.'})
    return jsonify({'status': 'ok', 'data': rows,
                    'count': len(rows), 'timestamp': datetime.now().isoformat()})


@app.route('/api/kpis')
def get_kpis():
    kpis = processor.get_kpis_from_db()
    return jsonify({'status': 'ok', 'data': kpis})


@app.route('/api/risk-heatmap')
def get_risk_heatmap():
    data = processor.get_risk_heatmap_from_db()
    return jsonify({'status': 'ok', 'data': data,
                    'count': len(data), 'db_connected': is_db_available()})


@app.route('/api/bottlenecks')
def get_bottlenecks():
    data = processor.detect_bottlenecks_from_db()
    return jsonify({'status': 'ok', 'data': data,
                    'count': len(data), 'db_connected': is_db_available()})


@app.route('/api/disruptions')
def get_disruptions():
    rows = processor.get_recent_shipments(limit=30)
    preds = predictor.predict_from_db_rows(rows)
    flagged = [p for p in preds if p.get('ml_prediction', {}).get('flagged')]
    return jsonify({'status': 'ok', 'data': preds,
                    'flagged_count': len(flagged),
                    'timestamp': datetime.now().isoformat()})


# ══════════════════════════════════════════════════════
# API — Forecasting
# ══════════════════════════════════════════════════════

@app.route('/api/forecast')
def get_forecast():
    metric  = request.args.get('metric', 'delay_probability')
    horizon = request.args.get('horizon', 24, type=int)
    return jsonify({'status': 'ok', 'data': forecaster.forecast(metric=metric, horizon=horizon)})


@app.route('/api/predictions/eta', methods=['POST'])
def predict_eta():
    data = request.json or {}
    return jsonify({'status': 'ok', 'data': forecaster.predict_eta(data)})


@app.route('/api/predictions/demand')
def predict_demand():
    days = request.args.get('days', 30, type=int)
    return jsonify({'status': 'ok', 'data': forecaster.demand_forecast(days=days)})


# ══════════════════════════════════════════════════════
# API — Routing
# ══════════════════════════════════════════════════════

@app.route('/api/optimize-route', methods=['POST'])
def optimize_route():
    d = request.json or {}
    result = optimizer.find_optimal_route(
        origin=d.get('origin', 'Shanghai'),
        dest=d.get('destination', 'Rotterdam'),
        mode=d.get('mode', 'sea'),
        priorities=d.get('priorities', {'time':0.4,'cost':0.3,'risk':0.3})
    )
    return jsonify({'status': 'ok', 'data': result})


@app.route('/api/route-alternatives', methods=['POST'])
def route_alternatives():
    d = request.json or {}
    return jsonify({'status': 'ok',
                    'data': optimizer.get_alternatives(d.get('shipment_id', 'DEMO'))})


@app.route('/api/network-graph')
def network_graph():
    return jsonify({'status': 'ok', 'data': optimizer.get_network_graph()})


# ══════════════════════════════════════════════════════
# API — ETL
# ══════════════════════════════════════════════════════

@app.route('/api/etl/status')
def etl_status():
    return jsonify({'status': 'ok', 'data': etl_runner.get_pipeline_status()})


@app.route('/api/etl/run', methods=['POST'])
def run_etl():
    d = request.json or {}
    source = d.get('source', 'all')
    csv_dir = os.path.join(os.path.dirname(__file__), '../data/csv')
    result = etl_runner.run_full_pipeline(csv_dir) if source == 'all' else {
        'status': 'partial', 'message': f'Single source run: {source}'
    }
    return jsonify({'status': 'ok', 'data': result})


@app.route('/api/db/status')
def db_status():
    available = DatabaseManager.is_available()
    stats = {}
    if available:
        stats = {
            'shipments_master': DatabaseManager.get_table_stats('shipments_master'),
            'route_events':     DatabaseManager.get_table_stats('route_events'),
        }
    return jsonify({'status': 'ok', 'db_connected': available,
                    'host': f"{os.environ.get('DB_HOST')}:{os.environ.get('DB_PORT')}",
                    'database': os.environ.get('DB_NAME'), 'table_stats': stats})


@app.route('/api/db/query', methods=['POST'])
def run_query():
    """Safe read-only query endpoint for dashboard analytics"""
    d = request.json or {}
    sql = d.get('sql', '')
    # Safety: only allow SELECT
    if not sql.strip().upper().startswith('SELECT'):
        return jsonify({'status': 'error', 'message': 'Only SELECT queries allowed'}), 400
    try:
        rows = DatabaseManager.execute_query(sql)
        return jsonify({'status': 'ok', 'data': rows[:1000], 'count': len(rows)})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ══════════════════════════════════════════════════════
# API — File Upload
# ══════════════════════════════════════════════════════

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'status': 'error', 'message': 'No file in request'}), 400
    f = request.files['file']
    if not f.filename:
        return jsonify({'status': 'error', 'message': 'Empty filename'}), 400

    ext = f.filename.rsplit('.', 1)[-1] if '.' in f.filename else ''
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    fpath = os.path.join(app.config['UPLOAD_FOLDER'], f.filename)
    f.save(fpath)

    load_to_db = request.form.get('load_to_db', 'false').lower() == 'true'

    try:
        result = processor.process_uploaded_file(fpath, ext)
        if load_to_db and is_db_available() and ext == 'csv':
            # Attempt to load into shipments_master if columns match
            table_result = loader.load_csv_to_table(fpath, 'shipments_master')
            result['db_load'] = table_result
        return jsonify({'status': 'ok', 'data': result,
                        'filename': f.filename, 'format': ext})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ══════════════════════════════════════════════════════
# WEBSOCKET — Real DB streaming
# ══════════════════════════════════════════════════════

@socketio.on('connect')
def on_connect():
    emit('connected', {
        'message': 'Supply Chain AI connected',
        'db_connected': DatabaseManager.is_available(),
        'stream_config': stream_reader.get_status(),
        'timestamp': datetime.now().isoformat()
    })


@socketio.on('start_stream')
def on_start_stream():
    stream_reader.start()
    emit('stream_started', stream_reader.get_status())
    logger.info("Live DB stream started via WebSocket")


@socketio.on('stop_stream')
def on_stop_stream():
    stream_reader.stop()
    emit('stream_stopped', {'status': 'stopped'})


@socketio.on('get_stream_status')
def on_stream_status():
    emit('stream_status_response', stream_reader.get_status())


@socketio.on('disconnect')
def on_disconnect():
    logger.info("Client disconnected")


# ══════════════════════════════════════════════════════
# STATIC
# ══════════════════════════════════════════════════════

@app.route('/static/<path:fn>')
def serve_static(fn):
    return send_from_directory('../frontend/static', fn)


if __name__ == '__main__':
    os.makedirs(os.environ.get('UPLOAD_FOLDER', './data/uploads'), exist_ok=True)
    logger.info("=" * 55)
    logger.info("  Supply Chain AI — Control Tower")
    logger.info(f"  PostgreSQL: {'✓ Connected' if is_db_available() else '✗ Not connected (run setup_db.py)'}")
    logger.info("  http://localhost:5000")
    logger.info("=" * 55)
    socketio.run(app, host=os.environ.get('HOST', '0.0.0.0'), port=int(os.environ.get('PORT', 5000)),
                 debug=os.environ.get('FLASK_DEBUG','true').lower()=='true',
                 allow_unsafe_werkzeug=True)
