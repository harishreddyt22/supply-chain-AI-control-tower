# 🚢 Supply Chain AI — PostgreSQL Control Tower

Real-time disruption detection with **actual PostgreSQL data** — 5 real DB records streamed every 10 seconds via WebSocket. Ships with two CSV files containing **5 million records each** (50 lakh rows).

---

## 📦 CSV Files Included

| File | Rows | Size | Description |
|------|------|------|-------------|
| `data/csv/shipments_master.csv` | **5,000,000** | ~826 MB | Core shipment transactions — 25 columns |
| `data/csv/route_events.csv`     | **5,000,000** | ~808 MB | IoT/GPS route events — 21 columns |

### shipments_master.csv columns
`shipment_id, carrier, origin_port, destination_port, transport_mode, status, goods_category, currency, created_date, eta_date, transit_hours, distance_km, weight_kg, volume_m3, value_usd, freight_cost_usd, num_containers, num_stops, delay_hours, risk_score, weather_severity, port_congestion, temperature_c, priority_level, insurance_required`

### route_events.csv columns
`event_id, shipment_id, event_type, event_timestamp, location_name, latitude, longitude, speed_knots, heading_deg, temperature_c, humidity_pct, shock_g, delay_added_hours, risk_score_delta, port_wait_hours, fuel_consumed_lt, co2_kg, sensor_type, signal_quality, anomaly_flag, alert_sent`

---

## 🗂 Project Structure

```
supply_chain_v2/
├── .env                          ← DB credentials (edit this first)
├── .env.example                  ← Template
├── setup_db.py                   ← ONE-TIME: create tables + load CSVs
├── run_local.sh                  ← Start the app locally
├── requirements.txt
├── docker-compose.yml            ← PostgreSQL + pgAdmin + App
│
├── backend/
│   ├── app.py                    ← Flask + Socket.IO server
│   ├── db/database.py            ← PostgreSQL connection pool
│   ├── ml/
│   │   ├── disruption_model.py   ← RandomForest + GBM ensemble
│   │   └── time_series.py        ← ETA & demand forecasting
│   ├── routing/optimizer.py      ← Dijkstra / A* / RL routing
│   ├── ingestion/
│   │   ├── data_processor.py     ← DB queries + file parsing
│   │   └── live_stream.py        ← 5 records / 10s from PostgreSQL
│   └── etl/loader.py             ← CSV → PostgreSQL bulk loader
│
├── frontend/
│   ├── templates/
│   │   ├── dashboard.html        ← Control tower + live stream panel
│   │   ├── routing.html          ← Route optimizer
│   │   ├── analytics.html        ← ML predictions + live DB query
│   │   └── ingestion.html        ← Upload files + ETL status
│   └── static/
│       ├── css/main.css          ← Design system
│       └── js/dashboard.js       ← WebSocket + charts + maps
│
├── data/
│   ├── csv/
│   │   ├── shipments_master.csv  ← 5,000,000 rows
│   │   └── route_events.csv      ← 5,000,000 rows
│   └── uploads/                  ← User-uploaded files
│
└── deploy/
    ├── Dockerfile
    └── cloud_deploy.yml          ← Kubernetes (EKS/GKE/AKS)
```

---

## 🚀 Quick Start

### Step 1 — Configure Database

Edit `.env`:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=supply_chain_db
DB_USER=postgres
DB_PASSWORD=your_password_here
```

### Step 2 — Start PostgreSQL + pgAdmin (Docker)

```bash
docker-compose up postgres pgadmin -d
```

pgAdmin UI → **http://localhost:5050**
- Email: `admin@supplychain.ai`
- Password: `admin123`
- Connect to server: host=`postgres`, port=`5432`

### Step 3 — Load CSV Files into PostgreSQL

```bash
# Creates tables + loads both 50L CSV files
python setup_db.py
```

This runs COPY command (fast). Loading 5M rows takes ~3–8 minutes per file depending on hardware.

### Step 4 — Start the App

```bash
# Option A: shell script
chmod +x run_local.sh && ./run_local.sh

# Option B: manual
pip install -r requirements.txt
python backend/app.py

python -3.10 backend/app.py

# Option C: full Docker stack
docker-compose up --build
```

Open **http://localhost:5000/dashboard**

### Step 5 — Start Live Stream

On the dashboard, click **▶ Start Live Stream**.
The system reads **5 real rows from `route_events`** every **10 seconds** and streams them to the browser via WebSocket.

---

## 📊 Dashboard Pages

| Page | URL | What's Real from DB |
|------|-----|---------------------|
| **Control Tower** | `/dashboard` | Heatmap from `route_events`, KPIs from `shipments_master`, live stream |
| **Route Optimizer** | `/routing` | Congestion scores from `route_events` per location |
| **Analytics** | `/analytics` | Demand aggregated from DB, live SQL query panel |
| **Data Ingestion** | `/ingestion` | Table row counts, ETL status, CSV upload → pgAdmin |

---

## ⚡ Live Stream Architecture

```
PostgreSQL route_events (5M rows)
        ↓
LiveStreamReader (backend/ingestion/live_stream.py)
  - Reads BATCH_SIZE=5 rows every INTERVAL=10 seconds
  - Cursor-based pagination (offset advances each tick)
  - Wraps around to start when end of table reached
        ↓
Flask-SocketIO → WebSocket → Browser
  - Event: live_stream_batch  → updates stream log panel
  - Event: stream_health      → updates offset / total stats
  - Event: stream_status      → shows DB errors if any
```

Configure in `.env`:
```env
STREAM_BATCH_SIZE=5          # records per batch
STREAM_INTERVAL_SECONDS=10   # seconds between batches
STREAM_TABLE=route_events    # which table to read
```

---

## ☁️ Cloud Deployment (Kubernetes)

```bash
# 1. Build and push Docker image
docker build -t your-registry/supply-chain-ai:latest -f deploy/Dockerfile .
docker push your-registry/supply-chain-ai:latest

# 2. Edit deploy/cloud_deploy.yml
#    Replace: your-registry/supply-chain-ai:latest
#    Replace: supply-chain-ai.yourdomain.com
#    Update SECRET values in the Secret object

# 3. Deploy
kubectl apply -f deploy/cloud_deploy.yml

# 4. Check
kubectl get pods -n supply-chain-ai
kubectl get ingress -n supply-chain-ai
```

---

## 🔌 API Reference

| Endpoint | Method | DB Backed |
|----------|--------|-----------|
| `/api/shipments` | GET | ✓ shipments_master |
| `/api/kpis` | GET | ✓ shipments_master |
| `/api/risk-heatmap` | GET | ✓ route_events |
| `/api/bottlenecks` | GET | ✓ route_events |
| `/api/disruptions` | GET | ✓ shipments_master + ML |
| `/api/forecast` | GET | ML model |
| `/api/predictions/eta` | POST | ML model |
| `/api/predictions/demand` | GET | ✓ shipments_master + ML |
| `/api/optimize-route` | POST | ✓ route_events congestion |
| `/api/network-graph` | GET | ✓ route_events |
| `/api/etl/status` | GET | ✓ table stats |
| `/api/etl/run` | POST | ✓ CSV → DB |
| `/api/db/status` | GET | ✓ connection + stats |
| `/api/db/query` | POST | ✓ live SELECT |
| `/api/upload` | POST | file parse + optional DB load |

---

## 🛠 Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Python 3.11, Flask, Flask-SocketIO |
| Database | PostgreSQL 16, psycopg2 |
| DB UI | pgAdmin 4 |
| ML | scikit-learn (RandomForest, GBM, Ridge) |
| Routing | Dijkstra, A*, RL Q-policy (pure Python) |
| Frontend | HTML5, CSS3, Vanilla JS |
| Maps | Leaflet.js (OpenStreetMap) |
| Charts | Chart.js 4 |
| Real-time | Socket.IO WebSockets |
| Container | Docker, Docker Compose |
| Cloud | Kubernetes (EKS / GKE / AKS) |
