"""
Disruption Predictor — reads real records from PostgreSQL
"""
import numpy as np
import logging
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)


class DisruptionPredictor:

    DISRUPTION_TYPES = [
        'weather_event','port_congestion','customs_delay',
        'mechanical_failure','route_closure','demand_surge','none'
    ]

    def __init__(self):
        self.rf  = None
        self.gb  = None
        self.scaler = StandardScaler()
        self.is_trained = False
        self._train()

    def _train(self):
        np.random.seed(42)
        n = 8000
        X = np.column_stack([
            np.random.uniform(0, 1, n),   # weather_severity
            np.random.uniform(0, 1, n),   # port_congestion
            np.random.uniform(0, 1, n),   # traffic_density
            np.random.uniform(0, 1, n),   # route_complexity
            np.random.uniform(0.5,1, n),  # carrier_reliability
            np.random.uniform(0, 1, n),   # customs_risk
            np.random.uniform(0, 0.4, n), # historical_delay_rate
            np.random.uniform(100,20000,n),# distance_km
            np.random.randint(0, 6, n),   # num_stops
            np.random.randint(1, 5, n),   # priority_level
        ])
        risk = (X[:,0]*0.25 + X[:,1]*0.20 + X[:,2]*0.10 +
                X[:,3]*0.15 + (1-X[:,4])*0.15 + X[:,5]*0.10 +
                X[:,6]*0.05 + np.random.normal(0,0.04,n))
        y = (risk > 0.42).astype(int)

        Xs = self.scaler.fit_transform(X)
        # Use a single worker so local Windows runs do not fail on restricted IPC handles.
        self.rf = RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42, n_jobs=1)
        self.gb = GradientBoostingClassifier(n_estimators=80, learning_rate=0.1, max_depth=5, random_state=42)
        self.rf.fit(Xs, y)
        self.gb.fit(Xs, y)
        self.is_trained = True

    def predict_from_db_rows(self, rows: list) -> list:
        """Score real DB rows. Works with both shipments_master and route_events schema."""
        results = []
        for row in rows:
            features = self._extract_features(row)
            pred = self._score(features)
            results.append({**row, 'ml_prediction': pred})
        return results

    def _extract_features(self, row: dict) -> np.ndarray:
        """Extract feature vector from a DB row (handles both table schemas)"""
        # shipments_master fields
        weather  = float(row.get('weather_severity', 0) or 0)
        cong     = float(row.get('port_congestion', 0) or 0)
        risk     = float(row.get('risk_score', 0.3) or 0.3)
        delay    = float(row.get('delay_hours', 0) or 0)
        distance = float(row.get('distance_km', 5000) or 5000)
        stops    = int(row.get('num_stops', 2) or 2)
        priority = int(row.get('priority_level', 2) or 2)

        # route_events fields
        delta    = float(row.get('risk_score_delta', 0) or 0)
        shock    = float(row.get('shock_g', 0) or 0)
        wait     = float(row.get('port_wait_hours', 0) or 0)

        return np.array([[
            weather,
            cong + abs(delta),            # augmented congestion
            min(1.0, shock / 5.0),        # normalised shock
            risk,
            max(0, 1 - delay / 72),       # carrier reliability proxy
            min(1.0, wait / 24),          # customs risk proxy
            min(1.0, delay / 48),         # historical delay proxy
            distance,
            stops,
            priority,
        ]])

    def _score(self, X: np.ndarray) -> dict:
        Xs = self.scaler.transform(X)
        rf_p = float(self.rf.predict_proba(Xs)[0][1])
        gb_p = float(self.gb.predict_proba(Xs)[0][1])
        p    = 0.5 * rf_p + 0.5 * gb_p
        sev  = 'low' if p<0.3 else 'medium' if p<0.55 else 'high' if p<0.75 else 'critical'
        return {
            'risk_score':   round(p, 4),
            'severity':     sev,
            'rf_prob':      round(rf_p, 4),
            'gb_prob':      round(gb_p, 4),
            'confidence':   round(1 - abs(rf_p - gb_p), 3),
            'flagged':      p > 0.50,
        }
