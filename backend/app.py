import os
import math
import json
import subprocess
import sys
import threading
import time
import warnings
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory
from flask.json.provider import DefaultJSONProvider
from flask_cors import CORS
import pandas as pd
import requests as http_requests


class SafeJSONEncoder(json.JSONEncoder):
    """Fast JSON encoder that converts NaN/Inf to null without recursive pre-walk."""
    def default(self, o):
        return super().default(o)

    def iterencode(self, o, _one_shot=False):
        # Use the parent iterencode but with a custom float handler
        return super().iterencode(o, _one_shot=_one_shot)


class SafeJSONProvider(DefaultJSONProvider):
    def dumps(self, obj, **kwargs):
        kwargs.setdefault("default", self.default)
        kwargs.setdefault("ensure_ascii", self.ensure_ascii)
        kwargs.setdefault("sort_keys", self.sort_keys)
        kwargs.setdefault("allow_nan", False)
        try:
            return json.dumps(obj, **kwargs)
        except ValueError:
            # Fallback: only triggered if NaN/Inf sneak through unsanitized
            return json.dumps(self._sanitize(obj), **kwargs)

    @staticmethod
    def _sanitize(o):
        if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
            return None
        if isinstance(o, dict):
            return {k: SafeJSONProvider._sanitize(v) for k, v in o.items()}
        if isinstance(o, list):
            return [SafeJSONProvider._sanitize(v) for v in o]
        return o


STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "frontend", "build")

app = Flask(__name__, static_folder=STATIC_DIR, static_url_path="")
app.json_provider_class = SafeJSONProvider
app.json = SafeJSONProvider(app)
CORS(app)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
BETA_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_beta")

# In-memory CSV cache to avoid re-reading large files on every request
_csv_cache = {}
_csv_ts_cache = {}
_beta_csv_cache = {}
_beta_csv_ts_cache = {}
_beta_csv_mtime = {}  # track file modification times for cache invalidation
_beta_alert_tables_cache = {}

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "")  # e.g. "username/repo"
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "")


def keep_alive():
    """Ping self every 10 minutes to prevent Render free-tier spin-down."""
    while True:
        time.sleep(600)
        if RENDER_EXTERNAL_URL:
            try:
                http_requests.get(f"{RENDER_EXTERNAL_URL}/api/health", timeout=10)
            except Exception:
                pass


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

USERS = {
    "Akash": "a1234",
    "Tejas": "t1234",
    "Naina": "n1234",
    "Gaurav": "g1234",
    "Sagar": "s1234",
}

TIMESTAMP_CANDIDATES = [
    "timestamp_utc",
    "Unnamed: 0",
    "timestamp",
    "time",
    "datetime",
    "date",
    "ts",
    "START_T",
    "index",
]


def safe_to_datetime(values, errors="coerce", utc=None):
    """
    Parse datetimes without noisy per-element inference warnings.
    Prefers pandas 'mixed' parsing when available, then falls back.
    """
    kwargs = {"errors": errors}
    if utc is not None:
        kwargs["utc"] = utc
    try:
        return pd.to_datetime(values, format="mixed", **kwargs)
    except (TypeError, ValueError):
        # Older pandas or incompatible args: silence inference warning on fallback.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Could not infer format, so each element will be parsed individually.*",
                category=UserWarning,
            )
            return pd.to_datetime(values, **kwargs)


def find_timestamp_col(df):
    for candidate in TIMESTAMP_CANDIDATES:
        if candidate in df.columns:
            return candidate
    # Check index name before fuzzy column keyword search
    if df.index.name and df.index.name.lower().strip() in ["timestamp_utc", "timestamp", "time", "datetime", "ts"]:
        return "__index__"
    for col in df.columns:
        col_lower = col.lower().strip()
        if any(kw in col_lower for kw in ["time", "date", "ts", "timestamp"]):
            return col
    if not isinstance(df.index, pd.RangeIndex):
        try:
            test = safe_to_datetime(df.index[:5], errors="coerce")
            if test.notna().sum() >= 3:
                return "__index__"
        except Exception:
            pass
    for col in df.columns:
        try:
            sample = df[col].dropna().head(10)
            if len(sample) == 0:
                continue
            parsed = safe_to_datetime(sample, errors="coerce")
            if parsed.notna().sum() >= max(1, len(sample) * 0.7):
                return col
        except Exception:
            continue
    return None


def load_csv(filename):
    if filename in _csv_cache:
        return _csv_cache[filename]
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        _csv_cache[filename] = df
        return df
    except Exception as e:
        print(f"Error loading {filename}: {e}")
        return pd.DataFrame()


def load_csv_with_ts(filename):
    if filename in _csv_ts_cache:
        return _csv_ts_cache[filename]
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return pd.DataFrame(), None
    try:
        df = pd.read_csv(path)
        ts_col = find_timestamp_col(df)

        if ts_col == "__index__":
            df = pd.read_csv(path, index_col=0)
            df.index.name = df.index.name or "timestamp_utc"
            idx_name = df.index.name
            df = df.reset_index()
            ts_col = idx_name if idx_name in df.columns else df.columns[0]

        if ts_col and ts_col in df.columns:
            try:
                df[ts_col] = safe_to_datetime(df[ts_col], errors="coerce", utc=True)
            except Exception:
                try:
                    df[ts_col] = safe_to_datetime(df[ts_col], errors="coerce")
                except Exception:
                    pass

        if ts_col is None:
            try:
                df_retry = pd.read_csv(path, index_col=0)
                if not isinstance(df_retry.index, pd.RangeIndex):
                    test_parsed = safe_to_datetime(df_retry.index[:5], errors="coerce")
                    if test_parsed.notna().sum() >= 3:
                        df_retry.index.name = df_retry.index.name or "timestamp_utc"
                        idx_name = df_retry.index.name
                        df_retry = df_retry.reset_index()
                        ts_col = idx_name
                        try:
                            df_retry[ts_col] = safe_to_datetime(df_retry[ts_col], errors="coerce", utc=True)
                        except Exception:
                            df_retry[ts_col] = safe_to_datetime(df_retry[ts_col], errors="coerce")
                        df = df_retry
            except Exception:
                pass

        _csv_ts_cache[filename] = (df, ts_col)
        return df, ts_col
    except Exception as e:
        print(f"Error loading {filename}: {e}")
        return pd.DataFrame(), None


def sanitize_val(x):
    if x is None:
        return None
    if isinstance(x, float):
        if math.isnan(x) or math.isinf(x):
            return None
    return x


def sanitize_df(df):
    """Vectorized NaN/Inf sanitization -- replaces row-by-row apply."""
    import numpy as np
    df = df.copy()
    # Replace inf/-inf with NaN, then NaN with None via where
    float_cols = df.select_dtypes(include=["float64", "float32"]).columns
    if len(float_cols):
        df[float_cols] = df[float_cols].replace([np.inf, -np.inf], np.nan)
    # Handle datetime columns
    dt_cols = df.select_dtypes(include=["datetime64", "datetimetz"]).columns
    for col in dt_cols:
        df[col] = df[col].astype(str).replace("NaT", None)
    # Convert all remaining NaN to None (needed for JSON serialization)
    df = df.where(pd.notnull(df), None)
    return df


@app.route("/api/login", methods=["POST"])
def login():
    data = request.get_json()
    username = data.get("username", "")
    password = data.get("password", "")
    if username in USERS and USERS[username] == password:
        return jsonify({"success": True, "username": username})
    return jsonify({"success": False, "message": "Invalid credentials"}), 401


@app.route("/api/alerts", methods=["GET"])
def get_alerts():
    df = load_csv("alerts.csv")
    if df.empty:
        return jsonify({"alerts": [], "summary": {}})

    # Exclude NORMAL -- not a valid alert classification
    if "class" in df.columns:
        df = df[df["class"] != "NORMAL"]

    df = sanitize_df(df)

    high_count = len(df[df["severity"] == "HIGH"]) if "severity" in df.columns else 0
    medium_count = len(df[df["severity"] == "MEDIUM"]) if "severity" in df.columns else 0

    class_dist = {}
    if "class" in df.columns:
        class_dist = df["class"].value_counts().to_dict()

    threshold_dist = {}
    if "threshold" in df.columns:
        threshold_dist = df["threshold"].value_counts().to_dict()

    summary = {
        "total_alerts": len(df),
        "high_count": int(high_count),
        "medium_count": int(medium_count),
        "class_distribution": class_dist,
        "threshold_distribution": threshold_dist,
    }

    alerts = df.to_dict(orient="records")
    return jsonify({"alerts": alerts, "summary": summary})


@app.route("/api/alerts_sensor_level", methods=["GET"])
def get_alerts_sensor_level():
    df = load_csv("alerts_sensor_level.csv")
    if df.empty:
        return jsonify({"sensor_alerts": []})

    df = sanitize_df(df)

    start_ts = request.args.get("start_ts")
    end_ts = request.args.get("end_ts")
    alert_class = request.args.get("class")

    if start_ts and "start_ts" in df.columns:
        df = df[df["start_ts"] == start_ts]
    if end_ts and "end_ts" in df.columns:
        df = df[df["end_ts"] == end_ts]
    if alert_class and "class" in df.columns:
        df = df[df["class"] == alert_class]

    sensor_alerts = df.to_dict(orient="records")
    return jsonify({"sensor_alerts": sensor_alerts})


@app.route("/api/scores", methods=["GET"])
def get_scores():
    df, ts_col = load_csv_with_ts("scores.csv")
    if df.empty:
        return jsonify({"scores": [], "stats": {}, "timestamp_col": None})

    df = sanitize_df(df)

    limit = request.args.get("limit", default=None, type=int)
    offset = request.args.get("offset", default=0, type=int)
    mode_filter = request.args.get("mode")

    if mode_filter and "mode" in df.columns:
        df = df[df["mode"] == mode_filter]

    total_rows = len(df)

    # Compute counts on full dataset before pagination
    normal_count = 0
    anomaly_count = 0
    if "class" in df.columns:
        normal_count = int((df["class"] == "NORMAL").sum())
        anomaly_count = int((df["class"] != "NORMAL").sum())

    running_count = 0
    downtime_count = 0
    if "mode" in df.columns:
        running_count = int((df["mode"] == "RUNNING").sum())
        downtime_count = int((df["mode"] == "DOWNTIME").sum())

    if limit:
        df = df.iloc[offset: offset + limit]

    stats = {}
    score_cols = ["risk_score", "physics_score", "subsystem_score", "sqs_mean"]
    # Add dynamic score_SYS_* columns
    for col in df.columns:
        if col.startswith("score_SYS_") or col.startswith("risk_SYS_"):
            score_cols.append(col)
    # Legacy
    for col in ["mech_score", "elec_score", "therm_score"]:
        if col in df.columns and col not in score_cols:
            score_cols.append(col)
    for col in score_cols:
        if col in df.columns:
            series = pd.to_numeric(df[col], errors="coerce")
            stats[col] = {
                "mean": round(float(series.mean()), 4) if not series.isna().all() else None,
                "max": round(float(series.max()), 4) if not series.isna().all() else None,
                "min": round(float(series.min()), 4) if not series.isna().all() else None,
                "std": round(float(series.std()), 4) if not series.isna().all() else None,
            }

    stats["normal_count"] = normal_count
    stats["anomaly_count"] = anomaly_count
    stats["running_count"] = running_count
    stats["downtime_count"] = downtime_count
    stats["total_rows"] = total_rows

    scores = df.to_dict(orient="records")
    return jsonify({"scores": scores, "stats": stats, "timestamp_col": ts_col})


@app.route("/api/scores/timeseries", methods=["GET"])
def get_scores_timeseries():
    df, ts_col = load_csv_with_ts("scores.csv")
    if df.empty:
        return jsonify({"timeseries": [], "timestamp_col": None})

    if ts_col is None:
        print("[WARN] scores.csv: Could not identify timestamp column.")
        print(f"[WARN] Available columns: {list(df.columns)}")
        print(f"[WARN] First row: {df.iloc[0].to_dict() if len(df) > 0 else 'empty'}")
        return jsonify({"timeseries": [], "timestamp_col": None})

    downsample = request.args.get("downsample", default=10, type=int)
    df_sampled = df.iloc[::max(1, downsample)]

    cols_to_include = [ts_col]
    # Include known fixed columns
    for col in ["risk_score", "physics_score", "subsystem_score", "sqs_mean", "mode", "class",
                 "risk_INSTRUMENT"]:
        if col in df_sampled.columns and col != ts_col:
            cols_to_include.append(col)
    # Include dynamic score_SYS_* and risk_SYS_* columns
    for col in df_sampled.columns:
        if (col.startswith("score_SYS_") or col.startswith("risk_SYS_") or
            col.startswith("score_") or col.startswith("risk_")) and col not in cols_to_include:
            cols_to_include.append(col)
    # Legacy support for old column names
    for col in ["mech_score", "elec_score", "therm_score",
                 "risk_mech", "risk_elec", "risk_therm", "risk_instrument"]:
        if col in df_sampled.columns and col not in cols_to_include:
            cols_to_include.append(col)

    existing_cols = [c for c in cols_to_include if c in df_sampled.columns]
    df_out = sanitize_df(df_sampled[existing_cols].copy())

    if ts_col in df_out.columns:
        df_out[ts_col] = df_out[ts_col].astype(str)

    timeseries = df_out.to_dict(orient="records")
    return jsonify({"timeseries": timeseries, "timestamp_col": ts_col})


@app.route("/api/risk_decomposition", methods=["GET"])
def get_risk_decomposition():
    df = load_csv("risk_sensor_decomposition.csv")
    if df.empty:
        return jsonify({"decomposition": [], "summary": {}})

    df = sanitize_df(df)

    start_ts = request.args.get("start_ts")
    end_ts = request.args.get("end_ts")
    sensor_id = request.args.get("sensor_id")
    subsystem = request.args.get("subsystem")

    if start_ts and "timestamp_utc" in df.columns:
        df = df[df["timestamp_utc"] >= start_ts]
    if end_ts and "timestamp_utc" in df.columns:
        df = df[df["timestamp_utc"] <= end_ts]
    if sensor_id and "sensor_id" in df.columns:
        df = df[df["sensor_id"] == sensor_id]
    if subsystem and "subsystem" in df.columns:
        df = df[df["subsystem"] == subsystem]

    summary = {}
    if "sensor_id" in df.columns and "risk_score_component" in df.columns:
        sensor_risk = df.groupby("sensor_id")["risk_score_component"].sum()
        summary["top_sensors"] = sensor_risk.nlargest(10).to_dict()

    if "subsystem" in df.columns and "risk_score_component" in df.columns:
        sub_risk = df.groupby("subsystem")["risk_score_component"].sum()
        summary["subsystem_totals"] = sub_risk.to_dict()

    limit = request.args.get("limit", default=5000, type=int)
    decomposition = df.head(limit).to_dict(orient="records")
    return jsonify({"decomposition": decomposition, "summary": summary})


@app.route("/api/risk_decomposition/episode", methods=["GET"])
def get_risk_decomposition_for_episode():
    df = load_csv("risk_sensor_decomposition.csv")
    if df.empty:
        return jsonify({"decomposition": [], "flow_data": {}})

    start_ts = request.args.get("start_ts")
    end_ts = request.args.get("end_ts")

    if not start_ts or not end_ts:
        return jsonify({"decomposition": [], "flow_data": {}})

    if "timestamp_utc" in df.columns:
        try:
            df["timestamp_utc"] = safe_to_datetime(df["timestamp_utc"], errors="coerce", utc=True)
            start_dt = safe_to_datetime(start_ts, utc=True)
            end_dt = safe_to_datetime(end_ts, utc=True)
            df = df[(df["timestamp_utc"] >= start_dt) & (df["timestamp_utc"] <= end_dt)]
        except Exception:
            df["timestamp_utc"] = df["timestamp_utc"].astype(str)
            df = df[(df["timestamp_utc"] >= start_ts) & (df["timestamp_utc"] <= end_ts)]

    df = sanitize_df(df)

    flow_data = {"subsystems": {}, "sensors": {}}

    if "subsystem" in df.columns and "risk_score_component" in df.columns:
        risk_col = pd.to_numeric(df["risk_score_component"], errors="coerce")
        sub_agg = df.assign(risk_numeric=risk_col).groupby("subsystem").agg(
            total_risk=("risk_numeric", "sum"),
            mean_risk=("risk_numeric", "mean"),
            max_risk=("risk_numeric", "max"),
            sensor_count=("sensor_id", "nunique"),
        ).to_dict(orient="index")
        flow_data["subsystems"] = sub_agg

    if "sensor_id" in df.columns and "risk_score_component" in df.columns:
        numeric_cols_map = {
            "risk_score_component": "risk_numeric",
            "confidence_factor": "conf_numeric",
            "base_component": "base_numeric",
            "uplift_component": "uplift_numeric",
        }
        df_calc = df.copy()
        for orig, new_name in numeric_cols_map.items():
            if orig in df_calc.columns:
                df_calc[new_name] = pd.to_numeric(df_calc[orig], errors="coerce")

        agg_dict = {}
        if "risk_numeric" in df_calc.columns:
            agg_dict["total_risk"] = ("risk_numeric", "sum")
            agg_dict["mean_risk"] = ("risk_numeric", "mean")
            agg_dict["max_risk"] = ("risk_numeric", "max")
        if "conf_numeric" in df_calc.columns:
            agg_dict["mean_confidence"] = ("conf_numeric", "mean")
        if "base_numeric" in df_calc.columns:
            agg_dict["mean_base"] = ("base_numeric", "mean")
        if "uplift_numeric" in df_calc.columns:
            agg_dict["mean_uplift"] = ("uplift_numeric", "mean")

        if agg_dict:
            sensor_agg = df_calc.groupby(["subsystem", "sensor_id"]).agg(**agg_dict).reset_index()
            sensor_agg = sanitize_df(sensor_agg)
            flow_data["sensors"] = sensor_agg.to_dict(orient="records")

    decomposition = df.to_dict(orient="records")
    return jsonify({"decomposition": decomposition, "flow_data": flow_data})


@app.route("/api/systems", methods=["GET"])
def get_systems():
    """Return discovered systems with their sensors from dynamic_catalog.csv."""
    catalog_df = load_csv("dynamic_catalog.csv")
    weights_df = load_csv("dynamic_weights.csv")
    summary_df = load_csv("system_summary.csv")

    if catalog_df.empty:
        return jsonify({"systems": [], "isolated": []})

    systems = []
    isolated_sensors = []

    grouped = catalog_df.groupby("system")["sensor"].apply(list).to_dict()

    # Build weight lookup
    fusion_weights = {}
    risk_weights = {}
    if not weights_df.empty:
        for _, row in weights_df.iterrows():
            if row.get("type") == "fusion":
                fusion_weights[row["key"]] = row["weight"]
            elif row.get("type") == "risk":
                risk_weights[row["key"]] = row["weight"]

    # Build summary lookup
    summary_lookup = {}
    if not summary_df.empty and "System_ID" in summary_df.columns:
        for _, row in summary_df.iterrows():
            summary_lookup[row["System_ID"]] = sanitize_df(pd.DataFrame([row])).to_dict(orient="records")[0]

    for sys_label, sensors in sorted(grouped.items()):
        if sys_label == "ISOLATED":
            isolated_sensors = sensors
            continue
        sys_info = {
            "system_id": sys_label,
            "sensors": sensors,
            "sensor_count": len(sensors),
            "fusion_weight": fusion_weights.get(sys_label),
            "risk_weight": risk_weights.get(sys_label),
        }
        if sys_label in summary_lookup:
            sys_info["r2_adj_mean"] = summary_lookup[sys_label].get("R2_Adj_Mean")
            sys_info["quality"] = summary_lookup[sys_label].get("Quality")
        systems.append(sys_info)

    return jsonify({"systems": systems, "isolated": isolated_sensors})


@app.route("/api/systems/<system_id>/sensors", methods=["GET"])
def get_system_sensor_values(system_id):
    """Return raw sensor time series for a system, plus downtime bands and alerts."""
    downsample = request.args.get("downsample", default=5, type=int)

    # Load sensor values from df_chart_data.csv, filtered by catalog mapping
    catalog_df = load_csv("dynamic_catalog.csv")
    chart_df, ts_col = load_csv_with_ts("df_chart_data.csv")
    if not chart_df.empty and not catalog_df.empty:
        sys_sensors = catalog_df[catalog_df["system"] == system_id]["sensor"].tolist()
        keep_cols = [ts_col] + [s for s in sys_sensors if s in chart_df.columns] if ts_col else [s for s in sys_sensors if s in chart_df.columns]
        sensor_df = chart_df[keep_cols]
    else:
        # Fallback to per-system files
        fname = f"sensor_values_{system_id}.csv"
        sensor_df, ts_col = load_csv_with_ts(fname)
    if sensor_df.empty:
        return jsonify({"timeseries": [], "downtime_bands": [], "alert_bands": [], "sensors": []})

    # Downsample
    sensor_df = sensor_df.iloc[::max(1, downsample)]
    sensor_df = sanitize_df(sensor_df)

    if ts_col and ts_col in sensor_df.columns:
        sensor_df[ts_col] = sensor_df[ts_col].astype(str)
    sensor_cols = [c for c in sensor_df.columns if c != ts_col]

    timeseries = sensor_df.to_dict(orient="records")

    # Downtime bands from scores.csv — time-based episode merging
    scores_df, scores_ts = load_csv_with_ts("scores.csv")
    downtime_bands = []
    if not scores_df.empty and "mode" in scores_df.columns and scores_ts:
        dt_mask = scores_df["mode"] == "DOWNTIME"
        if dt_mask.any():
            dt_times = safe_to_datetime(scores_df.loc[dt_mask, scores_ts], errors="coerce")
            dt_times = dt_times.sort_values().reset_index(drop=True)
            merge_gap = pd.Timedelta(minutes=15)
            span_start = dt_times.iloc[0]
            span_end = dt_times.iloc[0]
            for t in dt_times.iloc[1:]:
                if t - span_end <= merge_gap:
                    span_end = t
                else:
                    downtime_bands.append({"start": str(span_start), "end": str(span_end)})
                    span_start = t
                    span_end = t
            downtime_bands.append({"start": str(span_start), "end": str(span_end)})

    # Alert bands filtered to this system
    alerts_df = load_csv("alerts.csv")
    alert_bands = []
    if not alerts_df.empty and "class" in alerts_df.columns:
        sys_alerts = alerts_df[
            (alerts_df["class"] == system_id) |
            (alerts_df["class"] == "PROCESS")
        ]
        for _, row in sys_alerts.iterrows():
            alert_bands.append({
                "start": str(row.get("start_ts", "")),
                "end": str(row.get("end_ts", "")),
                "severity": row.get("severity", "MEDIUM"),
                "class": row.get("class", ""),
            })

    return jsonify({
        "timeseries": timeseries,
        "timestamp_col": ts_col,
        "sensors": sensor_cols,
        "downtime_bands": downtime_bands,
        "alert_bands": alert_bands,
    })


@app.route("/api/sensor_config", methods=["GET"])
def get_sensor_config():
    df = load_csv("sensor_config.csv")
    if df.empty:
        return jsonify({"sensors": []})
    df = sanitize_df(df)
    sensors = df.to_dict(orient="records")
    return jsonify({"sensors": sensors})


@app.route("/api/sensor/<sensor_id>/detail", methods=["GET"])
def get_sensor_detail(sensor_id):
    config_df = load_csv("sensor_config.csv")
    sensor_config = None
    if not config_df.empty:
        col_name = None
        for c in ["sensor", "sensor_id"]:
            if c in config_df.columns:
                col_name = c
                break
        if col_name:
            match = config_df[config_df[col_name] == sensor_id]
            if not match.empty:
                sensor_config = sanitize_df(match).to_dict(orient="records")[0]

    decomp_df = load_csv("risk_sensor_decomposition.csv")
    risk_profile = []
    if not decomp_df.empty and "sensor_id" in decomp_df.columns:
        sensor_decomp = decomp_df[decomp_df["sensor_id"] == sensor_id]
        if not sensor_decomp.empty:
            sensor_decomp = sanitize_df(sensor_decomp)
            risk_profile = sensor_decomp.to_dict(orient="records")

    sensor_alerts_df = load_csv("alerts_sensor_level.csv")
    sensor_alerts = []
    if not sensor_alerts_df.empty and "sensor" in sensor_alerts_df.columns:
        sa = sensor_alerts_df[sensor_alerts_df["sensor"] == sensor_id]
        if not sa.empty:
            sensor_alerts = sanitize_df(sa).to_dict(orient="records")

    return jsonify({
        "sensor_id": sensor_id,
        "config": sensor_config,
        "risk_profile": risk_profile[-500:] if len(risk_profile) > 500 else risk_profile,
        "alerts": sensor_alerts,
    })


@app.route("/api/dashboard/summary", methods=["GET"])
def get_dashboard_summary():
    alerts_df = load_csv("alerts.csv")
    scores_df, ts_col = load_csv_with_ts("scores.csv")
    config_df = load_csv("sensor_config.csv")

    summary = {
        "total_alerts": 0,
        "high_alerts": 0,
        "medium_alerts": 0,
        "total_sensors": 0,
        "class_distribution": {},
        "avg_risk_score": None,
        "max_risk_score": None,
        "current_risk_score": None,
        "avg_sqs": None,
        "running_pct": None,
        "data_range": {"start": None, "end": None},
    }

    if not alerts_df.empty:
        # Exclude NORMAL from alert counts -- NORMAL is never a valid alert classification
        if "class" in alerts_df.columns:
            alerts_df = alerts_df[alerts_df["class"] != "NORMAL"]
        summary["total_alerts"] = len(alerts_df)
        if "severity" in alerts_df.columns:
            summary["high_alerts"] = int((alerts_df["severity"] == "HIGH").sum())
            summary["medium_alerts"] = int((alerts_df["severity"] == "MEDIUM").sum())
        if "class" in alerts_df.columns:
            summary["class_distribution"] = alerts_df["class"].value_counts().to_dict()

    if not scores_df.empty:
        if "risk_score" in scores_df.columns:
            rs = pd.to_numeric(scores_df["risk_score"], errors="coerce")
            summary["avg_risk_score"] = round(float(rs.mean()), 4) if not rs.isna().all() else None
            summary["max_risk_score"] = round(float(rs.max()), 4) if not rs.isna().all() else None
            last_valid = rs.dropna()
            if len(last_valid) > 0:
                summary["current_risk_score"] = round(float(last_valid.iloc[-1]), 4)
        if "sqs_mean" in scores_df.columns:
            sqs = pd.to_numeric(scores_df["sqs_mean"], errors="coerce")
            summary["avg_sqs"] = round(float(sqs.mean()), 4) if not sqs.isna().all() else None
        if "mode" in scores_df.columns:
            total = len(scores_df)
            running = int((scores_df["mode"] == "RUNNING").sum())
            summary["running_pct"] = round(running / total * 100, 1) if total > 0 else None
        if ts_col and ts_col in scores_df.columns:
            ts_series = scores_df[ts_col].dropna()
            if len(ts_series) > 0:
                summary["data_range"]["start"] = str(ts_series.iloc[0])
                summary["data_range"]["end"] = str(ts_series.iloc[-1])

    if not config_df.empty:
        summary["total_sensors"] = len(config_df)

    # Include dynamic system info
    catalog_df = load_csv("dynamic_catalog.csv")
    if not catalog_df.empty:
        grouped = catalog_df.groupby("system")["sensor"].apply(list).to_dict()
        systems_info = []
        for sys_label, sensors in sorted(grouped.items()):
            if sys_label == "ISOLATED":
                continue
            systems_info.append({"system_id": sys_label, "sensors": sensors, "sensor_count": len(sensors)})
        summary["systems"] = systems_info
        summary["total_sensors"] = len(catalog_df)

    return jsonify(summary)


@app.route("/api/normal_periods", methods=["GET"])
def get_normal_periods():
    scores_df, ts_col = load_csv_with_ts("scores.csv")
    if scores_df.empty or "class" not in scores_df.columns:
        return jsonify({"normal_stats": {}, "periods": []})

    normal_mask = scores_df["class"] == "NORMAL"
    total = len(scores_df)
    normal_count = int(normal_mask.sum())

    normal_stats = {
        "normal_count": normal_count,
        "total_count": total,
        "normal_pct": round(normal_count / total * 100, 1) if total > 0 else 0,
    }

    if "risk_score" in scores_df.columns:
        normal_risk = pd.to_numeric(scores_df.loc[normal_mask, "risk_score"], errors="coerce")
        normal_stats["avg_risk_during_normal"] = round(float(normal_risk.mean()), 4) if not normal_risk.isna().all() else None

    if "sqs_mean" in scores_df.columns:
        normal_sqs = pd.to_numeric(scores_df.loc[normal_mask, "sqs_mean"], errors="coerce")
        normal_stats["avg_sqs_during_normal"] = round(float(normal_sqs.mean()), 4) if not normal_sqs.isna().all() else None

    return jsonify({"normal_stats": normal_stats})


BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
PIPELINE_SCRIPT = os.path.join(BACKEND_DIR, "pipeline.py")
PIPELINE_DONE = os.path.join(BACKEND_DIR, "pipeline.done")


@app.route("/api/run_pipeline", methods=["POST"])
def run_pipeline():
    # Remove old done marker
    try:
        os.remove(PIPELINE_DONE)
    except OSError:
        pass
    try:
        log_fh = open(os.path.join(BACKEND_DIR, "pipeline.log"), "w", encoding="utf-8", errors="replace")
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        subprocess.Popen(
            [sys.executable, "-u", PIPELINE_SCRIPT],
            cwd=BACKEND_DIR,
            stdout=log_fh,
            stderr=log_fh,
            env=env,
        )
        return jsonify({"status": "started"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/pipeline_status", methods=["GET"])
def pipeline_status():
    if os.path.exists(PIPELINE_DONE):
        return jsonify({"status": "finished", "returncode": 0})
    return jsonify({"status": "running"})


@app.route("/api/feedback", methods=["POST"])
def submit_feedback():
    data = request.get_json(force=True)
    if not data:
        return jsonify({"error": "No data provided"}), 400

    if not GITHUB_TOKEN or not GITHUB_REPO:
        return jsonify({"error": "GitHub integration not configured"}), 500

    section = data.get("section", "General")
    rating = data.get("rating", "")
    comment = data.get("comment", "")
    context = data.get("context", "")
    user = data.get("user", "anonymous")

    title = f"[Feedback] {section} - Rating {rating}/5"
    body = (
        f"**Section:** {section}\n"
        f"**Rating:** {rating}/5\n"
        f"**User:** {user}\n"
        f"**Context:** {context or 'N/A'}\n\n"
        f"**Comment:**\n{comment or 'No comment provided.'}\n\n"
        f"*Submitted at {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}*"
    )
    labels = ["feedback"]
    if isinstance(rating, int) and rating <= 2:
        labels.append("needs-attention")

    try:
        resp = http_requests.post(
            f"https://api.github.com/repos/{GITHUB_REPO}/issues",
            headers={
                "Authorization": f"token {GITHUB_TOKEN}",
                "Accept": "application/vnd.github.v3+json",
            },
            json={"title": title, "body": body, "labels": labels},
            timeout=15,
        )
        if resp.status_code == 201:
            issue = resp.json()
            return jsonify({"status": "ok", "issue_url": issue.get("html_url")})
        return jsonify({"error": f"GitHub API error: {resp.status_code}", "detail": resp.text}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =============================================================================
# BETA API ENDPOINTS — reads exclusively from BETA_DATA_DIR
# =============================================================================

def _beta_resolve_path(filename):
    """Return the actual file path (parquet preferred, CSV fallback)."""
    if filename.endswith(".csv"):
        pq = os.path.join(BETA_DATA_DIR, filename.rsplit(".", 1)[0] + ".parquet")
        if os.path.exists(pq):
            return pq
    csv = os.path.join(BETA_DATA_DIR, filename)
    if os.path.exists(csv):
        return csv
    return None


def _beta_cache_is_stale(filename):
    """Check if the cached file has been modified on disk."""
    path = _beta_resolve_path(filename)
    if path is None:
        return True
    current_mtime = os.path.getmtime(path)
    return _beta_csv_mtime.get(filename) != current_mtime


def _beta_update_mtime(filename):
    path = _beta_resolve_path(filename)
    if path:
        _beta_csv_mtime[filename] = os.path.getmtime(path)


def beta_load_csv(filename):
    if filename in _beta_csv_cache and not _beta_cache_is_stale(filename):
        return _beta_csv_cache[filename]
    # Prefer parquet for faster loads and smaller memory footprint
    parquet_name = filename.rsplit(".", 1)[0] + ".parquet" if filename.endswith(".csv") else None
    parquet_path = os.path.join(BETA_DATA_DIR, parquet_name) if parquet_name else None
    csv_path = os.path.join(BETA_DATA_DIR, filename)
    try:
        if parquet_path and os.path.exists(parquet_path):
            df = pd.read_parquet(parquet_path)
            _beta_csv_cache[filename] = df
            _beta_update_mtime(filename)
            return df
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            _beta_csv_cache[filename] = df
            _beta_update_mtime(filename)
            return df
    except Exception as e:
        print(f"Error loading beta {filename}: {e}")
    return pd.DataFrame()


def beta_load_csv_with_ts(filename):
    if filename in _beta_csv_ts_cache and not _beta_cache_is_stale(filename):
        return _beta_csv_ts_cache[filename]
    # Prefer parquet for faster loads
    parquet_name = filename.rsplit(".", 1)[0] + ".parquet" if filename.endswith(".csv") else None
    parquet_path = os.path.join(BETA_DATA_DIR, parquet_name) if parquet_name else None
    csv_path = os.path.join(BETA_DATA_DIR, filename)
    try:
        if parquet_path and os.path.exists(parquet_path):
            df = pd.read_parquet(parquet_path)
        elif os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
        else:
            return pd.DataFrame(), None
        ts_col = find_timestamp_col(df)
        if ts_col == "__index__":
            # Re-read CSV with index if parquet didn't have it
            if not (parquet_path and os.path.exists(parquet_path)):
                df = pd.read_csv(csv_path, index_col=0)
            df.index.name = df.index.name or "timestamp_utc"
            idx_name = df.index.name
            df = df.reset_index()
            ts_col = idx_name if idx_name in df.columns else df.columns[0]
        if ts_col and ts_col in df.columns:
            try:
                df[ts_col] = safe_to_datetime(df[ts_col], errors="coerce", utc=True)
            except Exception:
                try:
                    df[ts_col] = safe_to_datetime(df[ts_col], errors="coerce")
                except Exception:
                    pass
        if ts_col is None:
            try:
                df_retry = pd.read_csv(csv_path, index_col=0)
                if not isinstance(df_retry.index, pd.RangeIndex):
                    test_parsed = safe_to_datetime(df_retry.index[:5], errors="coerce")
                    if test_parsed.notna().sum() >= 3:
                        df_retry.index.name = df_retry.index.name or "timestamp_utc"
                        idx_name = df_retry.index.name
                        df_retry = df_retry.reset_index()
                        ts_col = idx_name
                        try:
                            df_retry[ts_col] = safe_to_datetime(df_retry[ts_col], errors="coerce", utc=True)
                        except Exception:
                            df_retry[ts_col] = safe_to_datetime(df_retry[ts_col], errors="coerce")
                        df = df_retry
            except Exception:
                pass
        _beta_csv_ts_cache[filename] = (df, ts_col)
        _beta_update_mtime(filename)
        return df, ts_col
    except Exception as e:
        print(f"Error loading beta {filename}: {e}")
    return pd.DataFrame(), None


def _beta_normalize_alarm_level(value):
    if value is None:
        return "UNKNOWN"
    text = str(value).strip().upper()
    if text in {"LOW", "MEDIUM", "HIGH"}:
        return text
    return text or "UNKNOWN"


def _beta_lookup_indexed_row(indexed_df, key):
    if indexed_df is None or indexed_df.empty or key not in indexed_df.index:
        return None
    row = indexed_df.loc[key]
    if isinstance(row, pd.DataFrame):
        return row.iloc[0]
    return row


def _beta_build_timestamp_alert_tables():
    alert_cols = [
        "view_type", "start_ts", "end_ts", "duration_minutes", "minute_count", "severity", "severity_mix",
        "high_count", "medium_count", "low_count", "class",
        "sensor_id", "sensor_max_score", "sensor_mean_score",
        "affected_sensor_count", "affected_sensors", "max_score", "mean_score",
        "risk_score", "peak_risk_score", "mean_risk_score", "adaptive_threshold",
        "threshold", "system_confidence", "reliable_count", "degraded_count",
        "avg_sqs", "sqs_good_count", "sqs_poor_count",
    ]
    sensor_cols = [
        "view_type", "start_ts", "end_ts", "duration_minutes", "minute_count", "severity", "severity_mix",
        "high_count", "medium_count", "low_count", "class",
        "sensor", "sensor_rank", "sensor_peak_score", "sensor_mean_score",
        "sensor_contribution_pct", "sensor_trust", "sensor_normal_minutes", "sensor_anomalous_minutes", "sensor_sqs",
        "alert_max_score", "alert_mean_score",
        "risk_score", "peak_risk_score", "mean_risk_score", "adaptive_threshold",
        "threshold",
    ]

    alarms_df, alarms_ts = beta_load_csv_with_ts("detailed_subsystem_alarms.csv")
    rankings_df, rankings_ts = beta_load_csv_with_ts("detailed_sensor_rankings.csv")
    scores_df, scores_ts = beta_load_csv_with_ts("detailed_system_sensor_scores.csv")
    trust_df, trust_ts = beta_load_csv_with_ts("detailed_sensor_trust.csv")

    if alarms_df.empty or not alarms_ts or alarms_ts not in alarms_df.columns:
        return pd.DataFrame(columns=alert_cols), pd.DataFrame(columns=sensor_cols)

    alarms_df = alarms_df.copy()
    alarms_df[alarms_ts] = alarms_df[alarms_ts].astype(str)

    rankings_indexed = pd.DataFrame()
    if not rankings_df.empty and rankings_ts and rankings_ts in rankings_df.columns:
        rankings_df = rankings_df.copy()
        rankings_df[rankings_ts] = rankings_df[rankings_ts].astype(str)
        rankings_indexed = rankings_df.set_index(rankings_ts, drop=False)

    scores_indexed = pd.DataFrame()
    if not scores_df.empty and scores_ts and scores_ts in scores_df.columns:
        scores_df = scores_df.copy()
        scores_df[scores_ts] = scores_df[scores_ts].astype(str)
        scores_indexed = scores_df.set_index(scores_ts, drop=False)

    trust_indexed = pd.DataFrame()
    if not trust_df.empty and trust_ts and trust_ts in trust_df.columns:
        trust_df = trust_df.copy()
        trust_df[trust_ts] = trust_df[trust_ts].astype(str)
        trust_indexed = trust_df.set_index(trust_ts, drop=False)

    alerts_rows = []
    sensor_rows = []
    alarm_cols = sorted([col for col in alarms_df.columns if col.endswith("__Alarm") and col != "downtime_flag"])

    for alarm_col in alarm_cols:
        sys_label = alarm_col.replace("__Alarm", "")
        level_col = f"{sys_label}__Score_Level_At_Alarm"

        active_rows = alarms_df[alarms_df[alarm_col] == 1].copy()
        if active_rows.empty:
            continue

        for _, alarm_row in active_rows.iterrows():
            ts_str = str(alarm_row[alarms_ts])
            severity = _beta_normalize_alarm_level(alarm_row.get(level_col))

            ranking_row = _beta_lookup_indexed_row(rankings_indexed, ts_str)
            score_row = _beta_lookup_indexed_row(scores_indexed, ts_str)
            trust_row = _beta_lookup_indexed_row(trust_indexed, ts_str)

            # Look up risk score, adaptive threshold, and system confidence from scores CSV
            risk_score_val = None
            adaptive_threshold_val = None
            system_confidence_val = None
            score_col = f"{sys_label}__System_Score"
            threshold_col = f"{sys_label}__Adaptive_Threshold"
            if score_row is not None:
                if score_col in score_row.index:
                    rv = pd.to_numeric(pd.Series([score_row.get(score_col)]), errors="coerce").iloc[0]
                    risk_score_val = None if pd.isna(rv) else float(rv)
                if threshold_col in score_row.index:
                    tv = pd.to_numeric(pd.Series([score_row.get(threshold_col)]), errors="coerce").iloc[0]
                    adaptive_threshold_val = None if pd.isna(tv) else float(tv)
                confidence_col = f"{sys_label}__System_Confidence"
                if confidence_col in score_row.index:
                    cv = pd.to_numeric(pd.Series([score_row.get(confidence_col)]), errors="coerce").iloc[0]
                    system_confidence_val = None if pd.isna(cv) else float(cv)

            # Look up per-sensor trust from trust CSV
            # Trust columns: {sys_label}__{sensor_id} -> "Reliable" or "Degraded"
            sensor_trust_map = {}
            if trust_row is not None:
                prefix = f"{sys_label}__"
                for col in trust_row.index:
                    if col.startswith(prefix) and col != prefix.rstrip("_"):
                        sensor_id_trust = col[len(prefix):]
                        trust_val = trust_row.get(col)
                        if isinstance(trust_val, str) and trust_val in ("Reliable", "Degraded"):
                            sensor_trust_map[sensor_id_trust] = trust_val

            # Look up per-sensor SQS from scores CSV
            # SQS columns: {sys_label}__{sensor_id}__SQS -> float 0-1
            sensor_sqs_map = {}
            if score_row is not None:
                prefix = f"{sys_label}__"
                sqs_suffix = "__SQS"
                for col in score_row.index:
                    if col.startswith(prefix) and col.endswith(sqs_suffix):
                        sensor_id_sqs = col[len(prefix):-len(sqs_suffix)]
                        sv = pd.to_numeric(pd.Series([score_row.get(col)]), errors="coerce").iloc[0]
                        if pd.notna(sv):
                            sensor_sqs_map[sensor_id_sqs] = float(sv)

            contribution_pairs = []
            if ranking_row is not None:
                prefix = f"{sys_label}__"
                suffix = "__Contribution"
                for col, raw_val in ranking_row.items():
                    if not col.startswith(prefix) or not col.endswith(suffix):
                        continue
                    sensor_id = col[len(prefix):-len(suffix)]
                    numeric = pd.to_numeric(pd.Series([raw_val]), errors="coerce").iloc[0]
                    if pd.notna(numeric) and float(numeric) > 0:
                        contribution_pairs.append((sensor_id, float(numeric)))

                contribution_pairs.sort(key=lambda item: (-item[1], item[0]))

                if not contribution_pairs:
                    for rank in range(1, 6):
                        sensor_col = f"{sys_label}___Rank_{rank}_Sensor"
                        score_rank_col = f"{sys_label}___Rank_{rank}_Score"
                        sensor_id = ranking_row.get(sensor_col)
                        numeric = pd.to_numeric(pd.Series([ranking_row.get(score_rank_col)]), errors="coerce").iloc[0]
                        if sensor_id and pd.notna(numeric):
                            contribution_pairs.append((str(sensor_id), float(numeric)))

            if contribution_pairs:
                top_sensor, top_score = contribution_pairs[0]
                affected_sensor_count = len(contribution_pairs)
                affected_sensors = "|".join(sensor for sensor, _ in contribution_pairs)
            else:
                top_sensor = "UNKNOWN"
                top_score = None
                affected_sensor_count = 0
                affected_sensors = "UNKNOWN"

            # Use raw contribution sum instead of normalized System_Score
            score_val = sum(score for _, score in contribution_pairs) if contribution_pairs else None

            # Compute contribution percentages
            total_contribution = sum(score for _, score in contribution_pairs) if contribution_pairs else 0

            # Count trust statuses for affected sensors
            reliable_count = sum(1 for sid, _ in contribution_pairs if sensor_trust_map.get(sid) == "Reliable")
            degraded_count = sum(1 for sid, _ in contribution_pairs if sensor_trust_map.get(sid) == "Degraded")

            # SQS stats for affected sensors
            affected_sqs = [sensor_sqs_map[sid] for sid, _ in contribution_pairs if sid in sensor_sqs_map]
            avg_sqs = float(sum(affected_sqs) / len(affected_sqs)) if affected_sqs else None
            sqs_good_count = sum(1 for v in affected_sqs if v >= 0.8)
            sqs_poor_count = sum(1 for v in affected_sqs if v < 0.8)

            alerts_rows.append({
                "view_type": "minute",
                "start_ts": ts_str,
                "end_ts": ts_str,
                "duration_minutes": 1,
                "minute_count": 1,
                "severity": severity,
                "severity_mix": f"1 {severity.lower()}",
                "high_count": 1 if severity == "HIGH" else 0,
                "medium_count": 1 if severity == "MEDIUM" else 0,
                "low_count": 1 if severity == "LOW" else 0,
                "class": sys_label,
                "sensor_id": top_sensor,
                "sensor_max_score": top_score,
                "sensor_mean_score": top_score,
                "affected_sensor_count": affected_sensor_count,
                "affected_sensors": affected_sensors,
                "max_score": score_val,
                "mean_score": score_val,
                "risk_score": risk_score_val,
                "peak_risk_score": risk_score_val,
                "mean_risk_score": risk_score_val,
                "adaptive_threshold": adaptive_threshold_val,
                "threshold": "ADAPTIVE",
                "system_confidence": system_confidence_val,
                "reliable_count": reliable_count,
                "degraded_count": degraded_count,
                "avg_sqs": round(avg_sqs, 3) if avg_sqs is not None else None,
                "sqs_good_count": sqs_good_count,
                "sqs_poor_count": sqs_poor_count,
            })

            for rank_idx, (sensor_id, sensor_score) in enumerate(contribution_pairs, start=1):
                pct = (sensor_score / total_contribution * 100) if total_contribution > 0 else 0
                sensor_rows.append({
                    "view_type": "minute",
                    "start_ts": ts_str,
                    "end_ts": ts_str,
                    "duration_minutes": 1,
                    "minute_count": 1,
                    "severity": severity,
                    "severity_mix": f"1 {severity.lower()}",
                    "high_count": 1 if severity == "HIGH" else 0,
                    "medium_count": 1 if severity == "MEDIUM" else 0,
                    "low_count": 1 if severity == "LOW" else 0,
                    "class": sys_label,
                    "sensor": sensor_id,
                    "sensor_rank": rank_idx,
                    "sensor_peak_score": sensor_score,
                    "sensor_mean_score": sensor_score,
                    "sensor_contribution_pct": round(pct, 1),
                    "sensor_trust": sensor_trust_map.get(sensor_id, "Unknown"),
                    "sensor_normal_minutes": 1 if sensor_trust_map.get(sensor_id) == "Reliable" else 0,
                    "sensor_anomalous_minutes": 1 if sensor_trust_map.get(sensor_id) == "Degraded" else 0,
                    "sensor_sqs": sensor_sqs_map.get(sensor_id),
                    "alert_max_score": score_val,
                    "alert_mean_score": score_val,
                    "risk_score": risk_score_val,
                    "peak_risk_score": risk_score_val,
                    "mean_risk_score": risk_score_val,
                    "adaptive_threshold": adaptive_threshold_val,
                    "threshold": "ADAPTIVE",
                })

    alerts_out = pd.DataFrame(alerts_rows, columns=alert_cols)
    sensor_out = pd.DataFrame(sensor_rows, columns=sensor_cols)
    return alerts_out, sensor_out


def _beta_format_severity_mix(high_count, medium_count, low_count):
    parts = []
    if high_count:
        parts.append(f"{int(high_count)} high")
    if medium_count:
        parts.append(f"{int(medium_count)} medium")
    if low_count:
        parts.append(f"{int(low_count)} low")
    return ", ".join(parts) if parts else "0 alerts"


def _beta_span_severity_label(high_count, medium_count, low_count):
    nonzero = sum(1 for count in [high_count, medium_count, low_count] if count)
    if nonzero > 1:
        return "MIXED"
    if high_count:
        return "HIGH"
    if medium_count:
        return "MEDIUM"
    if low_count:
        return "LOW"
    return "LOW"


def _beta_build_span_alert_tables():
    minute_alerts, minute_sensors = _beta_build_timestamp_alert_tables()
    if minute_alerts.empty:
        return minute_alerts.copy(), minute_sensors.copy()

    alerts = minute_alerts.copy()
    alerts["_ts_dt"] = safe_to_datetime(alerts["start_ts"], errors="coerce", utc=True)
    alerts = alerts.dropna(subset=["_ts_dt"]).sort_values(["class", "_ts_dt"]).reset_index(drop=True)

    sensors = minute_sensors.copy()
    if not sensors.empty:
        sensors["_ts_dt"] = safe_to_datetime(sensors["start_ts"], errors="coerce", utc=True)
        sensors = sensors.dropna(subset=["_ts_dt"]).sort_values(["class", "_ts_dt", "sensor"]).reset_index(drop=True)

    span_alert_rows = []
    span_sensor_rows = []

    for sys_label, sys_alerts in alerts.groupby("class", sort=True):
        sys_alerts = sys_alerts.sort_values("_ts_dt").reset_index(drop=True)
        span_start_idx = 0

        def finalize_span(start_idx, end_idx):
            span_df = sys_alerts.iloc[start_idx:end_idx + 1].copy()
            if span_df.empty:
                return

            start_dt = span_df["_ts_dt"].iloc[0]
            end_dt = span_df["_ts_dt"].iloc[-1]
            minute_count = int(len(span_df))
            duration_minutes = int((end_dt - start_dt).total_seconds() / 60) + 1

            high_count = int((span_df["severity"] == "HIGH").sum())
            medium_count = int((span_df["severity"] == "MEDIUM").sum())
            low_count = int((span_df["severity"] == "LOW").sum())
            severity = _beta_span_severity_label(high_count, medium_count, low_count)
            severity_mix = _beta_format_severity_mix(high_count, medium_count, low_count)

            span_sensor_df = pd.DataFrame()
            if not sensors.empty:
                span_sensor_df = sensors[
                    (sensors["class"] == sys_label) &
                    (sensors["_ts_dt"] >= start_dt) &
                    (sensors["_ts_dt"] <= end_dt)
                ].copy()

            sensor_agg_rows = []
            if not span_sensor_df.empty:
                grouped = span_sensor_df.groupby("sensor", sort=True)
                for sensor_id, sensor_group in grouped:
                    sensor_high_count = int((sensor_group["severity"] == "HIGH").sum())
                    sensor_medium_count = int((sensor_group["severity"] == "MEDIUM").sum())
                    sensor_low_count = int((sensor_group["severity"] == "LOW").sum())
                    sensor_scores = pd.to_numeric(sensor_group["sensor_peak_score"], errors="coerce")
                    active_scores = sensor_scores.dropna()
                    # Get trust from the most recent minute in the span for this sensor
                    sensor_trust_val = "Unknown"
                    sensor_normal_minutes = 0
                    sensor_anomalous_minutes = 0
                    if "sensor_trust" in sensor_group.columns:
                        trust_vals = sensor_group["sensor_trust"].dropna()
                        if not trust_vals.empty:
                            sensor_trust_val = trust_vals.iloc[-1]
                            sensor_normal_minutes = int((trust_vals == "Reliable").sum())
                            sensor_anomalous_minutes = int((trust_vals == "Degraded").sum())
                    # Get SQS from the most recent minute in the span
                    sensor_sqs_val = None
                    if "sensor_sqs" in sensor_group.columns:
                        sqs_vals = pd.to_numeric(sensor_group["sensor_sqs"], errors="coerce").dropna()
                        if not sqs_vals.empty:
                            sensor_sqs_val = float(sqs_vals.iloc[-1])
                    sensor_agg_rows.append({
                        "sensor": sensor_id,
                        "minute_count": int(len(sensor_group)),
                        "severity": _beta_span_severity_label(sensor_high_count, sensor_medium_count, sensor_low_count),
                        "severity_mix": _beta_format_severity_mix(sensor_high_count, sensor_medium_count, sensor_low_count),
                        "high_count": sensor_high_count,
                        "medium_count": sensor_medium_count,
                        "low_count": sensor_low_count,
                        "sensor_peak_score": float(active_scores.max()) if not active_scores.empty else None,
                        "sensor_mean_score": float(active_scores.mean()) if not active_scores.empty else 0.0,
                        "sensor_trust": sensor_trust_val,
                        "sensor_normal_minutes": sensor_normal_minutes,
                        "sensor_anomalous_minutes": sensor_anomalous_minutes,
                        "sensor_sqs": sensor_sqs_val,
                    })

            sensor_agg_rows.sort(
                key=lambda row: (
                    -(row["sensor_peak_score"] if row["sensor_peak_score"] is not None else -1),
                    -(row["sensor_mean_score"] if row["sensor_mean_score"] is not None else -1),
                    row["sensor"],
                )
            )

            # Compute contribution pct for span sensors
            span_total_contribution = sum(r["sensor_peak_score"] or 0 for r in sensor_agg_rows)

            for rank_idx, row in enumerate(sensor_agg_rows, start=1):
                pct = ((row["sensor_peak_score"] or 0) / span_total_contribution * 100) if span_total_contribution > 0 else 0
                span_sensor_rows.append({
                    "view_type": "span",
                    "start_ts": str(start_dt),
                    "end_ts": str(end_dt),
                    "duration_minutes": duration_minutes,
                    "minute_count": row["minute_count"],
                    "severity": row["severity"],
                    "severity_mix": row["severity_mix"],
                    "high_count": row["high_count"],
                    "medium_count": row["medium_count"],
                    "low_count": row["low_count"],
                    "class": sys_label,
                    "sensor": row["sensor"],
                    "sensor_rank": rank_idx,
                    "sensor_peak_score": row["sensor_peak_score"],
                    "sensor_mean_score": row["sensor_mean_score"],
                    "sensor_contribution_pct": round(pct, 1),
                    "sensor_trust": row.get("sensor_trust", "Unknown"),
                    "sensor_normal_minutes": row.get("sensor_normal_minutes", 0),
                    "sensor_anomalous_minutes": row.get("sensor_anomalous_minutes", 0),
                    "sensor_sqs": row.get("sensor_sqs"),
                    "alert_max_score": float(pd.to_numeric(span_df["max_score"], errors="coerce").max())
                    if not span_df["max_score"].empty else None,
                    "alert_mean_score": float(pd.to_numeric(span_df["mean_score"], errors="coerce").mean())
                    if not span_df["mean_score"].empty else None,
                    "risk_score": None,
                    "peak_risk_score": float(pd.to_numeric(span_df["risk_score"], errors="coerce").max())
                    if "risk_score" in span_df.columns else None,
                    "mean_risk_score": float(pd.to_numeric(span_df["risk_score"], errors="coerce").mean())
                    if "risk_score" in span_df.columns else None,
                    "adaptive_threshold": float(pd.to_numeric(span_df["adaptive_threshold"], errors="coerce").mean())
                    if "adaptive_threshold" in span_df.columns else None,
                    "threshold": "ADAPTIVE",
                })

            primary_sensor = sensor_agg_rows[0]["sensor"] if sensor_agg_rows else "UNKNOWN"
            primary_sensor_peak = sensor_agg_rows[0]["sensor_peak_score"] if sensor_agg_rows else None
            primary_sensor_mean = sensor_agg_rows[0]["sensor_mean_score"] if sensor_agg_rows else None
            affected_sensors = "|".join(row["sensor"] for row in sensor_agg_rows) if sensor_agg_rows else "UNKNOWN"

            # Aggregate risk scores across the span
            span_risk_scores = pd.to_numeric(span_df["risk_score"], errors="coerce") if "risk_score" in span_df.columns else pd.Series(dtype=float)
            span_thresholds = pd.to_numeric(span_df["adaptive_threshold"], errors="coerce") if "adaptive_threshold" in span_df.columns else pd.Series(dtype=float)

            span_alert_rows.append({
                "view_type": "span",
                "start_ts": str(start_dt),
                "end_ts": str(end_dt),
                "duration_minutes": duration_minutes,
                "minute_count": minute_count,
                "severity": severity,
                "severity_mix": severity_mix,
                "high_count": high_count,
                "medium_count": medium_count,
                "low_count": low_count,
                "class": sys_label,
                "sensor_id": primary_sensor,
                "sensor_max_score": primary_sensor_peak,
                "sensor_mean_score": primary_sensor_mean,
                "affected_sensor_count": len(sensor_agg_rows),
                "affected_sensors": affected_sensors,
                "max_score": float(pd.to_numeric(span_df["max_score"], errors="coerce").max())
                if not span_df["max_score"].empty else None,
                "mean_score": float(pd.to_numeric(span_df["mean_score"], errors="coerce").mean())
                if not span_df["mean_score"].empty else None,
                "risk_score": None,
                "peak_risk_score": float(span_risk_scores.max()) if not span_risk_scores.empty and span_risk_scores.notna().any() else None,
                "mean_risk_score": float(span_risk_scores.mean()) if not span_risk_scores.empty and span_risk_scores.notna().any() else None,
                "adaptive_threshold": float(span_thresholds.mean()) if not span_thresholds.empty and span_thresholds.notna().any() else None,
                "threshold": "ADAPTIVE",
                "system_confidence": float(pd.to_numeric(span_df["system_confidence"], errors="coerce").mean())
                if "system_confidence" in span_df.columns and not pd.to_numeric(span_df["system_confidence"], errors="coerce").isna().all() else None,
                "reliable_count": int(pd.to_numeric(span_df["reliable_count"], errors="coerce").max())
                if "reliable_count" in span_df.columns else 0,
                "degraded_count": int(pd.to_numeric(span_df["degraded_count"], errors="coerce").max())
                if "degraded_count" in span_df.columns else 0,
                "avg_sqs": float(pd.to_numeric(span_df["avg_sqs"], errors="coerce").mean())
                if "avg_sqs" in span_df.columns and not pd.to_numeric(span_df["avg_sqs"], errors="coerce").isna().all() else None,
                "sqs_good_count": int(pd.to_numeric(span_df["sqs_good_count"], errors="coerce").max())
                if "sqs_good_count" in span_df.columns else 0,
                "sqs_poor_count": int(pd.to_numeric(span_df["sqs_poor_count"], errors="coerce").max())
                if "sqs_poor_count" in span_df.columns else 0,
            })

        for idx in range(1, len(sys_alerts)):
            current_ts = sys_alerts.loc[idx, "_ts_dt"]
            prev_ts = sys_alerts.loc[idx - 1, "_ts_dt"]
            if current_ts - prev_ts != pd.Timedelta(minutes=1):
                finalize_span(span_start_idx, idx - 1)
                span_start_idx = idx
        finalize_span(span_start_idx, len(sys_alerts) - 1)

    alert_cols = minute_alerts.columns.tolist()
    sensor_cols = minute_sensors.columns.tolist()
    span_alerts = pd.DataFrame(span_alert_rows, columns=alert_cols)
    span_sensors = pd.DataFrame(span_sensor_rows, columns=sensor_cols)
    return span_alerts, span_sensors


def _beta_get_alarm_view():
    alarm_view = str(request.args.get("alarm_view", "minute")).strip().lower()
    return "span" if alarm_view == "span" else "minute"


def _beta_alert_cache_dependencies():
    return [
        "detailed_subsystem_alarms.csv",
        "detailed_sensor_rankings.csv",
        "detailed_system_sensor_scores.csv",
        "detailed_sensor_trust.csv",
    ]


def _beta_alert_cache_payload_stale(cached_entry):
    dep_mtimes = (cached_entry or {}).get("dep_mtimes", {})
    if not dep_mtimes:
        return True
    for dep, cached_mtime in dep_mtimes.items():
        path = _beta_resolve_path(dep)
        if path is None:
            return True
        if os.path.getmtime(path) != cached_mtime:
            return True
    return False


def _beta_build_alert_tables_for_view(alarm_view):
    cached = _beta_alert_tables_cache.get(alarm_view)
    if cached and not _beta_alert_cache_payload_stale(cached):
        return cached["alerts"].copy(), cached["sensors"].copy()

    if alarm_view == "span":
        alerts_df, sensors_df = _beta_build_span_alert_tables()
    else:
        alerts_df, sensors_df = _beta_build_timestamp_alert_tables()

    dep_mtimes = {}
    for dep in _beta_alert_cache_dependencies():
        path = _beta_resolve_path(dep)
        if path is not None:
            dep_mtimes[dep] = os.path.getmtime(path)
    _beta_alert_tables_cache[alarm_view] = {
        "alerts": alerts_df.copy(),
        "sensors": sensors_df.copy(),
        "dep_mtimes": dep_mtimes,
    }
    return alerts_df, sensors_df


def maybe_preload_beta_data():
    """
    Optional warmup for local/dev use.
    Disabled by default because eager loading large beta datasets can exceed
    memory limits on small hosts (for example 512MB Render instances).
    """
    preload_flag = str(os.environ.get("BETA_PRELOAD_ON_STARTUP", "")).strip().lower()
    should_preload = preload_flag in {"1", "true", "yes", "on"}
    if not should_preload:
        print("Beta data pre-load skipped (BETA_PRELOAD_ON_STARTUP not enabled).")
        return

    print("Pre-loading beta data files into cache...")
    for _pf in ["dynamic_catalog.csv", "dynamic_weights.csv", "sensor_config.csv",
                "df_chart_data.csv", "alerts.csv", "alerts_sensor_level.csv",
                "detailed_sqs.csv", "detailed_engine_a.csv",
                "detailed_engine_b.csv", "detailed_subsystem_scores.csv",
                "detailed_subsystem_alarms.csv"]:
        beta_load_csv(_pf)
        beta_load_csv_with_ts(_pf)
    try:
        _beta_build_alert_tables_for_view("minute")
        _beta_build_alert_tables_for_view("span")
        print("Beta alert table cache warm complete.")
    except Exception as _warm_err:
        print(f"Beta alert cache warm skipped: {_warm_err}")
    print("Beta data pre-load complete.")


# Optional pre-load at import time only when explicitly enabled.
maybe_preload_beta_data()


@app.route("/api/beta/login", methods=["POST"])
def beta_login():
    data = request.get_json()
    username = data.get("username", "")
    password = data.get("password", "")
    if password == "beta1234" and username:
        return jsonify({"success": True, "username": username, "mode": "beta"})
    return jsonify({"success": False, "message": "Invalid credentials"}), 401


@app.route("/api/beta/overview", methods=["GET"])
def beta_overview():
    scores_df, ts_col = beta_load_csv_with_ts("detailed_subsystem_scores.csv")
    catalog_df = beta_load_csv("dynamic_catalog.csv")
    config_df = beta_load_csv("sensor_config.csv")

    result = {
        "system_name": "Shredder",
        "total_sensors": 0,
        "downtime_minutes": 0,
        "running_minutes": 0,
        "total_minutes": 0,
        "downtime_pct": 0,
        "running_pct": 0,
        "data_range": {"start": None, "end": None},
    }

    if not scores_df.empty:
        result["total_minutes"] = len(scores_df)
        if "downtime_flag" in scores_df.columns:
            dt_flags = pd.to_numeric(scores_df["downtime_flag"], errors="coerce").fillna(0)
            result["downtime_minutes"] = int((dt_flags == 1).sum())
            result["running_minutes"] = int((dt_flags != 1).sum())
            total = result["total_minutes"]
            if total > 0:
                result["downtime_pct"] = round(result["downtime_minutes"] / total * 100, 1)
                result["running_pct"] = round(result["running_minutes"] / total * 100, 1)
        elif "mode" in scores_df.columns:
            result["running_minutes"] = int((scores_df["mode"] == "RUNNING").sum())
            result["downtime_minutes"] = int((scores_df["mode"] == "DOWNTIME").sum())
            total = result["total_minutes"]
            if total > 0:
                result["downtime_pct"] = round(result["downtime_minutes"] / total * 100, 1)
                result["running_pct"] = round(result["running_minutes"] / total * 100, 1)
        if ts_col and ts_col in scores_df.columns:
            ts_series = scores_df[ts_col].dropna()
            if len(ts_series) > 0:
                result["data_range"]["start"] = str(ts_series.iloc[0])
                result["data_range"]["end"] = str(ts_series.iloc[-1])

    if not catalog_df.empty:
        result["total_sensors"] = catalog_df["sensor"].nunique() if "sensor" in catalog_df.columns else 0

    return jsonify(result)


@app.route("/api/beta/sensor_validation_report", methods=["GET"])
def beta_sensor_validation_report():
    """Return the initial sensor validation report (pre-clustering)."""
    df = beta_load_csv("initial_sensor_validation_report.csv")
    if df.empty:
        return jsonify({"sensors": [], "total": 0, "passed": 0, "failed": 0})
    records = []
    for _, row in df.iterrows():
        records.append({
            "sensor": row.get("sensor", ""),
            "n_total": int(row.get("n_total", 0)),
            "n_missing": int(row.get("n_missing", 0)),
            "missing_ratio": round(float(row.get("missing_ratio", 0)), 4),
            "std": float(row.get("std", 0)),
            "n_unique": int(row.get("n_unique", 0)),
            "unique_ratio": round(float(row.get("unique_ratio", 0)), 4),
            "passed": bool(row.get("passed", True)),
            "removal_reasons": str(row.get("removal_reasons", "")) if pd.notna(row.get("removal_reasons")) else "",
        })
    passed = sum(1 for r in records if r["passed"])
    failed = len(records) - passed
    return jsonify({"sensors": records, "total": len(records), "passed": passed, "failed": failed})


@app.route("/api/beta/invalid_sensors", methods=["GET"])
def beta_invalid_sensors():
    """Return sensors with high invalid/NaN rates."""
    config_df = beta_load_csv("sensor_config.csv")
    if config_df.empty:
        return jsonify({"sensors": []})

    threshold = request.args.get("threshold", default=0.10, type=float)

    rows = []
    if "sensor" in config_df.columns and "missing_pct" in config_df.columns:
        for _, row in config_df.iterrows():
            mp = row.get("missing_pct", 0)
            if mp is not None and not (isinstance(mp, float) and math.isnan(mp)) and mp > threshold:
                rows.append({
                    "sensor": row["sensor"],
                    "missing_pct": round(float(mp) * 100, 1) if mp <= 1 else round(float(mp), 1),
                    "n_running": int(row.get("n_running", 0)) if not (isinstance(row.get("n_running"), float) and math.isnan(row.get("n_running", 0))) else 0,
                })
    rows.sort(key=lambda x: x["missing_pct"], reverse=True)
    return jsonify({"sensors": rows, "threshold_pct": round(threshold * 100, 1)})


@app.route("/api/beta/subsystems", methods=["GET"])
def beta_subsystems():
    catalog_df = beta_load_csv("dynamic_catalog.csv")
    weights_df = beta_load_csv("dynamic_weights.csv")
    summary_df = beta_load_csv("system_summary.csv")

    if catalog_df.empty:
        return jsonify({"subsystems": [], "isolated": []})

    grouped = catalog_df.groupby("system")["sensor"].apply(list).to_dict()

    fusion_weights = {}
    if not weights_df.empty:
        for _, row in weights_df.iterrows():
            if row.get("type") == "fusion":
                fusion_weights[row["key"]] = row["weight"]

    summary_lookup = {}
    if not summary_df.empty and "System_ID" in summary_df.columns:
        for _, row in summary_df.iterrows():
            summary_lookup[row["System_ID"]] = {
                "r2_adj_mean": row.get("R2_Adj_Mean"),
                "r2_adj_min": row.get("R2_Adj_Min"),
                "quality": row.get("Quality"),
                "cohesion": row.get("Cohesion_C"),
                "cond_number": row.get("Cond_Number_k"),
                "svd_status": row.get("SVD_Status"),
            }

    subsystems = []
    isolated = []
    for sys_label, sensors in sorted(grouped.items()):
        if sys_label == "ISOLATED":
            isolated = sensors
            continue
        info = {
            "system_id": sys_label,
            "sensors": sensors,
            "sensor_count": len(sensors),
            "fusion_weight": fusion_weights.get(sys_label),
        }
        if sys_label in summary_lookup:
            info.update(summary_lookup[sys_label])
        subsystems.append(info)

    # Include ISOLATED as a pseudo-system for the UI
    if isolated:
        subsystems.append({
            "system_id": "ISOLATED",
            "sensors": isolated,
            "sensor_count": len(isolated),
            "fusion_weight": None,
        })

    return jsonify({"subsystems": subsystems, "isolated": isolated})


@app.route("/api/beta/sensor_quality/<system_id>", methods=["GET"])
def beta_sensor_quality(system_id):
    """Return per-sensor SQS, Engine A, Engine B time series for a given subsystem."""
    downsample = request.args.get("downsample", default=1, type=int)
    start_ts = request.args.get("start_ts", default=None, type=str)
    end_ts = request.args.get("end_ts", default=None, type=str)

    catalog_df = beta_load_csv("dynamic_catalog.csv")
    empty_response = {
        "sensors": [], "timeseries": [], "subsystem_timeseries": [],
        "timestamp_col": "ts", "downtime_bands": [], "alarm_bands": [],
    }
    if catalog_df.empty:
        return jsonify(empty_response)

    sys_sensors = catalog_df[catalog_df["system"] == system_id]["sensor"].tolist() if "system" in catalog_df.columns else []
    if not sys_sensors:
        return jsonify(empty_response)

    # Try detailed files first (new pipeline outputs)
    sqs_df, sqs_ts = beta_load_csv_with_ts("detailed_sqs.csv")
    enga_df, enga_ts = beta_load_csv_with_ts("detailed_engine_a.csv")
    engb_df, engb_ts = beta_load_csv_with_ts("detailed_engine_b.csv")

    # Load downtime info from detailed_subsystem_alarms.csv
    alarms_df, alarms_ts = beta_load_csv_with_ts("detailed_subsystem_alarms.csv")

    prefix = f"{system_id}__"
    result_sensors = []
    timeseries = []

    has_detailed = not sqs_df.empty or not enga_df.empty or not engb_df.empty

    if has_detailed:
        ref_df = sqs_df if not sqs_df.empty else (enga_df if not enga_df.empty else engb_df)
        ref_ts = sqs_ts or enga_ts or engb_ts

        # Find sensor columns matching this system (prefixed first, then plain)
        for sensor in sys_sensors:
            col_key = f"{prefix}{sensor}"
            if any(col_key in df.columns for df in [sqs_df, enga_df, engb_df]):
                result_sensors.append(sensor)
        if not result_sensors:
            for sensor in sys_sensors:
                if any(sensor in df.columns for df in [sqs_df, enga_df, engb_df]):
                    result_sensors.append(sensor)

        # Vectorized: build a combined DataFrame with renamed columns
        combined = pd.DataFrame()
        if ref_ts and ref_ts in ref_df.columns:
            combined["ts"] = ref_df[ref_ts].astype(str).values

        # Downtime flag
        if not alarms_df.empty and "downtime_flag" in alarms_df.columns:
            dt_vals = alarms_df["downtime_flag"].iloc[:len(ref_df)]
            combined["downtime"] = dt_vals.fillna(0).astype(int).values[:len(ref_df)] if len(dt_vals) >= len(ref_df) else pd.concat([dt_vals, pd.Series([0] * (len(ref_df) - len(dt_vals)))]).fillna(0).astype(int).values

        for sensor in result_sensors:
            col_prefixed = f"{prefix}{sensor}"
            col_plain = sensor
            # SQS
            for col_try in [col_prefixed, col_plain]:
                if not sqs_df.empty and col_try in sqs_df.columns:
                    combined[f"{sensor}__sqs"] = sqs_df[col_try].iloc[:len(ref_df)].values
                    break
            # Engine A
            for col_try in [col_prefixed, col_plain]:
                if not enga_df.empty and col_try in enga_df.columns:
                    combined[f"{sensor}__a"] = enga_df[col_try].iloc[:len(ref_df)].values
                    break
            # Engine B
            for col_try in [col_prefixed, col_plain]:
                if not engb_df.empty and col_try in engb_df.columns:
                    combined[f"{sensor}__b"] = engb_df[col_try].iloc[:len(ref_df)].values
                    break

        # Time-range filter (before downsample)
        if (start_ts or end_ts) and "ts" in combined.columns:
            ts_parsed = safe_to_datetime(combined["ts"], errors="coerce")
            mask = pd.Series(True, index=combined.index)
            if start_ts:
                mask &= ts_parsed >= safe_to_datetime(start_ts)
            if end_ts:
                mask &= ts_parsed <= safe_to_datetime(end_ts)
            combined = combined.loc[mask].reset_index(drop=True)

        # Downsample
        if downsample > 1:
            combined = combined.iloc[::downsample]

        combined = sanitize_df(combined)
        timeseries = combined.to_dict(orient="records")
    else:
        result_sensors = sys_sensors

    # Downtime bands from detailed_subsystem_alarms.csv
    downtime_bands = []
    if not alarms_df.empty and "downtime_flag" in alarms_df.columns and alarms_ts:
        dt_mask = alarms_df["downtime_flag"] == 1
        if dt_mask.any():
            dt_times = safe_to_datetime(alarms_df.loc[dt_mask, alarms_ts], errors="coerce")
            dt_times = dt_times.sort_values().reset_index(drop=True)
            merge_gap = pd.Timedelta(minutes=15)
            span_start = dt_times.iloc[0]
            span_end = dt_times.iloc[0]
            for t in dt_times.iloc[1:]:
                if t - span_end <= merge_gap:
                    span_end = t
                else:
                    downtime_bands.append({"start": str(span_start), "end": str(span_end)})
                    span_start = t
                    span_end = t
            downtime_bands.append({"start": str(span_start), "end": str(span_end)})

    # Subsystem-level scores from detailed_subsystem_scores.csv (vectorized)
    sub_df, sub_ts = beta_load_csv_with_ts("detailed_subsystem_scores.csv")
    subsystem_timeseries = []
    score_col = f"{system_id}__System_Score"
    thresh_col = f"{system_id}__Adaptive_Threshold"
    has_subsystem = not sub_df.empty and sub_ts and score_col in sub_df.columns
    if has_subsystem:
        sub_combined = pd.DataFrame()
        sub_combined["ts"] = sub_df[sub_ts].astype(str).values
        if "downtime_flag" in sub_df.columns:
            sub_combined["downtime"] = sub_df["downtime_flag"].fillna(0).astype(int).values
        sub_combined["system_score"] = sub_df[score_col].round(6).values
        if thresh_col in sub_df.columns:
            sub_combined["adaptive_threshold"] = sub_df[thresh_col].round(6).values
        # Time-range filter
        if (start_ts or end_ts) and "ts" in sub_combined.columns:
            ts_parsed = safe_to_datetime(sub_combined["ts"], errors="coerce")
            mask = pd.Series(True, index=sub_combined.index)
            if start_ts:
                mask &= ts_parsed >= safe_to_datetime(start_ts)
            if end_ts:
                mask &= ts_parsed <= safe_to_datetime(end_ts)
            sub_combined = sub_combined.loc[mask].reset_index(drop=True)
        if downsample > 1:
            sub_combined = sub_combined.iloc[::downsample]
        sub_combined = sanitize_df(sub_combined)
        subsystem_timeseries = sub_combined.to_dict(orient="records")

    return jsonify({
        "sensors": result_sensors,
        "timeseries": timeseries,
        "subsystem_timeseries": subsystem_timeseries,
        "timestamp_col": "ts",
        "downtime_bands": downtime_bands,
        "alarm_bands": [],
    })


@app.route("/api/beta/subsystem_scores", methods=["GET"])
def beta_subsystem_scores():
    """Return subsystem score time series for stacked chart."""
    downsample = request.args.get("downsample", default=1, type=int)

    sub_df, sub_ts = beta_load_csv_with_ts("detailed_subsystem_scores.csv")

    systems = []
    timeseries = []

    if not sub_df.empty and sub_ts:
        # New pipeline: columns like SYS_1__System_Score, downtime_flag
        score_cols = [c for c in sub_df.columns if c.endswith("__System_Score")]
        systems = [c.replace("__System_Score", "") for c in score_cols]

        n = len(sub_df)
        indices = list(range(0, n, max(1, downsample)))
        for i in indices:
            row = {"ts": str(sub_df[sub_ts].iloc[i])}
            if "downtime_flag" in sub_df.columns:
                row["downtime"] = int(sub_df["downtime_flag"].iloc[i]) if not (isinstance(sub_df["downtime_flag"].iloc[i], float) and math.isnan(sub_df["downtime_flag"].iloc[i])) else 0
            else:
                row["downtime"] = 0
            for col in score_cols:
                sys_name = col.replace("__System_Score", "")
                v = sub_df[col].iloc[i]
                row[sys_name] = None if (isinstance(v, float) and math.isnan(v)) else round(float(v), 4) if v is not None else None
            timeseries.append(row)
    return jsonify({"systems": systems, "timeseries": timeseries, "timestamp_col": "ts"})


@app.route("/api/beta/subsystem_behavior/<system_id>", methods=["GET"])
def beta_subsystem_behavior(system_id):
    """Return beta subsystem behavior from a single beta artifact folder.

    Raw sensor traces, subsystem mapping, downtime bands, and alarm bands are all
    read from BETA_DATA_DIR. This avoids the previous split where live traces came
    from data/ while beta overlays came from data_beta/.
    """
    downsample = request.args.get("downsample", default=1, type=int)

    # --- Raw sensor data from beta artifact directory ---
    catalog_df = beta_load_csv("dynamic_catalog.csv")
    chart_df, ts_col = beta_load_csv_with_ts("df_chart_data.csv")
    sensor_df = pd.DataFrame()

    if not chart_df.empty and not catalog_df.empty:
        sys_sensors = catalog_df[catalog_df["system"] == system_id]["sensor"].tolist() if "system" in catalog_df.columns else []
        keep_cols = [ts_col] + [s for s in sys_sensors if s in chart_df.columns] if ts_col else [s for s in sys_sensors if s in chart_df.columns]
        sensor_df = chart_df[keep_cols]
    else:
        fname = f"sensor_values_{system_id}.csv"
        sensor_df, ts_col = beta_load_csv_with_ts(fname)

    if sensor_df.empty:
        return jsonify({"timeseries": [], "sensors": [], "timestamp_col": "", "downtime_bands": [], "alarm_bands": []})

    sensor_df = sensor_df.iloc[::max(1, downsample)]
    sensor_df = sanitize_df(sensor_df)
    if ts_col and ts_col in sensor_df.columns:
        sensor_df[ts_col] = sensor_df[ts_col].astype(str)
    sensor_cols = [c for c in sensor_df.columns if c != ts_col]
    timeseries = sensor_df.to_dict(orient="records")

    # --- Downtime and alarm bands from the same beta artifact directory ---
    alarms_df, alarms_ts = beta_load_csv_with_ts("detailed_subsystem_alarms.csv")

    # Downtime bands
    downtime_bands = []
    if not alarms_df.empty and "downtime_flag" in alarms_df.columns and alarms_ts:
        dt_mask = alarms_df["downtime_flag"] == 1
        if dt_mask.any():
            dt_times = safe_to_datetime(alarms_df.loc[dt_mask, alarms_ts], errors="coerce")
            dt_times = dt_times.sort_values().reset_index(drop=True)
            merge_gap = pd.Timedelta(minutes=15)
            span_start = dt_times.iloc[0]
            span_end = dt_times.iloc[0]
            for t in dt_times.iloc[1:]:
                if t - span_end <= merge_gap:
                    span_end = t
                else:
                    downtime_bands.append({"start": str(span_start), "end": str(span_end)})
                    span_start = t
                    span_end = t
            downtime_bands.append({"start": str(span_start), "end": str(span_end)})

    return jsonify({
        "timeseries": timeseries,
        "timestamp_col": ts_col,
        "sensors": sensor_cols,
        "downtime_bands": downtime_bands,
        "alarm_bands": [],
    })


@app.route("/api/beta/sensor_contributions/<system_id>", methods=["GET"])
def beta_sensor_contributions(system_id):
    """Return per-sensor AE contribution time series for a subsystem, normalized to stack to system score."""
    downsample = request.args.get("downsample", default=1, type=int)

    df, ts_col = beta_load_csv_with_ts("detailed_system_sensor_scores.csv")
    if df.empty or not ts_col:
        return jsonify({"sensors": [], "timeseries": [], "timestamp_col": "ts"})

    # Find contribution columns for this system: {system_id}__{sensor}__AE_Contribution
    prefix = f"{system_id}__"
    contrib_cols = [c for c in df.columns if c.startswith(prefix) and c.endswith("__AE_Contribution")]
    if not contrib_cols:
        return jsonify({"sensors": [], "timeseries": [], "timestamp_col": "ts"})

    # Extract sensor names
    sensors = [c.replace(prefix, "").replace("__AE_Contribution", "") for c in contrib_cols]

    # System score column
    score_col = f"{system_id}__System_Score"
    has_score = score_col in df.columns

    # Build output
    combined = pd.DataFrame()
    combined["ts"] = df[ts_col].astype(str).values
    if has_score:
        combined["system_score"] = df[score_col].values

    # Normalize contributions so they stack to system_score
    contrib_raw = df[contrib_cols].copy()
    contrib_raw.columns = sensors
    total = contrib_raw.sum(axis=1).replace(0, float("nan"))
    if has_score:
        for s in sensors:
            combined[s] = (contrib_raw[s] / total * df[score_col]).values
    else:
        for s in sensors:
            combined[s] = contrib_raw[s].values

    if downsample > 1:
        combined = combined.iloc[::downsample]

    combined = sanitize_df(combined)
    return jsonify({
        "sensors": sensors,
        "timeseries": combined.to_dict(orient="records"),
        "timestamp_col": "ts",
    })


@app.route("/api/beta/ae_metadata", methods=["GET"])
def beta_ae_metadata():
    df = beta_load_csv("ae_model_metadata.csv")
    if df.empty:
        return jsonify({"models": []})
    df = sanitize_df(df)
    return jsonify({"models": df.to_dict(orient="records")})


# Reuse existing endpoints for beta alerts/scores/risk — just proxy to same data
@app.route("/api/beta/alerts", methods=["GET"])
def beta_alerts():
    alarm_view = _beta_get_alarm_view()
    class_filter = request.args.get("class", default=None, type=str)
    df, _ = _beta_build_alert_tables_for_view(alarm_view)
    if df.empty:
        return jsonify({"alerts": [], "summary": {}})
    if "class" in df.columns:
        df = df[df["class"] != "NORMAL"]
        if class_filter:
            df = df[df["class"] == class_filter]
    df = sanitize_df(df)
    if alarm_view == "span":
        high_count = int((pd.to_numeric(df["high_count"], errors="coerce").fillna(0) > 0).sum()) if "high_count" in df.columns else 0
        medium_count = int((pd.to_numeric(df["medium_count"], errors="coerce").fillna(0) > 0).sum()) if "medium_count" in df.columns else 0
        low_count = int((pd.to_numeric(df["low_count"], errors="coerce").fillna(0) > 0).sum()) if "low_count" in df.columns else 0
    else:
        high_count = len(df[df["severity"] == "HIGH"]) if "severity" in df.columns else 0
        medium_count = len(df[df["severity"] == "MEDIUM"]) if "severity" in df.columns else 0
        low_count = len(df[df["severity"] == "LOW"]) if "severity" in df.columns else 0
    class_dist = df["class"].value_counts().to_dict() if "class" in df.columns else {}
    summary = {
        "total_alerts": len(df),
        "high_count": int(high_count),
        "medium_count": int(medium_count),
        "low_count": int(low_count),
        "class_distribution": class_dist,
        "alarm_view": alarm_view,
    }
    return jsonify({"alerts": df.to_dict(orient="records"), "summary": summary})


@app.route("/api/beta/scores/timeseries", methods=["GET"])
def beta_scores_timeseries():
    df, ts_col = beta_load_csv_with_ts("detailed_subsystem_scores.csv")
    if df.empty:
        return jsonify({"timeseries": [], "timestamp_col": None})
    if ts_col is None:
        return jsonify({"timeseries": [], "timestamp_col": None})
    downsample = request.args.get("downsample", default=1, type=int)
    df_sampled = df.iloc[::max(1, downsample)]
    # Map beta column names to the format the frontend expects
    rename_map = {}
    for col in df_sampled.columns:
        if col.endswith("__System_Score"):
            sys_id = col.replace("__System_Score", "")
            rename_map[col] = f"score_{sys_id}"
    df_sampled = df_sampled.rename(columns=rename_map)
    cols_to_include = [ts_col]
    for col in ["risk_score", "subsystem_score", "sqs_mean", "mode", "class", "downtime_flag"]:
        if col in df_sampled.columns and col != ts_col:
            cols_to_include.append(col)
    for col in df_sampled.columns:
        if (col.startswith("score_SYS_") or col.startswith("risk_SYS_") or
            col.startswith("ae_score_") or col.startswith("ae_alarm_")):
            if col not in cols_to_include:
                cols_to_include.append(col)
    existing_cols = [c for c in cols_to_include if c in df_sampled.columns]
    df_out = sanitize_df(df_sampled[existing_cols].copy())
    if ts_col in df_out.columns:
        df_out[ts_col] = df_out[ts_col].astype(str)
    return jsonify({"timeseries": df_out.to_dict(orient="records"), "timestamp_col": ts_col})


@app.route("/api/beta/dashboard/summary", methods=["GET"])
def beta_dashboard_summary():
    alarm_view = _beta_get_alarm_view()
    alerts_df, _ = _beta_build_alert_tables_for_view(alarm_view)
    scores_df, ts_col = beta_load_csv_with_ts("detailed_subsystem_scores.csv")
    catalog_df = beta_load_csv("dynamic_catalog.csv")

    summary = {
        "total_alerts": 0, "high_alerts": 0, "medium_alerts": 0, "low_alerts": 0,
        "total_sensors": 0, "class_distribution": {},
        "avg_risk_score": None, "max_risk_score": None, "current_risk_score": None,
        "avg_sqs": None, "running_pct": None,
        "data_range": {"start": None, "end": None},
    }

    if not alerts_df.empty:
        if "class" in alerts_df.columns:
            alerts_df = alerts_df[alerts_df["class"] != "NORMAL"]
        summary["total_alerts"] = len(alerts_df)
        if alarm_view == "span":
            summary["high_alerts"] = int((pd.to_numeric(alerts_df["high_count"], errors="coerce").fillna(0) > 0).sum()) if "high_count" in alerts_df.columns else 0
            summary["medium_alerts"] = int((pd.to_numeric(alerts_df["medium_count"], errors="coerce").fillna(0) > 0).sum()) if "medium_count" in alerts_df.columns else 0
            summary["low_alerts"] = int((pd.to_numeric(alerts_df["low_count"], errors="coerce").fillna(0) > 0).sum()) if "low_count" in alerts_df.columns else 0
        elif "severity" in alerts_df.columns:
            summary["high_alerts"] = int((alerts_df["severity"] == "HIGH").sum())
            summary["medium_alerts"] = int((alerts_df["severity"] == "MEDIUM").sum())
            summary["low_alerts"] = int((alerts_df["severity"] == "LOW").sum())
        if "class" in alerts_df.columns:
            summary["class_distribution"] = alerts_df["class"].value_counts().to_dict()
        summary["alarm_view"] = alarm_view

    if not scores_df.empty:
        if "risk_score" in scores_df.columns:
            rs = pd.to_numeric(scores_df["risk_score"], errors="coerce")
            summary["avg_risk_score"] = round(float(rs.mean()), 4) if not rs.isna().all() else None
            summary["max_risk_score"] = round(float(rs.max()), 4) if not rs.isna().all() else None
            last_valid = rs.dropna()
            if len(last_valid) > 0:
                summary["current_risk_score"] = round(float(last_valid.iloc[-1]), 4)
        if "sqs_mean" in scores_df.columns:
            sqs = pd.to_numeric(scores_df["sqs_mean"], errors="coerce")
            summary["avg_sqs"] = round(float(sqs.mean()), 4) if not sqs.isna().all() else None
        if "mode" in scores_df.columns:
            total = len(scores_df)
            running = int((scores_df["mode"] == "RUNNING").sum())
            summary["running_pct"] = round(running / total * 100, 1) if total > 0 else None
        if ts_col and ts_col in scores_df.columns:
            ts_series = scores_df[ts_col].dropna()
            if len(ts_series) > 0:
                summary["data_range"]["start"] = str(ts_series.iloc[0])
                summary["data_range"]["end"] = str(ts_series.iloc[-1])

    if not catalog_df.empty:
        summary["total_sensors"] = catalog_df["sensor"].nunique() if "sensor" in catalog_df.columns else 0

    return jsonify(summary)


@app.route("/api/beta/scores", methods=["GET"])
def beta_scores():
    df, ts_col = beta_load_csv_with_ts("detailed_subsystem_scores.csv")
    if df.empty:
        return jsonify({"scores": [], "stats": {}, "timestamp_col": None})
    df = sanitize_df(df)
    limit = request.args.get("limit", default=None, type=int)
    offset = request.args.get("offset", default=0, type=int)
    total_rows = len(df)
    normal_count = int((df["class"] == "NORMAL").sum()) if "class" in df.columns else 0
    anomaly_count = int((df["class"] != "NORMAL").sum()) if "class" in df.columns else 0
    running_count = int((df["mode"] == "RUNNING").sum()) if "mode" in df.columns else 0
    downtime_count = int((df["mode"] == "DOWNTIME").sum()) if "mode" in df.columns else 0
    if limit:
        df = df.iloc[offset: offset + limit]
    stats = {}
    score_cols = ["risk_score", "subsystem_score", "sqs_mean"]
    for col in df.columns:
        if col.startswith("score_SYS_") or col.startswith("risk_SYS_") or col.startswith("ae_score_"):
            score_cols.append(col)
    for col in score_cols:
        if col in df.columns:
            series = pd.to_numeric(df[col], errors="coerce")
            stats[col] = {
                "mean": round(float(series.mean()), 4) if not series.isna().all() else None,
                "max": round(float(series.max()), 4) if not series.isna().all() else None,
                "min": round(float(series.min()), 4) if not series.isna().all() else None,
                "std": round(float(series.std()), 4) if not series.isna().all() else None,
            }
    stats["normal_count"] = normal_count
    stats["anomaly_count"] = anomaly_count
    stats["running_count"] = running_count
    stats["downtime_count"] = downtime_count
    stats["total_rows"] = total_rows
    return jsonify({"scores": df.to_dict(orient="records"), "stats": stats, "timestamp_col": ts_col})


@app.route("/api/beta/normal_periods", methods=["GET"])
def beta_normal_periods():
    scores_df, ts_col = beta_load_csv_with_ts("detailed_subsystem_scores.csv")
    if scores_df.empty or "class" not in scores_df.columns:
        return jsonify({"normal_stats": {}, "periods": []})
    normal_mask = scores_df["class"] == "NORMAL"
    total = len(scores_df)
    normal_count = int(normal_mask.sum())
    normal_stats = {
        "normal_count": normal_count,
        "total_count": total,
        "normal_pct": round(normal_count / total * 100, 1) if total > 0 else 0,
    }
    if "risk_score" in scores_df.columns:
        nr = pd.to_numeric(scores_df.loc[normal_mask, "risk_score"], errors="coerce")
        normal_stats["avg_risk_during_normal"] = round(float(nr.mean()), 4) if not nr.isna().all() else None
    if "sqs_mean" in scores_df.columns:
        ns = pd.to_numeric(scores_df.loc[normal_mask, "sqs_mean"], errors="coerce")
        normal_stats["avg_sqs_during_normal"] = round(float(ns.mean()), 4) if not ns.isna().all() else None
    return jsonify({"normal_stats": normal_stats})


@app.route("/api/beta/systems", methods=["GET"])
def beta_systems():
    return beta_subsystems()


@app.route("/api/beta/radar_fingerprints", methods=["GET"])
def beta_radar_fingerprints():
    """Compute per-subsystem radar fingerprint data from detailed_system_sensor_scores.csv.

    For each subsystem, finds alarm windows, picks the one with highest peak risk,
    and returns mean AE_Contribution per sensor during that window.
    Also enriches with subsystem_summary.csv and system_summary.csv stats.
    """
    scores_df, scores_ts = beta_load_csv_with_ts("detailed_system_sensor_scores.csv")
    catalog_df = beta_load_csv("dynamic_catalog.csv")

    if scores_df.empty or catalog_df.empty:
        return jsonify({"fingerprints": []})

    # Load enrichment data
    sub_summary = beta_load_csv("subsystem_summary.csv")
    sys_summary = beta_load_csv("system_summary.csv")

    sub_lookup = {}
    if not sub_summary.empty and "Subsystem" in sub_summary.columns:
        for _, row in sub_summary.iterrows():
            sub_lookup[row["Subsystem"]] = row.to_dict()

    sys_lookup = {}
    if not sys_summary.empty and "System_ID" in sys_summary.columns:
        for _, row in sys_summary.iterrows():
            sys_lookup[row["System_ID"]] = row.to_dict()

    grouped = catalog_df.groupby("system")["sensor"].apply(list).to_dict()
    fingerprints = []

    for sys_label, sensors in sorted(grouped.items()):
        if sys_label == "ISOLATED":
            continue

        prefix = f"{sys_label}__"
        alarm_col = f"{prefix}System_Alarm"
        score_col = f"{prefix}System_Score"
        conf_col = f"{prefix}System_Confidence"
        thresh_col = f"{prefix}Adaptive_Threshold"
        sigma_col = f"{prefix}Baseline_Sigma"

        if alarm_col not in scores_df.columns or score_col not in scores_df.columns:
            continue

        alarm_s = scores_df[alarm_col].fillna(0).astype(int)
        risk_s = pd.to_numeric(scores_df[score_col], errors="coerce").fillna(0)

        # Latest row snapshot for current state
        last_valid_idx = scores_df[score_col].dropna().index
        latest = {}
        if len(last_valid_idx):
            li = last_valid_idx[-1]
            latest["current_score"] = round(float(risk_s.iloc[li]), 6) if li < len(risk_s) else None
            if conf_col in scores_df.columns:
                v = scores_df[conf_col].iloc[li]
                latest["confidence"] = round(float(v), 4) if pd.notna(v) else None
            if thresh_col in scores_df.columns:
                v = scores_df[thresh_col].iloc[li]
                latest["threshold"] = round(float(v), 6) if pd.notna(v) else None
            if sigma_col in scores_df.columns:
                v = scores_df[sigma_col].iloc[li]
                latest["baseline_sigma"] = round(float(v), 6) if pd.notna(v) else None
            latest["current_alarm"] = int(alarm_s.iloc[li])

            # Top ranked sensors from latest row
            top_sensors = []
            for rk in range(1, 6):
                s_col = f"{prefix}_Rank_{rk}_Sensor"
                sc_col = f"{prefix}_Rank_{rk}_Score"
                pct_col = f"{prefix}_Rank_{rk}_Pct"
                if s_col in scores_df.columns:
                    sname = scores_df[s_col].iloc[li]
                    if pd.notna(sname):
                        sc_val = float(scores_df[sc_col].iloc[li]) if sc_col in scores_df.columns and pd.notna(scores_df[sc_col].iloc[li]) else None
                        pct_val = float(scores_df[pct_col].iloc[li]) if pct_col in scores_df.columns and pd.notna(scores_df[pct_col].iloc[li]) else None
                        top_sensors.append({"rank": rk, "sensor": str(sname), "score": round(sc_val, 6) if sc_val is not None else None, "pct": round(pct_val, 2) if pct_val is not None else None})
            if top_sensors:
                latest["top_sensors"] = top_sensors

        # Per-sensor trust and SQS from latest row
        sensor_meta = {}
        if len(last_valid_idx):
            li = last_valid_idx[-1]
            for s in sensors:
                sm = {}
                trust_col = f"{prefix}{s}__Trust"
                sqs_col = f"{prefix}{s}__SQS"
                if trust_col in scores_df.columns:
                    v = scores_df[trust_col].iloc[li]
                    sm["trust"] = str(v) if pd.notna(v) else None
                if sqs_col in scores_df.columns:
                    v = scores_df[sqs_col].iloc[li]
                    sm["sqs"] = round(float(v), 4) if pd.notna(v) else None
                if sm:
                    sensor_meta[s] = sm

        # Enrichment from subsystem_summary.csv
        summary_info = {}
        if sys_label in sub_lookup:
            row = sub_lookup[sys_label]
            for k in ["Sensors", "Score_Mean", "Thresh_Mean", "High%", "High_Count", "Alarms", "Top_Alarm_Contributor", "Confidence", "Baseline_\u03c3"]:
                v = row.get(k)
                if v is not None and str(v) != "N/A" and str(v) != "nan":
                    summary_info[k.lower().replace("%", "_pct").replace("\u03c3", "sigma")] = v

        # Enrichment from system_summary.csv
        model_info = {}
        if sys_label in sys_lookup:
            row = sys_lookup[sys_label]
            for k in ["R2_Adj_Mean", "R2_Adj_Min", "Quality", "Cohesion_C", "N_Variables"]:
                v = row.get(k)
                if v is not None and str(v) != "nan":
                    model_info[k.lower()] = v

        # Find contiguous alarm windows
        diff = alarm_s.diff()
        starts = alarm_s.index[diff == 1].tolist()
        ends = alarm_s.index[diff == -1].tolist()
        if alarm_s.iloc[-1] == 1:
            ends.append(alarm_s.index[-1])
        if alarm_s.iloc[0] == 1 and (not starts or (ends and ends[0] < starts[0])):
            starts.insert(0, alarm_s.index[0])

        base_entry = {
            "system_id": sys_label,
            "sensor_count": len(sensors),
            "latest": latest,
            "sensor_meta": sensor_meta,
            "summary": summary_info,
            "model": model_info,
        }

        # Determine which column to use: prefer Sigma_Score, fall back to AE_Contribution
        def _sensor_col(s):
            sigma_col = f"{prefix}{s}__Sigma_Score"
            ae_col = f"{prefix}{s}__AE_Contribution"
            if sigma_col in scores_df.columns:
                return sigma_col
            return ae_col if ae_col in scores_df.columns else None

        if not starts or not ends:
            data_cols = {s: _sensor_col(s) for s in sensors}
            exist_cols = [c for c in data_cols.values() if c is not None]
            if not exist_cols:
                continue
            last_valid = scores_df[exist_cols].dropna(how="all")
            if last_valid.empty:
                continue
            last_row = last_valid.iloc[-1]
            sensor_data = []
            for s in sensors:
                col = data_cols[s]
                val = float(last_row.get(col, 0)) if col and col in last_row.index else 0
                sensor_data.append({"sensor": s, "value": round(val, 6)})
            fingerprints.append({
                **base_entry,
                "sensors": sensor_data,
                "has_alarm": False,
                "peak_risk": float(risk_s.max()),
                "alarm_count": 0,
            })
            continue

        # Find peak alarm event
        best_peak = -1
        best_start = starts[0]
        best_end = ends[0]
        for s_idx, e_idx in zip(starts, ends):
            window_risk = risk_s.iloc[s_idx:e_idx + 1]
            peak = float(window_risk.max()) if len(window_risk) else 0
            if peak > best_peak:
                best_peak = peak
                best_start = s_idx
                best_end = e_idx

        # Mean per-sensor value in peak alarm window (Sigma_Score preferred)
        sensor_data = []
        for s in sensors:
            col = _sensor_col(s)
            if col:
                window_vals = pd.to_numeric(scores_df[col].iloc[best_start:best_end + 1], errors="coerce").dropna()
                val = float(window_vals.mean()) if len(window_vals) else 0
            else:
                val = 0
            sensor_data.append({"sensor": s, "value": round(val, 6)})

        event_info = {}
        if scores_ts and scores_ts in scores_df.columns:
            event_info["event_start"] = str(scores_df[scores_ts].iloc[best_start])
            event_info["event_end"] = str(scores_df[scores_ts].iloc[best_end])

        fingerprints.append({
            **base_entry,
            "sensors": sensor_data,
            "has_alarm": True,
            "peak_risk": round(best_peak, 6),
            "alarm_count": len(starts),
            **event_info,
        })

    # sanitize NaN/Inf in nested structures
    import json
    import math
    def _clean(obj):
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        if isinstance(obj, dict):
            return {k: _clean(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_clean(v) for v in obj]
        return obj

    return jsonify({"fingerprints": _clean(fingerprints)})


@app.route("/api/beta/alerts_sensor_level", methods=["GET"])
def beta_alerts_sensor_level():
    alarm_view = _beta_get_alarm_view()
    _, df = _beta_build_alert_tables_for_view(alarm_view)
    if df.empty:
        return jsonify({"sensor_alerts": []})
    df = sanitize_df(df)
    start_ts = request.args.get("start_ts")
    end_ts = request.args.get("end_ts")
    alert_class = request.args.get("class")
    if start_ts and "start_ts" in df.columns:
        df = df[df["start_ts"] == start_ts]
    if end_ts and "end_ts" in df.columns:
        df = df[df["end_ts"] == end_ts]
    if alert_class and "class" in df.columns:
        df = df[df["class"] == alert_class]
    return jsonify({"sensor_alerts": df.to_dict(orient="records")})


@app.route("/api/beta/risk_decomposition/episode", methods=["GET"])
def beta_risk_decomposition_episode():
    df = beta_load_csv("risk_sensor_decomposition.csv")
    if df.empty:
        return jsonify({"decomposition": [], "flow_data": {}})
    start_ts = request.args.get("start_ts")
    end_ts = request.args.get("end_ts")
    if not start_ts or not end_ts:
        return jsonify({"decomposition": [], "flow_data": {}})
    if "timestamp_utc" in df.columns:
        try:
            df["timestamp_utc"] = safe_to_datetime(df["timestamp_utc"], errors="coerce", utc=True)
            start_dt = safe_to_datetime(start_ts, utc=True)
            end_dt = safe_to_datetime(end_ts, utc=True)
            df = df[(df["timestamp_utc"] >= start_dt) & (df["timestamp_utc"] <= end_dt)]
        except Exception:
            df["timestamp_utc"] = df["timestamp_utc"].astype(str)
            df = df[(df["timestamp_utc"] >= start_ts) & (df["timestamp_utc"] <= end_ts)]
    df = sanitize_df(df)
    flow_data = {"subsystems": {}, "sensors": {}}
    if "subsystem" in df.columns and "risk_score_component" in df.columns:
        risk_col = pd.to_numeric(df["risk_score_component"], errors="coerce")
        sub_agg = df.assign(risk_numeric=risk_col).groupby("subsystem").agg(
            total_risk=("risk_numeric", "sum"),
            mean_risk=("risk_numeric", "mean"),
            max_risk=("risk_numeric", "max"),
            sensor_count=("sensor_id", "nunique"),
        ).to_dict(orient="index")
        flow_data["subsystems"] = sub_agg
    if "sensor_id" in df.columns and "risk_score_component" in df.columns:
        df_calc = df.copy()
        df_calc["risk_numeric"] = pd.to_numeric(df_calc["risk_score_component"], errors="coerce")
        sensor_agg = df_calc.groupby(["subsystem", "sensor_id"]).agg(
            total_risk=("risk_numeric", "sum"),
            mean_risk=("risk_numeric", "mean"),
            max_risk=("risk_numeric", "max"),
        ).reset_index()
        sensor_agg = sanitize_df(sensor_agg)
        flow_data["sensors"] = sensor_agg.to_dict(orient="records")
    return jsonify({"decomposition": df.to_dict(orient="records"), "flow_data": flow_data})


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_frontend(path):
    static = app.static_folder
    if path and os.path.exists(os.path.join(static, path)):
        return send_from_directory(static, path)
    return send_from_directory(static, "index.html")


# Start keep-alive thread for Render (works under both gunicorn and direct run)
if RENDER_EXTERNAL_URL:
    _keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    _keep_alive_thread.start()

if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"Data directory: {DATA_DIR}")
    print("Place your CSV files in the data/ folder.")

    for fname in ["scores.csv", "alerts.csv", "alerts_sensor_level.csv",
                   "risk_sensor_decomposition.csv", "sensor_config.csv"]:
        fpath = os.path.join(DATA_DIR, fname)
        if os.path.exists(fpath):
            print(f"  [OK] {fname}")
        else:
            print(f"  [MISSING] {fname}")

    # Optional warmup for local runs; disabled unless explicitly enabled.
    maybe_preload_beta_data()

    backend_host = os.environ.get("HOST", "0.0.0.0")
    backend_port = int(os.environ.get("PORT", "5000"))
    app.run(debug=True, host=backend_host, port=backend_port)
