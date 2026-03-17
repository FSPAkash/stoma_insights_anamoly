import os
import math
import json
import subprocess
import sys
import threading
import time
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory
from flask.json.provider import DefaultJSONProvider
from flask_cors import CORS
import pandas as pd
import requests as http_requests


class SafeJSONProvider(DefaultJSONProvider):
    def dumps(self, obj, **kwargs):
        return json.dumps(self._sanitize(obj), **kwargs)

    def _sanitize(self, o):
        if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
            return None
        if isinstance(o, dict):
            return {k: self._sanitize(v) for k, v in o.items()}
        if isinstance(o, list):
            return [self._sanitize(v) for v in o]
        return o


STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "frontend", "build")

app = Flask(__name__, static_folder=STATIC_DIR, static_url_path="")
app.json_provider_class = SafeJSONProvider
app.json = SafeJSONProvider(app)
CORS(app)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
BETA_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_beta")

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


def find_timestamp_col(df):
    for candidate in TIMESTAMP_CANDIDATES:
        if candidate in df.columns:
            return candidate
    for col in df.columns:
        col_lower = col.lower().strip()
        if any(kw in col_lower for kw in ["time", "date", "ts", "timestamp"]):
            return col
    if df.index.name and df.index.name.lower().strip() in ["timestamp_utc", "timestamp", "time", "datetime", "ts"]:
        return "__index__"
    if not isinstance(df.index, pd.RangeIndex):
        try:
            test = pd.to_datetime(df.index[:5], errors="coerce")
            if test.notna().sum() >= 3:
                return "__index__"
        except Exception:
            pass
    for col in df.columns:
        try:
            sample = df[col].dropna().head(10)
            if len(sample) == 0:
                continue
            parsed = pd.to_datetime(sample, errors="coerce")
            if parsed.notna().sum() >= max(1, len(sample) * 0.7):
                return col
        except Exception:
            continue
    return None


def load_csv(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        return df
    except Exception as e:
        print(f"Error loading {filename}: {e}")
        return pd.DataFrame()


def load_csv_with_ts(filename):
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
                df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
            except Exception:
                try:
                    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
                except Exception:
                    pass

        if ts_col is None:
            try:
                df_retry = pd.read_csv(path, index_col=0)
                if not isinstance(df_retry.index, pd.RangeIndex):
                    test_parsed = pd.to_datetime(df_retry.index[:5], errors="coerce")
                    if test_parsed.notna().sum() >= 3:
                        df_retry.index.name = df_retry.index.name or "timestamp_utc"
                        idx_name = df_retry.index.name
                        df_retry = df_retry.reset_index()
                        ts_col = idx_name
                        try:
                            df_retry[ts_col] = pd.to_datetime(df_retry[ts_col], errors="coerce", utc=True)
                        except Exception:
                            df_retry[ts_col] = pd.to_datetime(df_retry[ts_col], errors="coerce")
                        df = df_retry
            except Exception:
                pass

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
    df = df.copy()
    for col in df.columns:
        if df[col].dtype in ["float64", "float32"]:
            df[col] = df[col].astype(object)
            df[col] = df[col].apply(sanitize_val)
        elif df[col].dtype == "datetime64[ns, UTC]" or df[col].dtype == "datetime64[ns]":
            df[col] = df[col].astype(str).replace("NaT", None)
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
            df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], errors="coerce", utc=True)
            start_dt = pd.to_datetime(start_ts, utc=True)
            end_dt = pd.to_datetime(end_ts, utc=True)
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
            dt_times = pd.to_datetime(scores_df.loc[dt_mask, scores_ts])
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

def beta_load_csv(filename):
    path = os.path.join(BETA_DATA_DIR, filename)
    if os.path.exists(path):
        try:
            return pd.read_csv(path)
        except Exception as e:
            print(f"Error loading beta {filename}: {e}")
    return pd.DataFrame()


def beta_load_csv_with_ts(filename):
    path = os.path.join(BETA_DATA_DIR, filename)
    if os.path.exists(path):
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
                    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
                except Exception:
                    try:
                        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
                    except Exception:
                        pass
            if ts_col is None:
                try:
                    df_retry = pd.read_csv(path, index_col=0)
                    if not isinstance(df_retry.index, pd.RangeIndex):
                        test_parsed = pd.to_datetime(df_retry.index[:5], errors="coerce")
                        if test_parsed.notna().sum() >= 3:
                            df_retry.index.name = df_retry.index.name or "timestamp_utc"
                            idx_name = df_retry.index.name
                            df_retry = df_retry.reset_index()
                            ts_col = idx_name
                            try:
                                df_retry[ts_col] = pd.to_datetime(df_retry[ts_col], errors="coerce", utc=True)
                            except Exception:
                                df_retry[ts_col] = pd.to_datetime(df_retry[ts_col], errors="coerce")
                            df = df_retry
                except Exception:
                    pass
            return df, ts_col
        except Exception as e:
            print(f"Error loading beta {filename}: {e}")
    return pd.DataFrame(), None


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

    catalog_df = beta_load_csv("dynamic_catalog.csv")
    if catalog_df.empty:
        return jsonify({"sensors": [], "timeseries": []})

    sys_sensors = catalog_df[catalog_df["system"] == system_id]["sensor"].tolist() if "system" in catalog_df.columns else []
    if not sys_sensors:
        return jsonify({"sensors": [], "timeseries": []})

    # Try detailed files first (new pipeline outputs)
    sqs_df, sqs_ts = beta_load_csv_with_ts("detailed_sqs.csv")
    enga_df, enga_ts = beta_load_csv_with_ts("detailed_engine_a.csv")
    engb_df, engb_ts = beta_load_csv_with_ts("detailed_engine_b.csv")

    # Load downtime info from detailed_subsystem_alarms.csv (scores.csv doesn't exist in beta)
    alarms_df, alarms_ts = beta_load_csv_with_ts("detailed_subsystem_alarms.csv")

    ts_col = sqs_ts or enga_ts or engb_ts or alarms_ts

    # Build per-sensor data using the double-underscore naming convention: {SYS}__{sensor}
    prefix = f"{system_id}__"
    result_sensors = []
    timeseries = []

    # Determine which source has data
    has_detailed = not sqs_df.empty or not enga_df.empty or not engb_df.empty

    if has_detailed:
        # Use the longest df for timestamps
        ref_df = sqs_df if not sqs_df.empty else (enga_df if not enga_df.empty else engb_df)
        ref_ts = sqs_ts or enga_ts or engb_ts

        # Find sensor columns matching this system
        for sensor in sys_sensors:
            col_key = f"{prefix}{sensor}"
            found = False
            for df_check in [sqs_df, enga_df, engb_df]:
                if col_key in df_check.columns:
                    found = True
                    break
            if found:
                result_sensors.append(sensor)

        if not result_sensors:
            # Fallback: try columns without system prefix
            for sensor in sys_sensors:
                for df_check in [sqs_df, enga_df, engb_df]:
                    if sensor in df_check.columns:
                        result_sensors.append(sensor)
                        break

        # Downsample
        n = len(ref_df)
        indices = list(range(0, n, max(1, downsample)))

        for i in indices:
            row = {}
            if ref_ts and ref_ts in ref_df.columns:
                row["ts"] = str(ref_df[ref_ts].iloc[i])

            # Downtime flag from alarms file
            if not alarms_df.empty and "downtime_flag" in alarms_df.columns and i < len(alarms_df):
                val = alarms_df["downtime_flag"].iloc[i]
                row["downtime"] = int(val) if not (isinstance(val, float) and math.isnan(val)) else 0

            for sensor in result_sensors:
                col_prefixed = f"{prefix}{sensor}"
                col_plain = sensor

                # SQS
                for col_try in [col_prefixed, col_plain]:
                    if not sqs_df.empty and col_try in sqs_df.columns and i < len(sqs_df):
                        v = sqs_df[col_try].iloc[i]
                        row[f"{sensor}__sqs"] = None if (isinstance(v, float) and math.isnan(v)) else v
                        break

                # Engine A
                for col_try in [col_prefixed, col_plain]:
                    if not enga_df.empty and col_try in enga_df.columns and i < len(enga_df):
                        v = enga_df[col_try].iloc[i]
                        row[f"{sensor}__a"] = None if (isinstance(v, float) and math.isnan(v)) else v
                        break

                # Engine B
                for col_try in [col_prefixed, col_plain]:
                    if not engb_df.empty and col_try in engb_df.columns and i < len(engb_df):
                        v = engb_df[col_try].iloc[i]
                        row[f"{sensor}__b"] = None if (isinstance(v, float) and math.isnan(v)) else v
                        break

            timeseries.append(row)
    else:
        # Fallback: use SQS from main scores + Engine A/B from combined scores
        # This path works with the old pipeline data
        result_sensors = sys_sensors

    # Downtime bands from detailed_subsystem_alarms.csv
    downtime_bands = []
    if not alarms_df.empty and "downtime_flag" in alarms_df.columns and alarms_ts:
        dt_mask = alarms_df["downtime_flag"] == 1
        if dt_mask.any():
            dt_times = pd.to_datetime(alarms_df.loc[dt_mask, alarms_ts])
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

    # Subsystem-level scores from detailed_subsystem_scores.csv
    sub_df, sub_ts = beta_load_csv_with_ts("detailed_subsystem_scores.csv")
    subsystem_timeseries = []
    score_col = f"{system_id}__System_Score"
    thresh_col = f"{system_id}__Adaptive_Threshold"
    has_subsystem = not sub_df.empty and sub_ts and score_col in sub_df.columns
    if has_subsystem:
        n_sub = len(sub_df)
        indices_sub = list(range(0, n_sub, max(1, downsample)))
        for i in indices_sub:
            row = {"ts": str(sub_df[sub_ts].iloc[i])}
            if "downtime_flag" in sub_df.columns:
                val = sub_df["downtime_flag"].iloc[i]
                row["downtime"] = int(val) if not (isinstance(val, float) and math.isnan(val)) else 0
            v = sub_df[score_col].iloc[i]
            row["system_score"] = None if (isinstance(v, float) and math.isnan(v)) else round(float(v), 6)
            if thresh_col in sub_df.columns:
                t = sub_df[thresh_col].iloc[i]
                row["adaptive_threshold"] = None if (isinstance(t, float) and math.isnan(t)) else round(float(t), 6)
            subsystem_timeseries.append(row)

    return jsonify({
        "sensors": result_sensors,
        "timeseries": timeseries,
        "subsystem_timeseries": subsystem_timeseries,
        "timestamp_col": "ts",
        "downtime_bands": downtime_bands,
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
    """Return live sensor data (from standard data/) plus downtime and alarm bands from data_beta/.

    Uses the same raw sensor data as the standard /api/systems/<system_id>/sensors endpoint
    (df_chart_data.csv or sensor_values_{system_id}.csv from DATA_DIR), but overlays
    downtime and alarm bands from data_beta/detailed_subsystem_alarms.csv.
    Only includes High (Alarm=1) alarm bands.
    """
    downsample = request.args.get("downsample", default=1, type=int)

    # --- Live sensor data from standard data/ directory ---
    catalog_df = load_csv("dynamic_catalog.csv")
    chart_df, ts_col = load_csv_with_ts("df_chart_data.csv")
    sensor_df = pd.DataFrame()

    if not chart_df.empty and not catalog_df.empty:
        sys_sensors = catalog_df[catalog_df["system"] == system_id]["sensor"].tolist() if "system" in catalog_df.columns else []
        keep_cols = [ts_col] + [s for s in sys_sensors if s in chart_df.columns] if ts_col else [s for s in sys_sensors if s in chart_df.columns]
        sensor_df = chart_df[keep_cols]
    else:
        fname = f"sensor_values_{system_id}.csv"
        sensor_df, ts_col = load_csv_with_ts(fname)

    if sensor_df.empty:
        return jsonify({"timeseries": [], "sensors": [], "timestamp_col": "", "downtime_bands": [], "alarm_bands": []})

    sensor_df = sensor_df.iloc[::max(1, downsample)]
    sensor_df = sanitize_df(sensor_df)
    if ts_col and ts_col in sensor_df.columns:
        sensor_df[ts_col] = sensor_df[ts_col].astype(str)
    sensor_cols = [c for c in sensor_df.columns if c != ts_col]
    timeseries = sensor_df.to_dict(orient="records")

    # --- Downtime and alarm bands from data_beta/ ---
    alarms_df, alarms_ts = beta_load_csv_with_ts("detailed_subsystem_alarms.csv")

    # Downtime bands
    downtime_bands = []
    if not alarms_df.empty and "downtime_flag" in alarms_df.columns and alarms_ts:
        dt_mask = alarms_df["downtime_flag"] == 1
        if dt_mask.any():
            dt_times = pd.to_datetime(alarms_df.loc[dt_mask, alarms_ts])
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

    # Alarm bands: only where {system_id}__Alarm == 1 (High alarms)
    alarm_bands = []
    if not alarms_df.empty and alarms_ts:
        alarm_col = f"{system_id}__Alarm"
        level_col = f"{system_id}__Score_Level_At_Alarm"
        if alarm_col in alarms_df.columns:
            alarm_mask = alarms_df[alarm_col] == 1
            if alarm_mask.any():
                alarm_rows = alarms_df[alarm_mask].copy()
                alarm_rows["_ts"] = pd.to_datetime(alarm_rows[alarms_ts])
                alarm_rows = alarm_rows.sort_values("_ts").reset_index(drop=True)
                merge_gap = pd.Timedelta(minutes=15)
                span_start = alarm_rows["_ts"].iloc[0]
                span_end = alarm_rows["_ts"].iloc[0]
                span_level = alarm_rows[level_col].iloc[0] if level_col in alarm_rows.columns else "High"
                for j in range(1, len(alarm_rows)):
                    t = alarm_rows["_ts"].iloc[j]
                    lv = alarm_rows[level_col].iloc[j] if level_col in alarm_rows.columns else "High"
                    if t - span_end <= merge_gap:
                        span_end = t
                        sev_order = {"Low": 0, "Medium": 1, "High": 2}
                        if sev_order.get(str(lv), 0) > sev_order.get(str(span_level), 0):
                            span_level = lv
                    else:
                        alarm_bands.append({"start": str(span_start), "end": str(span_end), "severity": str(span_level)})
                        span_start = t
                        span_end = t
                        span_level = lv
                alarm_bands.append({"start": str(span_start), "end": str(span_end), "severity": str(span_level)})

    return jsonify({
        "timeseries": timeseries,
        "timestamp_col": ts_col,
        "sensors": sensor_cols,
        "downtime_bands": downtime_bands,
        "alarm_bands": alarm_bands,
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
    df = beta_load_csv("alerts.csv")
    if df.empty:
        return jsonify({"alerts": [], "summary": {}})
    if "class" in df.columns:
        df = df[df["class"] != "NORMAL"]
    df = sanitize_df(df)
    high_count = len(df[df["severity"] == "HIGH"]) if "severity" in df.columns else 0
    medium_count = len(df[df["severity"] == "MEDIUM"]) if "severity" in df.columns else 0
    class_dist = df["class"].value_counts().to_dict() if "class" in df.columns else {}
    summary = {
        "total_alerts": len(df),
        "high_count": int(high_count),
        "medium_count": int(medium_count),
        "class_distribution": class_dist,
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
    alerts_df = beta_load_csv("alerts.csv")
    scores_df, ts_col = beta_load_csv_with_ts("detailed_subsystem_scores.csv")
    catalog_df = beta_load_csv("dynamic_catalog.csv")

    summary = {
        "total_alerts": 0, "high_alerts": 0, "medium_alerts": 0,
        "total_sensors": 0, "class_distribution": {},
        "avg_risk_score": None, "max_risk_score": None, "current_risk_score": None,
        "avg_sqs": None, "running_pct": None,
        "data_range": {"start": None, "end": None},
    }

    if not alerts_df.empty:
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


@app.route("/api/beta/alerts_sensor_level", methods=["GET"])
def beta_alerts_sensor_level():
    df = beta_load_csv("alerts_sensor_level.csv")
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
            df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], errors="coerce", utc=True)
            start_dt = pd.to_datetime(start_ts, utc=True)
            end_dt = pd.to_datetime(end_ts, utc=True)
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

    backend_host = os.environ.get("HOST", "0.0.0.0")
    backend_port = int(os.environ.get("PORT", "5000"))
    app.run(debug=True, host=backend_host, port=backend_port)
