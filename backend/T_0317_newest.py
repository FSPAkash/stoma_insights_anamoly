# %%
# =============================================================================
# Cell 1: Imports and Constants
# =============================================================================
from __future__ import annotations

import os
import warnings
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

import torch
import torch.nn as nn
import torch.optim as optim

from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from scipy.cluster.hierarchy import linkage, fcluster, dendrogram
from scipy.spatial.distance import squareform

warnings.filterwarnings("ignore")

EPS = 1e-9
torch.manual_seed(42)
np.random.seed(42)
import time
print("✅ Imports complete.")

# %%
# =============================================================================
# Cell 2: Pipeline Configuration (ALL config at top)
# =============================================================================
@dataclass
class PipelineConfig:
    # --- Source ---
    parquet_path: str = (
        r"data_analysis_data_bq-results-iot-staging-shredder-sensor-2026_02_01-2026_02_07.parquet"
    )
    long_ts_col: str = "START_T"
    long_sensor_col: str = "SENSOR_ID"
    long_value_col: str = "QUANTITY_VALUE_D"

    # --- Time grid ---
    freq: str = "1min"

    # --- Stable discovery ---
    n_stable_min: int = 300

    # --- Engine A (drift) ---
    engineA_missing_max: float = 0.15
    engineA_sqs_min: float = 0.60
    engineA_baseline_win: int = 120
    engineA_mad_win: int = 240
    engineA_score_k: float = 3.0

    # --- Engine B (periodicity) ---
    engineB_sqs_min: float = 0.70
    engineB_win: int = 120
    engineB_valid_frac_min: float = 0.80
    engineB_period_min: int = 5
    engineB_period_max: int = 60

    # --- AutoEncoder Subsystem Scoring ---
    ae_latent_dim: int = 3
    ae_epochs: int = 300
    ae_lr: float = 0.01
    ae_batch_size: int = 256
    ae_min_sensors: int = 2
    ae_min_training_rows: int = 300
    ae_missing_max: float = 0.10
    ae_mean_abs_corr_min: float = 0.05
    ae_risk_sigma_factor: float = 3.0
    ae_slew_rate: float = 0.0005
    ae_gate_factor: float = 1.5
    ae_threshold_window: int = 100
    ae_on_delay: int = 3
    ae_off_delay: int = 5

    # --- Block scoring ---
    block_score_topk: int = 3

    # --- Alert thresholds ---
    high: float = 0.80
    medium: float = 0.55

    # --- Episode controls ---
    alert_min_duration_min: int = 5
    alert_merge_gap_min: int = 3

    # --- Dynamic system discovery ---
    sys_null_threshold_pct: float = 30.0
    sys_corr_method: str = "spearman"
    sys_cluster_method: str = "complete"
    sys_threshold_auto: bool = True
    sys_threshold_manual: float = 0.80
    sys_threshold_max: float = 1.0
    sys_min_system_size: int = 2
    sys_r2_adj_min_quality: float = 0.50
    sys_ffill_limit: int = 5

    # --- Output ---
    output_dir: str = f"pipeline_outputs_PLAY_{time.strftime('%Y%m%d_%H%M%S')}"
    BASELINE_FILE: str = "sensor_baseline_range_season_2024-2025.xlsx"

    #trust thresholds 
    trust_sqs_unusable: float = 0.60
    trust_sqs_reliable: float = 0.80
    trust_drift_reliable: float = 0.50
    trust_period_reliable: float = 0.50


# --- Directional Anomaly Configuration ---
ANOMALY_DIRECTION_CONFIG: Dict[str, str] = {
    # Mechanical vibration (fault = vibration increase)
    "VT5449011H": "pos",
    "VT5449012H": "pos",
    "VT5449012A": "pos",
    "VT5449013H": "pos",
    "VT5449016H": "pos",
    "VT5449017H": "pos",
    "VT5449017A": "pos",
    "VT5449018H": "pos",

    # Thermal temperature sensors (fault = temperature rise)
    "TT5449012H": "pos",
    "TT5449017H": "pos",
    "TT5449018H": "pos",
    "DESF_TA__RTD_1": "pos",
    "DESF_TA__RTD_2": "pos",
    "DESF_TA__RTD_3": "pos",
    "DESF_TA__RTD_4": "pos",
    "DESF_TA__RTD_5": "pos",
    "DESF_TA__RTD_6": "pos",
    "DESF_TA__RTD_7": "pos",
    "DESF_TA__RTD_8": "pos",

    # Electrical currents
    "DESF_TA__J1_IA": "both",
    "DESF_TA__J1_IB": "both",
    "DESF_TA__J1_IC": "both",
    "DESF_TA__J1_I_AVG": "both",

    # Electrical voltages (LN)
    "DESF_TA__J2_VAN": "both",
    "DESF_TA__J2_VBN": "both",
    "DESF_TA__J2_VCN": "both",
    "DESF_TA__J2_V_AVG_LN": "both",

    # Electrical voltages (LL)
    "DESF_TA__J2_VAB": "both",
    "DESF_TA__J2_VBC": "both",
    "DESF_TA__J2_VCA": "both",
    "DESF_TA__J2_V_AVG_LL": "both",

    # Frequency
    "DESF_TA__FREC": "both",

    # Total power / energy
    "DESF_TA__KW_TOT": "both",
    "DESF_TA__KVAR_TOT": "both",
    "DESF_TA__KVA_TOT": "both",
    "DESF_TA__PF": "both",
    "DESF_TA__MWH_DEL": "both",

    # Phase powers
    "DESF_TA__KW_A": "both",
    "DESF_TA__KW_B": "both",
    "DESF_TA__KW_C": "both",
    "DESF_TA__KVAR_A": "both",
    "DESF_TA__KVAR_B": "both",
    "DESF_TA__KVAR_C": "both",
    "DESF_TA__KVA_A": "both",
    "DESF_TA__KVA_B": "both",
    "DESF_TA__KVA_C": "both",
    "DESF_TA__PF_A": "both",
    "DESF_TA__PF_B": "both",
    "DESF_TA__PF_C": "both",
}

# Electrical Column Constants (downtime detection only)
COL_KW   = "DESF_TA__KW_TOT"
COL_PF   = "DESF_TA__PF"
COL_VLL  = "DESF_TA__J2_V_AVG_LL"
COL_IAVG = "DESF_TA__J1_I_AVG"
COL_FREC = "DESF_TA__FREC"
DOWNTIME_COLS = [COL_KW, COL_IAVG, COL_VLL, COL_FREC]

cfg = PipelineConfig()
os.makedirs(cfg.output_dir, exist_ok=True) 
print("✅ Configuration loaded.")
print(f"   Parquet path: {cfg.parquet_path}")
print(f"   Directional config sensors: {len(ANOMALY_DIRECTION_CONFIG)}")


# %%
# =============================================================================
# Cell 3: Read Parquet + Pivot + Resample
# =============================================================================
def read_parquet_long_to_wide(cfg: PipelineConfig) -> pd.DataFrame:
    print(f"📂 Reading parquet: {cfg.parquet_path}")
    raw = pd.read_parquet(cfg.parquet_path)
    print(f"   Raw rows: {len(raw):,}")
    print(f"   Unique sensors: {raw[cfg.long_sensor_col].nunique()}")
    print(f"   Time range: {raw[cfg.long_ts_col].min()} → {raw[cfg.long_ts_col].max()}")

    raw[cfg.long_ts_col] = pd.to_datetime(raw[cfg.long_ts_col], utc=True)
    raw = raw.dropna(subset=[cfg.long_ts_col, cfg.long_sensor_col])
    raw[cfg.long_value_col] = pd.to_numeric(raw[cfg.long_value_col], errors="coerce")

    wide = raw.pivot_table(
        index=cfg.long_ts_col,
        columns=cfg.long_sensor_col,
        values=cfg.long_value_col,
        aggfunc="mean",
    ).sort_index()
    wide.index.name = None
    print(f"   Wide shape after pivot: {wide.shape}")
    return wide


def detect_frequency(df: pd.DataFrame) -> pd.Timedelta:
    diffs = pd.Series(df.index).diff().dropna()
    diffs = diffs[diffs > pd.Timedelta(0)]
    if diffs.empty:
        return pd.Timedelta("1min")
    diffs_rounded = diffs.dt.round("1s")
    diffs_rounded = diffs_rounded[diffs_rounded > pd.Timedelta(0)]
    if diffs_rounded.empty:
        return pd.Timedelta("1min")
    mode_freq = diffs_rounded.mode()
    detected = mode_freq.iloc[0] if len(mode_freq) > 0 else pd.Timedelta("1min")
    print(f"   Detected native frequency: {detected}")
    return detected


def clean_resample_mean(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.replace(0.0, np.nan)
    return df_clean.resample("1min").mean()


def resample_to_minute_grid(df: pd.DataFrame) -> pd.DataFrame:
    native_freq = detect_frequency(df)
    target = pd.Timedelta("1min")
    if native_freq < target:
        print(f"   ↓ Downsampling from ~{native_freq} to 1min")
        df_resampled = clean_resample_mean(df)
    elif native_freq > target:
        print(f"   ↑ Upsampling from ~{native_freq} to 1min (ffill limit=1)")
        df_resampled = df.resample("1min").ffill(limit=1)
    else:
        df_resampled = clean_resample_mean(df)

    full_idx = pd.date_range(
        df_resampled.index.min(), df_resampled.index.max(),
        freq="1min", tz=df_resampled.index.tz,
    )
    df_resampled = df_resampled.reindex(full_idx)
    print(f"   Final grid: {df_resampled.shape[0]:,} minutes "
          f"({df_resampled.index.min()} → {df_resampled.index.max()})")
    return df_resampled


raw_wide = read_parquet_long_to_wide(cfg)
df = resample_to_minute_grid(raw_wide)
print(f"✅ Minute-grid data: {df.shape[0]:,} rows × {df.shape[1]} columns")

# %%
# =============================================================================
# Cell 4: Base Invalid Mask + Bad Cell Nullification
# =============================================================================
def base_invalid_mask(df: pd.DataFrame) -> pd.DataFrame:
    """Domain-knowledge physical impossibility checks."""
    bad = df.isna().copy()

    for c in [x for x in df.columns if "J2_V" in x]:
        bad[c] = bad[c] | (df[c] <= 0)
    for c in [x for x in df.columns if "FREC" in x]:
        bad[c] = bad[c] | (df[c] < 40) | (df[c] > 70)
    for c in [x for x in df.columns if "PF" in x.upper()]:
        bad[c] = bad[c] | (df[c] <= 0)
    for c in [x for x in df.columns if "_I" in x and "STATUS" not in x]:
        bad[c] = bad[c] | (df[c] <= 0)
    for c in [x for x in df.columns if any(k in c for k in ["KW_", "KVA_", "KVAR_", "MWH"])]:
        bad[c] = bad[c] | (df[c] < 0)
    for c in [x for x in df.columns if "VT" in x]:
        bad[c] = bad[c] | (df[c] <= 0)
    for c in [x for x in df.columns if any(k in x for k in ["TT5449", "RTD_"])]:
        bad[c] = bad[c] | (df[c] < -10) | (df[c] > 200)
    return bad


def apply_bad_mask_nullify_cells(
    df: pd.DataFrame, bad: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    n_nullified = 0
    df_clean = df.copy()
    for c in df_clean.columns:
        if c in bad.columns:
            mask = bad[c] & df_clean[c].notna()
            n = mask.sum()
            if n > 0:
                df_clean.loc[mask, c] = np.nan
                n_nullified += n
    print(f"   Bad-flagged cells set to NaN: {n_nullified:,}")
    return df_clean, bad


base_bad = base_invalid_mask(df)
df, base_bad = apply_bad_mask_nullify_cells(df, base_bad)
print(f"✅ Base invalid mask applied. df shape: {df.shape}")

# %%
# =============================================================================
# Cell 4.5: Initial Sensor Validation (Structural Quality Gate)
# =============================================================================
def initial_sensor_validation(
    df: pd.DataFrame,
    missing_threshold: float = 0.4,
    std_floor: float = 1e-8,
    unique_ratio_floor: float = 0.01,
    lookback: pd.Timedelta = pd.Timedelta("1D"),
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Detect structural data issues early before modeling.

    Uses the latest 1-day window (stable window) to compute thresholds.

    Checks
    ------
    1. Missingness:        missing_ratio > missing_threshold  → sensor has <60% coverage
    2. Constant Signal:    std < std_floor                    → signal flat, no information
    3. Near-Constant:      unique_ratio < unique_ratio_floor  → <1% unique values

    Returns
    -------
    df_valid : DataFrame with failed sensors removed
    report   : DataFrame summarising every sensor's check results
    """
    print("🔍 Initial Sensor Validation (structural quality gate)")
    print(f"   Lookup window: latest timestamp − {lookback}")
    print(f"   Thresholds → missing_ratio ≤ {missing_threshold}, "
          f"std ≥ {std_floor}, unique_ratio ≥ {unique_ratio_floor}")

    # --- Define the stable lookup window (last 1 day) -------------------
    latest_ts = df.index.max()
    window_start = latest_ts - lookback
    df_window = df.loc[window_start:latest_ts]
    n_rows = len(df_window)
    print(f"   Window: {window_start} → {latest_ts}  ({n_rows:,} rows)")

    if n_rows == 0:
        print("   ⚠️  Lookup window is empty – falling back to full dataset")
        df_window = df
        n_rows = len(df_window)

    # --- Compute per-sensor metrics ------------------------------------
    records = []
    for col in df.columns:
        series = df_window[col]
        n_total = len(series)
        n_missing = int(series.isna().sum())
        missing_ratio = n_missing / n_total if n_total > 0 else 1.0

        non_null = series.dropna()
        std_val = float(non_null.std()) if len(non_null) > 1 else 0.0
        n_unique = int(non_null.nunique())
        unique_ratio = n_unique / len(non_null) if len(non_null) > 0 else 0.0

        # --- Determine failure reasons --------------------------------
        reasons = []
        if missing_ratio > missing_threshold:
            reasons.append(
                f"HIGH_MISSINGNESS (missing_ratio={missing_ratio:.3f} > {missing_threshold})"
            )
        if std_val < std_floor:
            reasons.append(
                f"CONSTANT_SIGNAL (std={std_val:.2e} < {std_floor})"
            )
        if unique_ratio < unique_ratio_floor and len(non_null) > 0:
            reasons.append(
                f"NEAR_CONSTANT (unique_ratio={unique_ratio:.4f} < {unique_ratio_floor})"
            )

        passed = len(reasons) == 0
        records.append({
            "sensor": col,
            "n_total": n_total,
            "n_missing": n_missing,
            "missing_ratio": round(missing_ratio, 4),
            "std": round(std_val, 8),
            "n_unique": n_unique,
            "unique_ratio": round(unique_ratio, 4),
            "passed": passed,
            "removal_reasons": " | ".join(reasons) if reasons else "",
        })

    report = pd.DataFrame(records).set_index("sensor")

    # --- Report ---------------------------------------------------------
    failed = report[~report["passed"]]
    passed_sensors = report[report["passed"]]

    print(f"\n   ✅ Sensors PASSED : {len(passed_sensors)}")
    print(f"   ❌ Sensors FAILED : {len(failed)}")

    if len(failed) > 0:
        print("\n   ┌─── Removed Sensors ─────────────────────────────────────────")
        for sensor, row in failed.iterrows():
            print(f"   │  ✗ {sensor}")
            print(f"   │      Reason(s): {row['removal_reasons']}")
            print(f"   │      [missing={row['missing_ratio']:.1%}, "
                  f"std={row['std']:.2e}, "
                  f"unique_ratio={row['unique_ratio']:.4f}, "
                  f"n_unique={row['n_unique']}]")
        print(f"   └──────────────────────────────────────────────────────────────")

    # --- Drop failed sensors from the DataFrame -------------------------
    keep_cols = passed_sensors.index.tolist()
    df_valid = df[keep_cols].copy()

    print(f"\n   DataFrame: {df.shape[1]} cols → {df_valid.shape[1]} cols "
          f"(removed {df.shape[1] - df_valid.shape[1]})")
    print("✅ Initial sensor validation complete.")
    return df_valid, report


df, sensor_validation_report = initial_sensor_validation(
    df,
    missing_threshold=0.4,
    std_floor=1e-8,
    unique_ratio_floor=0.01,
    lookback=pd.Timedelta("1D"),
)
sensor_validation_report.to_csv(os.path.join(cfg.output_dir, "initial_sensor_validation_report.csv"))
# %%
# =============================================================================
# Cell 5: Downtime Detection (Electrical Signals Only)
# =============================================================================
def build_downtime_mask(
    df: pd.DataFrame,
    col_kw: str, col_iavg: str, col_vll: str, col_frec: str,
) -> pd.DataFrame:
    def qlow(s, q, floor):
        pos = s[s > 0].dropna()
        if len(pos) > 100:
            v = float(np.nanquantile(pos.values, q))
            if np.isfinite(v):
                return max(v, floor)
        return floor

    result = pd.DataFrame(index=df.index)
    kw_off  = qlow(df[col_kw], 0.02, 0.5) if col_kw in df.columns else 0.5
    i_off   = qlow(df[col_iavg], 0.02, 1.0) if col_iavg in df.columns else 1.0
    vll_off = qlow(df[col_vll], 0.02, 10.0) if col_vll in df.columns else 10.0
    f_off   = qlow(df[col_frec], 0.02, 1.0) if col_frec in df.columns else 1.0

    print(f"   OFF thresholds: kW≤{kw_off:.2f}, I≤{i_off:.2f}, "
          f"VLL≤{vll_off:.2f}, Frec≤{f_off:.2f}")

    result["kw_off"]   = (df[col_kw] <= kw_off) | df[col_kw].isna() if col_kw in df.columns else True
    result["i_off"]    = (df[col_iavg] <= i_off) | df[col_iavg].isna() if col_iavg in df.columns else True
    result["vll_off"]  = (df[col_vll] <= vll_off) | df[col_vll].isna() if col_vll in df.columns else True
    result["frec_off"] = (df[col_frec] <= f_off) | df[col_frec].isna() if col_frec in df.columns else True
    return result


def global_downtime(off_mask: pd.DataFrame) -> pd.Series:
    return off_mask.all(axis=1) | (off_mask["kw_off"] & off_mask["i_off"])


downtime_signals = build_downtime_mask(df, COL_KW, COL_IAVG, COL_VLL, COL_FREC)
downtime = global_downtime(downtime_signals)
running = ~downtime

mode = pd.Series("RUNNING", index=df.index)
mode.loc[downtime] = "DOWNTIME"

print(f"   Downtime: {downtime.sum():,} / {len(downtime):,} ({downtime.mean():.1%})")
print(f"   Running:  {running.sum():,} ({running.mean():.1%})")
print("✅ Downtime detection complete.")

# %%
# =============================================================================
# Cell 6: Load Baseline + Map Sensors (NO computation)
# =============================================================================
BASELINE_FILE = cfg.BASELINE_FILE


def load_sensor_cfg_with_logging(df, cfg):

    if os.path.exists(BASELINE_FILE):
        print(f"📂 Loading baseline: {BASELINE_FILE}")
        baseline = pd.read_excel(BASELINE_FILE)
    else:
        raise FileNotFoundError(f"Baseline file not found: {BASELINE_FILE}")

    baseline["sensor_key"] = baseline["sensor"].str.strip().str.lower()
    baseline_lookup = baseline.set_index("sensor_key")

    rows = []
    sensors_from_baseline = []
    sensors_default = []

    for s in df.columns:
        key = s.strip().lower()

        if key in baseline_lookup.index:
            row = baseline_lookup.loc[key]

            rows.append({
                "sensor": s,
                "p0_1": row["p0_1"],
                "p99_9": row["p99_9"],
                "roc_p99_9": row["roc_p99_9"],
            })

            sensors_from_baseline.append(s)

        else:
            rows.append({
                "sensor": s,
                "p0_1": np.nan,
                "p99_9": np.nan,
                "roc_p99_9": np.nan,
            })

            sensors_default.append(s)

    sensor_cfg = pd.DataFrame(rows)

    print("\n================ SENSOR BASELINE SUMMARY ================")
    print(f"Total sensors in df: {len(df.columns)}")
    print(f"Using baseline:     {len(sensors_from_baseline)}")
    print(f"Default score=1:    {len(sensors_default)}")

    if sensors_from_baseline:
        print("\n📌 Sensors loaded from baseline:")
        print(", ".join(sensors_from_baseline))

    if sensors_default:
        print("\n⚪ Sensors not in baseline (score always = 1):")
        print(", ".join(sensors_default))

    print("=========================================================\n")

    return sensor_cfg


sensor_cfg = load_sensor_cfg_with_logging(df, cfg)
print(f"✅ Sensor config ready: {len(sensor_cfg)} sensors")

# =============================================================================
# Cell 7: SQS — Simplified (Baseline sensors only)
# =============================================================================
def compute_sqs(df: pd.DataFrame, sensor_cfg: pd.DataFrame,
                running_mask: pd.Series) -> pd.DataFrame:

    cfg_idx = sensor_cfg.set_index("sensor")

    sqs = pd.DataFrame(
        np.nan,
        index=df.index,
        columns=df.columns,
        dtype=float
    )

    for s in df.columns:

        x = df[s]

        score = pd.Series(np.nan, index=df.index, dtype=float)

        present = x.notna() & running_mask

        # Default score = 1
        score.loc[present] = 1.0

        # If sensor not in baseline → keep score = 1
        if s not in cfg_idx.index:
            sqs[s] = score
            continue

        row = cfg_idx.loc[s]

        p0 = row["p0_1"]
        p9 = row["p99_9"]
        roc = row["roc_p99_9"]

        # Apply bounds only if baseline exists
        if np.isfinite(p0) and np.isfinite(p9):

            margin = (p9 - p0) * 0.01

            oob = present & (
                (x < p0 - margin) |
                (x > p9 + margin)
            )

            score.loc[oob] *= 0.60

        if np.isfinite(roc):

            jump = present & (
                x.diff().abs() > (roc * 1.25)
            )

            score.loc[jump] *= 0.70

        sqs[s] = score

    return sqs.clip(0, 1)


def summarize_sqs(sqs: pd.DataFrame) -> pd.DataFrame:

    arr = sqs.values

    n = arr.shape[0]

    sqs_mean = np.full(n, 0.0)
    sqs_p10 = np.full(n, 0.0)
    sqs_valid_frac = np.full(n, 0.0)

    for i in range(n):

        present = arr[i, :][np.isfinite(arr[i, :])]

        if len(present) > 0:
            sqs_mean[i] = np.mean(present)
            sqs_p10[i] = np.quantile(present, 0.10)
            sqs_valid_frac[i] = np.mean(present > 0)

    return pd.DataFrame({
        "sqs_mean": sqs_mean,
        "sqs_p10": sqs_p10,
        "sqs_valid_frac": sqs_valid_frac
    }, index=sqs.index)


sqs = compute_sqs(df, sensor_cfg, running)

sqs_summary = summarize_sqs(sqs)

print(f"SQS shape: {sqs.shape}")
print(f"SQS global mean (running): {sqs_summary.loc[running, 'sqs_mean'].mean():.4f}")
print("✅ SQS complete (baseline sensors only).")

# %%

# =============================================================================
# Cell 8: Engine A — Drift Detection (RUNNING only, all df columns)
# =============================================================================
def rolling_median(s, win):
    return s.rolling(win, min_periods=max(30, win // 5)).median()


def rolling_mad(resid, win):
    def _mad(a):
        a = a[np.isfinite(a)]
        if len(a) < 50:
            return np.nan
        med = np.median(a)
        return 1.4826 * np.median(np.abs(a - med))
    return resid.rolling(win, min_periods=max(50, win // 5)).apply(_mad, raw=True)


def get_all_sensors_from_df(df) -> List[str]:
    """Get all columns from df as sensor list."""
    return list(df.columns)


def compute_engineA(df, sqs, running_mask, sensors, baseline_win, mad_win, sqs_min, k):
    """Engine A on RUNNING data only. Downtime rows get NaN."""
    A = pd.DataFrame(np.nan, index=df.index, columns=sensors, dtype=float)
    cache = {}
    for s in sensors:
        if s not in df.columns:
            continue
        x = df[s]
        if s in sqs.columns:
            valid = running_mask & (sqs[s] >= sqs_min) & x.notna()
        else:
            valid = running_mask & x.notna()
        x_use = x.where(valid)
        base = rolling_median(x_use, baseline_win)
        resid = x_use - base
        mad = rolling_mad(resid, mad_win)
        z = resid.abs() / (mad + EPS)
        score = 1.0 - np.exp(-z / max(k, EPS))
        # Zero out downtime
        score.loc[~running_mask] = np.nan
        A[s] = score
        cache[s] = (base, mad)
    return A, cache


print("--- Engine A (all df columns, RUNNING only) ---")
A_sensors = get_all_sensors_from_df(df)
print(f"   All sensors for Engine A: {len(A_sensors)}")

A, baseline_cache = compute_engineA(
    df, sqs, running, A_sensors,
    cfg.engineA_baseline_win, cfg.engineA_mad_win, cfg.engineA_sqs_min, cfg.engineA_score_k,
)
print(f"   Engine A shape: {A.shape}")
print(f"   Engine A mean score (running): {A.loc[running].mean().mean():.4f}")
print("✅ Engine A complete (downtime excluded, all df columns).")

# %%
# =============================================================================
# Cell 9: Engine B — Periodicity Detection (RUNNING only, all df columns)
# =============================================================================

def spectral_energy_ratio(u_window, period_min, period_max):
    u = np.asarray(u_window, dtype=float)
    u = u[np.isfinite(u)]
    if len(u) < 60:
        return np.nan
    u = u - np.mean(u)
    w = np.hanning(len(u))
    fft_vals = np.fft.rfft(u * w)
    P = np.abs(fft_vals) ** 2
    freqs = np.fft.rfftfreq(len(u), d=1.0)
    band = (freqs >= 1.0 / period_max) & (freqs <= 1.0 / period_min)
    return float(np.nansum(P[band]) / (np.nansum(P) + EPS))


def learn_engineB_thresholds(u_running, win, period_min, period_max):
    arr = u_running.dropna().values
    if len(arr) < win * 3:
        return 0.15, 0.40
    ratios = []
    step = max(10, win // 2)
    for i in range(win, len(arr), step):
        r = spectral_energy_ratio(arr[i - win:i], period_min, period_max)
        if np.isfinite(r):
            ratios.append(r)
    if len(ratios) < 20:
        return 0.15, 0.40
    e_low = float(np.nanquantile(ratios, 0.90))
    e_high = float(np.nanquantile(ratios, 0.99))
    if e_high - e_low < 0.05:
        e_low = max(0.05, e_low - 0.05)
        e_high = min(0.95, e_high + 0.05)
    return e_low, e_high


def select_engineB_sensors(df, running_mask, sqs, sqs_min, all_sensors, max_sensors=50):
    """Select sensors for Engine B from all df columns."""
    eligible = []
    for s in all_sensors:
        if s not in df.columns:
            continue
        if s in sqs.columns:
            ok = running_mask & df[s].notna() & (sqs[s] >= sqs_min)
        else:
            ok = running_mask & df[s].notna()
        if ok.sum() < 200:
            continue
        if df.loc[running_mask, s].var(skipna=True) <= 0:
            continue
        eligible.append(s)
    return eligible[:max_sensors]


def compute_engineB(
    df, sqs, running_mask, sensors, baseline_cache,
    win, sqs_min, valid_frac_min, period_min, period_max,
):
    """Engine B on RUNNING data only. Downtime rows stay NaN."""
    B = pd.DataFrame(np.nan, index=df.index, columns=sensors, dtype=float)
    thresholds = {}
    for s in sensors:
        if s not in baseline_cache:
            continue
        base, mad = baseline_cache[s]
        if base is None or mad is None:
            continue
        u_running = ((df[s] - base) / (mad + EPS)).loc[running_mask]
        thresholds[s] = learn_engineB_thresholds(u_running, win, period_min, period_max)

    for s in sensors:
        if s not in thresholds or s not in baseline_cache:
            continue
        base, mad = baseline_cache[s]
        u = (df[s] - base) / (mad + EPS)
        e_low, e_high = thresholds[s]
        has_sqs = s in sqs.columns
        for t in range(win, len(df)):
            if not running_mask.iloc[t]:
                continue
            w_arr = u.iloc[t - win:t].values
            if has_sqs:
                sq = sqs[s].iloc[t - win:t].values
                valid = np.isfinite(w_arr) & (sq >= sqs_min)
            else:
                valid = np.isfinite(w_arr)
            if valid.mean() < valid_frac_min:
                continue
            ratio = spectral_energy_ratio(w_arr[valid], period_min, period_max)
            score = (ratio - e_low) / (e_high - e_low + EPS)
            B.iloc[t, B.columns.get_loc(s)] = float(np.clip(score, 0, 1))
    return B


print("--- Engine B (all df columns, RUNNING only) ---")
B_sensors = select_engineB_sensors(df, running, sqs, cfg.engineB_sqs_min, A_sensors)
print(f"   Eligible sensors for Engine B: {len(B_sensors)}")

B = compute_engineB(
    df, sqs, running, B_sensors, baseline_cache,
    cfg.engineB_win, cfg.engineB_sqs_min, cfg.engineB_valid_frac_min,
    cfg.engineB_period_min, cfg.engineB_period_max,
)
print(f"   Engine B shape: {B.shape}, non-null: {B.notna().sum().sum():,}")
print("✅ Engine B complete (downtime excluded, all df columns).")

# %%
# =============================================================================
# Cell 9.5: Sensor Trust Classification (SQS + Engine A + Engine B)
# =============================================================================
# Purpose: Classify each sensor at each timestamp as Reliable / Degraded / Unusable.
# This is purely informational — NO sensors are dropped or excluded.
# =============================================================================
print("=" * 70)
print("SENSOR TRUST CLASSIFICATION")
print("=" * 70)


def compute_sensor_trust(
    df: pd.DataFrame,
    sqs: pd.DataFrame,
    A: pd.DataFrame,
    B: pd.DataFrame,
    running_mask: pd.Series,
    sqs_unusable: float,
    sqs_reliable: float,
    drift_reliable: float,
    period_reliable: float,
) -> pd.DataFrame:
    """
    Classify each sensor at each timestamp:
        - 'Unusable'  : SQS < sqs_unusable
        - 'Reliable'  : SQS >= sqs_reliable AND Drift < drift_reliable AND Periodicity < period_reliable
        - 'Degraded'  : everything else

    Returns DataFrame of same shape as df with string labels.
    Non-running timestamps are labelled 'Unusable'.
    """
    all_sensors = list(df.columns)
    trust = pd.DataFrame("Unusable", index=df.index, columns=all_sensors)

    for s in all_sensors:
        # --- Get per-sensor SQS, A, B series (default to safe values if missing) ---
        s_sqs = sqs[s] if s in sqs.columns else pd.Series(1.0, index=df.index)
        s_a   = A[s]   if s in A.columns   else pd.Series(0.0, index=df.index)
        s_b   = B[s]   if s in B.columns   else pd.Series(0.0, index=df.index)

        # Fill NaN scores with conservative defaults
        s_sqs_filled = s_sqs.fillna(0.0)
        s_a_filled   = s_a.fillna(0.0)
        s_b_filled   = s_b.fillna(0.0)

        # --- Classification rules ---
        # Rule 1: Unusable if SQS <= threshold (already default)
        unusable_mask = (s_sqs_filled <= sqs_unusable) | (~running_mask)

        # Rule 2: Reliable if SQS >= high AND drift low AND periodicity low
        reliable_mask = (
            running_mask
            & (s_sqs_filled >= sqs_reliable)
            & (s_a_filled < drift_reliable)
            & (s_b_filled < period_reliable)
        )

        # Rule 3: Degraded = everything else (running, not unusable, not reliable)
        degraded_mask = running_mask & (~unusable_mask) & (~reliable_mask)

        trust.loc[reliable_mask, s] = "Reliable"
        trust.loc[degraded_mask, s] = "Degraded"
        # Unusable is already the default

    return trust


def summarize_trust(
    trust: pd.DataFrame,
    running_mask: pd.Series,
) -> pd.DataFrame:
    """Per-sensor summary: % Reliable / Degraded / Unusable during running."""
    running_trust = trust.loc[running_mask]
    n_running = len(running_trust)
    rows = []
    for s in trust.columns:
        counts = running_trust[s].value_counts()
        rows.append({
            "sensor": s,
            "n_running": n_running,
            "n_reliable": int(counts.get("Reliable", 0)),
            "n_degraded": int(counts.get("Degraded", 0)),
            "n_unusable": int(counts.get("Unusable", 0)),
            "pct_reliable": round(100.0 * counts.get("Reliable", 0) / max(n_running, 1), 2),
            "pct_degraded": round(100.0 * counts.get("Degraded", 0) / max(n_running, 1), 2),
            "pct_unusable": round(100.0 * counts.get("Unusable", 0) / max(n_running, 1), 2),
        })
    return pd.DataFrame(rows).sort_values("pct_reliable", ascending=True).reset_index(drop=True)


# =================== EXECUTE ===================
sensor_trust = compute_sensor_trust(
    df, sqs, A, B, running,
    sqs_unusable=cfg.trust_sqs_unusable,
    sqs_reliable=cfg.trust_sqs_reliable,
    drift_reliable=cfg.trust_drift_reliable,
    period_reliable=cfg.trust_period_reliable,
)

trust_summary = summarize_trust(sensor_trust, running)

# --- Print summary ---
n_sensors = len(trust_summary)
avg_reliable = trust_summary["pct_reliable"].mean()
avg_degraded = trust_summary["pct_degraded"].mean()
avg_unusable = trust_summary["pct_unusable"].mean()

print(f"\n   Total sensors classified: {n_sensors}")
print(f"   Avg % Reliable (running): {avg_reliable:.1f}%")
print(f"   Avg % Degraded (running): {avg_degraded:.1f}%")
print(f"   Avg % Unusable (running): {avg_unusable:.1f}%")

# Sensors that are mostly unusable
mostly_unusable = trust_summary[trust_summary["pct_unusable"] > 50]
if len(mostly_unusable) > 0:
    print(f"\n   ⚠️  Sensors >50% Unusable during running ({len(mostly_unusable)}):")
    for _, row in mostly_unusable.iterrows():
        print(f"       • {row['sensor']}: {row['pct_unusable']:.1f}% unusable")

# Sensors that are mostly reliable
mostly_reliable = trust_summary[trust_summary["pct_reliable"] > 90]
print(f"\n   ✅ Sensors >90% Reliable during running: {len(mostly_reliable)}")

# Save summary
trust_summary_path = os.path.join(cfg.output_dir, "sensor_trust_summary.csv")
trust_summary.to_csv(trust_summary_path, index=False)
print(f"   💾 Saved: {trust_summary_path}")

print(f"\n✅ Sensor Trust Classification complete. Shape: {sensor_trust.shape}")

# %%
# =============================================================================
# Cell 8: Dynamic System Discovery [Stability Plateau Threshold]
# =============================================================================
print("=" * 70)
print("DYNAMIC SYSTEM DISCOVERY  [Stability Plateau Threshold]")
print("=" * 70)


def clean_for_clustering(
    df: pd.DataFrame, running_mask: pd.Series,
    null_threshold_pct: float, ffill_limit: int,
) -> pd.DataFrame:
    # ONLY RUNNING data used for clustering
    df_running = df.loc[running_mask].copy()

    status_cols = [c for c in df_running.columns if "STATUS" in c.upper()]
    df_clean = df_running.drop(columns=status_cols, errors="ignore")

    null_pct = df_clean.isnull().mean() * 100
    high_null = null_pct[null_pct > null_threshold_pct].index.tolist()
    df_clean = df_clean.drop(columns=high_null, errors="ignore")
    print(f"   Dropped {len(status_cols)} STATUS cols, {len(high_null)} high-null cols")
    dropped_cols = set(status_cols) | set(high_null)
    print(f" Dropped columns: {dropped_cols}")
    std_vals = df_clean.std(skipna=True)
    constant = std_vals[std_vals < 1e-10].index.tolist()
    df_clean = df_clean.drop(columns=constant, errors="ignore")
    print(f"   Dropped {len(constant)} constant cols")

    df_clean = df_clean.dropna(how="all")
    df_clean = df_clean.ffill(limit=ffill_limit)
    rows_before = len(df_clean)
    df_clean = df_clean.dropna()
    print(f"   Dropped {rows_before - len(df_clean)} rows with remaining nulls")
    print(f"   Clustering input: {df_clean.shape[0]} rows × {df_clean.shape[1]} cols")
    return df_clean


def _find_plateau_threshold(
    Z: np.ndarray,
    min_system_size: int,
    plateau_min_d: float,
    plateau_max_d: float,
    plateau_steps: int,
    output_dir: str,
) -> float:
    thresholds = np.linspace(plateau_min_d, plateau_max_d, plateau_steps)
    n_valid = []
    for t in thresholds:
        labels = fcluster(Z, t, criterion="distance")
        _, counts = np.unique(labels, return_counts=True)
        n_valid.append(int(np.sum(counts >= min_system_size)))
    n_valid = np.array(n_valid)

    plateau_runs: List[Tuple[int, int, int, int]] = []
    start_idx = 0
    for i in range(1, len(n_valid)):
        if n_valid[i] != n_valid[i - 1]:
            plateau_runs.append((start_idx, i - 1, int(n_valid[start_idx]), i - start_idx))
            start_idx = i
    plateau_runs.append((start_idx, len(n_valid) - 1,
                         int(n_valid[start_idx]), len(n_valid) - start_idx))

    valid_runs = [p for p in plateau_runs if p[2] > 0]

    if not valid_runs:
        theta_star = (plateau_min_d + plateau_max_d) / 2
        print(f"   ⚠️  No stability plateau found — fallback θ*={theta_star:.4f}")
    else:
        longest = max(valid_runs, key=lambda x: x[3])
        idx_start, idx_end = longest[0], longest[1]
        theta_star = float((thresholds[idx_start] + thresholds[idx_end]) / 2)
        print(f"   Longest plateau: K={longest[2]}  "
              f"θ ∈ [{thresholds[idx_start]:.4f}, {thresholds[idx_end]:.4f}]")
        print(f"   Optimal θ* (plateau midpoint): {theta_star:.4f}")

    os.makedirs(output_dir, exist_ok=True)
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.step(thresholds, n_valid, where="post", color="#2c3e50", lw=2,
            label="Valid system count K(θ)")
    if valid_runs:
        longest = max(valid_runs, key=lambda x: x[3])
        ax.axvspan(thresholds[longest[0]], thresholds[longest[1]],
                   color="green", alpha=0.2, label="Longest stability plateau")
    ax.axvline(theta_star, color="red", linestyle="--", linewidth=2,
               label=f"θ* = {theta_star:.4f}")
    ax.set_title("Topological Stability Analysis", fontsize=13, fontweight="bold")
    ax.set_xlabel("Distance Threshold θ")
    ax.set_ylabel(f"Valid Sub-systems (size ≥ {min_system_size})")
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    stab_path = os.path.join(output_dir, "topological_stability.png")
    plt.savefig(stab_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"   💾 Saved stability plot: {stab_path}")
    return theta_star


def discover_systems(
    df_analysis: pd.DataFrame,
    corr_method: str, cluster_method: str,
    threshold_auto: bool, threshold_manual: float, threshold_max: float,
    min_system_size: int, output_dir: str,
    plateau_steps: int = 200,
    plateau_min_d: float = 0.0,
) -> Tuple[Dict[str, List[str]], List[str], float]:
    if df_analysis.shape[1] < 2:
        print("   ⚠️  Fewer than 2 columns — cannot cluster.")
        return {}, list(df_analysis.columns), threshold_manual

    corr_matrix = df_analysis.corr(method=corr_method).abs()
    dist_matrix = np.sqrt(2 * (1 - corr_matrix))
    np.fill_diagonal(dist_matrix.values, 0.0)

    print(f"   Correlation matrix: {corr_matrix.shape}")

    dist_condensed = squareform(dist_matrix.values, checks=False)
    Z = linkage(dist_condensed, method=cluster_method)
    merge_distances = Z[:, 2]
    print(f"   Merge distance range: [{merge_distances.min():.4f}, {merge_distances.max():.4f}]")

    if threshold_auto:
        threshold_used = _find_plateau_threshold(
            Z=Z, min_system_size=min_system_size,
            plateau_min_d=plateau_min_d, plateau_max_d=threshold_max,
            plateau_steps=plateau_steps, output_dir=output_dir,
        )
    else:
        threshold_used = threshold_manual
        print(f"   Manual threshold: {threshold_used:.4f}")

    os.makedirs(output_dir, exist_ok=True)
    fig, ax = plt.subplots(figsize=(16, 8))
    dendrogram(
        Z, labels=df_analysis.columns.tolist(),
        leaf_rotation=90, leaf_font_size=7,
        color_threshold=threshold_used, above_threshold_color="gray", ax=ax,
    )
    ax.axhline(y=threshold_used, color="red", linestyle="--", linewidth=2,
               label=f"θ* = {threshold_used:.4f}")
    ax.set_title("Hierarchical System Identification — Dendrogram",
                 fontsize=14, fontweight="bold")
    ax.set_ylabel("Distance d(i,j)")
    ax.set_xlabel("Sensor Variables")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    plt.tight_layout()
    dend_path = os.path.join(output_dir, "dendrogram_systems.png")
    plt.savefig(dend_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"   💾 Saved dendrogram: {dend_path}")

    fig, axes_hm = plt.subplots(1, 2, figsize=(22, 9))
    do_annot = corr_matrix.shape[0] <= 30
    sns.heatmap(corr_matrix, annot=do_annot, fmt=".2f", cmap="RdYlBu_r",
                vmin=0, vmax=1, ax=axes_hm[0], square=True,
                cbar_kws={"shrink": 0.8}, annot_kws={"size": 7})
    axes_hm[0].set_title("|Spearman Correlation|", fontsize=14, fontweight="bold")
    axes_hm[0].tick_params(axis="both", labelsize=7)
    sns.heatmap(dist_matrix, annot=do_annot, fmt=".2f", cmap="YlOrRd",
                vmin=0, vmax=1.5, ax=axes_hm[1], square=True,
                cbar_kws={"shrink": 0.8}, annot_kws={"size": 7})
    axes_hm[1].set_title("Distance d(i,j) = √(2(1-|ρ|))", fontsize=14, fontweight="bold")
    axes_hm[1].tick_params(axis="both", labelsize=7)
    plt.tight_layout()
    corr_path = os.path.join(output_dir, "correlation_distance_heatmaps.png")
    plt.savefig(corr_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"   💾 Saved correlation heatmap: {corr_path}")

    cluster_labels = fcluster(Z, threshold_used, criterion="distance")
    columns = df_analysis.columns.tolist()

    raw_systems: Dict[int, List[str]] = {}
    for idx, lab in enumerate(cluster_labels):
        raw_systems.setdefault(lab, []).append(columns[idx])

    systems_dict: Dict[str, List[str]] = {}
    isolated: List[str] = []
    sys_counter = 1
    for _, vars_list in sorted(raw_systems.items()):
        if len(vars_list) >= min_system_size:
            systems_dict[f"SYS_{sys_counter}"] = vars_list
            sys_counter += 1
        else:
            isolated.extend(vars_list)

    print(f"\n   📊 Total clusters: {len(raw_systems)}")
    print(f"   ✅ Multi-variable systems (≥{min_system_size} vars): {len(systems_dict)}")
    print(f"   ⚠️  Isolated variables: {len(isolated)}")

    for sys_id, vars_list in systems_dict.items():
        print(f"\n   🔷 {sys_id} ({len(vars_list)} variables):")
        for v in vars_list:
            print(f"       • {v}")

    if isolated:
        print(f"\n   🔸 ISOLATED:")
        for v in isolated:
            print(f"       • {v}")

    return systems_dict, isolated, threshold_used


def validate_systems_r2(
    df_analysis: pd.DataFrame,
    systems_dict: Dict[str, List[str]],
    r2_min_quality: float,
    output_dir: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, List[str]]]:
    results, details = [], []
    usable_systems: Dict[str, List[str]] = {}

    for sys_id, variables in sorted(systems_dict.items()):
        variables = [v for v in variables if v in df_analysis.columns]
        if len(variables) < 2:
            continue

        r2_adj_list = []
        for v_target in variables:
            predictors = [v for v in variables if v != v_target]
            X = df_analysis[predictors].values
            y = df_analysis[v_target].values
            reg = LinearRegression().fit(X, y)
            y_pred = reg.predict(X)
            r2 = r2_score(y, y_pred)
            n, p = X.shape
            r2_adj = 1 - (1 - r2) * (n - 1) / max(n - p - 1, 1)
            r2_adj_list.append(r2_adj)
            status = "✅" if r2_adj > 0.8 else "⚠️" if r2_adj > 0.5 else "❌"
            print(f"  {status} {sys_id} | Target: {v_target:40s} | R²_adj = {r2_adj:.4f}")
            details.append({
                "System_ID": sys_id, "Target": v_target,
                "N_Predictors": p, "R2": round(r2, 4), "R2_Adjusted": round(r2_adj, 4),
            })

        r2_mean    = float(np.mean(r2_adj_list))
        r2_min_val = float(np.min(r2_adj_list))

        subset = df_analysis[variables]
        subset_norm = (subset - subset.mean()) / subset.std()
        corr_svd = subset_norm.corr(method="spearman").values
        corr_svd += np.eye(len(variables)) * 1e-10
        S = np.linalg.svd(corr_svd, compute_uv=False)
        cohesion_index = float(S[0] / len(variables))
        cond_number    = float(S[0] / S[-1])
        svd_status = "STABLE ✅" if (cohesion_index > 0.65 and cond_number < 30) \
                     else "HIGH REDUNDANCY / WEAK ⚠️"

        print(f"       → Cohesion C={cohesion_index:.4f}  κ={cond_number:.2f}  [{svd_status}]")

        quality = "HIGH ✅" if r2_mean > 0.8 else "MEDIUM ⚠️" if r2_mean > r2_min_quality else "LOW ❌"
        results.append({
            "System_ID":     sys_id,
            "N_Variables":   len(variables),
            "Variables":     variables,
            "R2_Adj_Mean":   round(r2_mean, 4),
            "R2_Adj_Min":    round(r2_min_val, 4),
            "Quality":       quality,
            "Cohesion_C":    round(cohesion_index, 4),
            "Cond_Number_k": round(cond_number, 2),
            "SVD_Status":    svd_status,
        })
        if r2_mean >= r2_min_quality:
            usable_systems[sys_id] = variables

    if results:
        n_sys = len(results)
        fig, axes = plt.subplots(1, n_sys, figsize=(5 * n_sys, 4), squeeze=False)
        axes = axes.flatten()
        for i, row in enumerate(results):
            vars_list = row["Variables"]
            subset_norm = (df_analysis[vars_list] - df_analysis[vars_list].mean()) / df_analysis[vars_list].std()
            corr_m = subset_norm.corr(method="spearman").values
            S_plot = np.linalg.svd(corr_m, compute_uv=False)
            axes[i].bar(range(1, len(S_plot) + 1), S_plot,
                        color="#2c3e50", alpha=0.8, edgecolor="black")
            axes[i].set_title(
                f"{row['System_ID']}\nC={row['Cohesion_C']:.2f}  κ={row['Cond_Number_k']:.1f}",
                fontsize=11, fontweight="bold")
            axes[i].set_xlabel("Component")
            axes[i].set_ylabel("Singular Value (Energy)")
            axes[i].axhline(1.0, color="crimson", linestyle="--", alpha=0.6, label="Unit energy")
            axes[i].grid(axis="y", linestyle=":", alpha=0.6)
        plt.suptitle("Spectral Energy per System", fontsize=13, fontweight="bold", y=1.02)
        plt.tight_layout()
        spec_path = os.path.join(output_dir, "spectral_energy.png")
        plt.savefig(spec_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"   💾 Saved spectral energy plot: {spec_path}")

    return pd.DataFrame(results), pd.DataFrame(details), usable_systems


# =================== EXECUTE ===================
df_for_clustering = clean_for_clustering(
    df, running,
    null_threshold_pct=cfg.sys_null_threshold_pct,
    ffill_limit=cfg.sys_ffill_limit,
)

if len(df_for_clustering) < 100:
    print("⚠️  Very few clean rows for clustering. Results may be unreliable.")

_plateau_steps = getattr(cfg, "sys_plateau_steps", 200)
_plateau_min_d = getattr(cfg, "sys_plateau_min_d", 0.0)

systems_raw, isolated_sensors, threshold_used = discover_systems(
    df_for_clustering,
    corr_method=cfg.sys_corr_method,
    cluster_method=cfg.sys_cluster_method,
    threshold_auto=cfg.sys_threshold_auto,
    threshold_manual=cfg.sys_threshold_manual,
    threshold_max=cfg.sys_threshold_max,
    min_system_size=cfg.sys_min_system_size,
    output_dir=cfg.output_dir,
    plateau_steps=_plateau_steps,
    plateau_min_d=_plateau_min_d,
)

print("\n--- System Validation (Cross-Reconstruction R² + Cohesion + Condition Number) ---")
sys_summary, sys_detail, catalog = validate_systems_r2(
    df_for_clustering, systems_raw, cfg.sys_r2_adj_min_quality, cfg.output_dir,
)

print(f"\n{'=' * 70}")
print("SYSTEM SUMMARY")
print(f"{'=' * 70}")
if not sys_summary.empty:
    print(sys_summary[[
        "System_ID", "N_Variables", "R2_Adj_Mean", "R2_Adj_Min", "Quality",
        "Cohesion_C", "Cond_Number_k", "SVD_Status",
    ]].to_string(index=False))

if isolated_sensors:
    catalog["ISOLATED"] = isolated_sensors

all_catalog_sensors = []
for sensors in catalog.values():
    all_catalog_sensors.extend(sensors)

print(f"\n✅ Dynamic catalog: {len(catalog)} groups, {len(all_catalog_sensors)} total sensors")
for label, sensors in catalog.items():
    print(f"   {label}: {len(sensors)} sensors")

# Dynamic weights
total_in_systems = sum(len(v) for v in catalog.values())
dynamic_weights: Dict[str, float] = {}
dynamic_risk_weights: Dict[str, float] = {}
for label, sensors in catalog.items():
    if not sensors:
        continue
    frac = len(sensors) / max(total_in_systems, 1)
    dynamic_weights[label] = frac
    dynamic_risk_weights[label] = frac

print(f"\n   Dynamic fusion weights:  {dynamic_weights}")
print(f"   Dynamic risk weights:    {dynamic_risk_weights}")

os.makedirs(cfg.output_dir, exist_ok=True)
if not sys_summary.empty:
    sys_summary.to_csv(os.path.join(cfg.output_dir, "system_summary.csv"), index=False)
if not sys_detail.empty:
    sys_detail.to_csv(os.path.join(cfg.output_dir, "system_detail.csv"), index=False)

catalog_rows = []
for label, sensors in catalog.items():
    for s in sensors:
        catalog_rows.append({"system": label, "sensor": s})
pd.DataFrame(catalog_rows).to_csv(
    os.path.join(cfg.output_dir, "dynamic_catalog.csv"), index=False
)
print(f"   💾 Saved artifacts to {cfg.output_dir}")

# %%
# =============================================================================
# Cell 9: AutoEncoder Subsystem Scoring 
# =============================================================================
# Deep AutoEncoder with Directional Residual Operator, Reference Baseline
# Estimation, Subsystem Confidence, and Robust Adaptive Thresholding.
# Trained on RUNNING data only. Produces one system-level risk score per
# subsystem per timestamp plus confidence and alarm outputs.
# =============================================================================

print("=" * 70)
print("AUTOENCODER SUBSYSTEM SCORING (v2 – with Confidence & Baseline)")
print("=" * 70)

# ─────────────────────────────────────────────────────────────────────────────
# NEW CONFIG ADDITIONS (add these to your PipelineConfig dataclass above)
# ─────────────────────────────────────────────────────────────────────────────
# If PipelineConfig is a dataclass, add these fields; otherwise just set them
# on the existing cfg object.

# --- Reference Baseline Estimation ---
if not hasattr(cfg, 'ae_baseline_n_samples'):
    cfg.ae_baseline_n_samples = 500          # first N healthy samples for baseline
if not hasattr(cfg, 'ae_baseline_sqs_min'):
    cfg.ae_baseline_sqs_min = 0.8            # minimum SQS to qualify as healthy
if not hasattr(cfg, 'ae_baseline_drift_max'):
    cfg.ae_baseline_drift_max = 0.5          # max(DriftScore, PeriodicityScore) < this
if not hasattr(cfg, 'ae_min_sensors'):
    cfg.ae_min_sensors = 2                   # ← CHANGED from default (was likely 3+)

# --- Subsystem Confidence ---
if not hasattr(cfg, 'ae_confidence_weights'):
    # (subsystem_score_level, trust_level) → weight
    # subsystem_score_level: "high" if score >= threshold, else "low"
    # trust_level: "Reliable", "Degraded", "Unusable"
    cfg.ae_confidence_weights = {
        ("high", "Reliable"):  1.00,
        ("high", "Degraded"):  0.75,
        ("high", "Unusable"):  0.40,
        ("low",  "Reliable"):  0.70,
        ("low",  "Degraded"):  0.50,
        ("low",  "Unusable"):  0.20,
    }

# --- Alarm Persistence ---
if not hasattr(cfg, 'ae_on_delay'):
    cfg.ae_on_delay = 3                      # 3 consecutive minutes above threshold
if not hasattr(cfg, 'ae_off_delay'):
    cfg.ae_off_delay = 5                     # 5 consecutive minutes below threshold

# --- Existing configs (ensure they exist) ---
if not hasattr(cfg, 'ae_missing_max'):
    cfg.ae_missing_max = 0.3
if not hasattr(cfg, 'ae_mean_abs_corr_min'):
    cfg.ae_mean_abs_corr_min = 0.15
if not hasattr(cfg, 'ae_min_training_rows'):
    cfg.ae_min_training_rows = 100
if not hasattr(cfg, 'ae_latent_dim'):
    cfg.ae_latent_dim = 3
if not hasattr(cfg, 'ae_lr'):
    cfg.ae_lr = 1e-3
if not hasattr(cfg, 'ae_epochs'):
    cfg.ae_epochs = 80
if not hasattr(cfg, 'ae_batch_size'):
    cfg.ae_batch_size = 64
if not hasattr(cfg, 'ae_risk_sigma_factor'):
    cfg.ae_risk_sigma_factor = 3.0
if not hasattr(cfg, 'ae_threshold_window'):
    cfg.ae_threshold_window = 100
if not hasattr(cfg, 'ae_slew_rate'):
    cfg.ae_slew_rate = 0.0005
if not hasattr(cfg, 'ae_gate_factor'):
    cfg.ae_gate_factor = 1.5

print(f"   Config: ae_min_sensors        = {cfg.ae_min_sensors}")
print(f"   Config: ae_baseline_n_samples = {cfg.ae_baseline_n_samples}")
print(f"   Config: ae_baseline_sqs_min   = {cfg.ae_baseline_sqs_min}")
print(f"   Config: ae_baseline_drift_max = {cfg.ae_baseline_drift_max}")
print(f"   Config: ae_on_delay           = {cfg.ae_on_delay} min")
print(f"   Config: ae_off_delay          = {cfg.ae_off_delay} min")
print(f"   Config: ae_confidence_weights = {len(cfg.ae_confidence_weights)} entries")


# ─────────────────────────────────────────────────────────────────────────────
# DeepAutoEncoder Model
# ─────────────────────────────────────────────────────────────────────────────

class DeepAutoEncoder(nn.Module):
    """Symmetric encoder–decoder with configurable bottleneck."""

    def __init__(self, input_dim: int, latent_dim: int = 3):
        super().__init__()
        h1 = max(latent_dim + 1, min(16, input_dim))
        h2 = max(latent_dim, min(8, h1))
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, h1),
            nn.Tanh(),
            nn.Linear(h1, h2),
            nn.ReLU(),
            nn.Linear(h2, latent_dim),
        )
        self.decoder = nn.Sequential(
            nn.Linear(latent_dim, h2),
            nn.ReLU(),
            nn.Linear(h2, h1),
            nn.Tanh(),
            nn.Linear(h1, input_dim),
        )

    def forward(self, x):
        return self.decoder(self.encoder(x))


# ─────────────────────────────────────────────────────────────────────────────
# Directional Residual Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_direction(sensor_name: str) -> str:
    """Lookup directional mode from ANOMALY_DIRECTION_CONFIG; default='both'."""
    return ANOMALY_DIRECTION_CONFIG.get(sensor_name, "both")


def directional_residual(raw_residual: np.ndarray, mode: str) -> np.ndarray:
    """
    Ψ(r, δ):
      pos  → max(0, r)   only care when actual > predicted
      neg  → max(0, -r)  only care when actual < predicted
      both → |r|
    """
    if mode == "pos":
        return np.maximum(0.0, raw_residual)
    elif mode == "neg":
        return np.maximum(0.0, -raw_residual)
    else:
        return np.abs(raw_residual)


# ─────────────────────────────────────────────────────────────────────────────
# 1. REFERENCE BASELINE ESTIMATION (NEW)
# ─────────────────────────────────────────────────────────────────────────────

def compute_reference_baseline(
    risk_array: np.ndarray,
    running_mask: pd.Series,
    sqs: pd.DataFrame,
    sensors: List[str],
    n_samples: int = 500,
    sqs_min: float = 0.8,
    drift_max: float = 0.5,
) -> float:
    """
    Reference Baseline Estimation.

    Defines the natural fluctuation band of the process using the first
    `n_samples` healthy samples that satisfy:
      - No downtime (running_mask == True)
      - mean SQS across subsystem sensors >= sqs_min
      - max(DriftScore, PeriodicityScore) < drift_max  (if available)

    Returns:
        sigma_baseline: std of the risk evidence over the healthy baseline window
    """
    # Build a boolean mask for "healthy" timestamps
    n_total = len(risk_array)
    healthy_mask = np.ones(n_total, dtype=bool)

    # Condition 1: must be running
    running_arr = running_mask.values if hasattr(running_mask, 'values') else np.array(running_mask)
    healthy_mask &= running_arr[:n_total]

    # Condition 2: mean SQS across subsystem sensors >= sqs_min
    available_sqs_sensors = [s for s in sensors if s in sqs.columns]
    if available_sqs_sensors:
        mean_sqs = sqs[available_sqs_sensors].mean(axis=1).values[:n_total]
        # Treat NaN as unhealthy
        healthy_mask &= np.nan_to_num(mean_sqs, nan=0.0) >= sqs_min

    # Condition 3: max(DriftScore, PeriodicityScore) < drift_max
    # Check if drift_scores and periodicity_scores DataFrames exist globally
    drift_available = False
    try:
        # Try to access drift/periodicity scores if they exist
        if 'drift_scores' in dir() or 'drift_scores' in globals():
            drift_df = globals().get('drift_scores', None)
            period_df = globals().get('periodicity_scores', None)
            if drift_df is not None and period_df is not None:
                avail_drift = [s for s in sensors if s in drift_df.columns]
                avail_period = [s for s in sensors if s in period_df.columns]
                if avail_drift or avail_period:
                    max_score = np.zeros(n_total)
                    if avail_drift:
                        max_score = np.maximum(
                            max_score,
                            drift_df[avail_drift].max(axis=1).values[:n_total]
                        )
                    if avail_period:
                        max_score = np.maximum(
                            max_score,
                            period_df[avail_period].max(axis=1).values[:n_total]
                        )
                    healthy_mask &= np.nan_to_num(max_score, nan=1.0) < drift_max
                    drift_available = True
    except Exception:
        pass

    # Select first n_samples healthy indices
    healthy_indices = np.where(healthy_mask)[0]

    if len(healthy_indices) < 20:
        # Fallback: use first n_samples of running data
        running_indices = np.where(running_arr[:n_total])[0]
        healthy_indices = running_indices[:min(n_samples, len(running_indices))]
        if len(healthy_indices) < 10:
            return float(np.std(risk_array)) if float(np.std(risk_array)) > 0 else 1e-6

    baseline_indices = healthy_indices[:min(n_samples, len(healthy_indices))]
    baseline_risk = risk_array[baseline_indices]

    sigma_baseline = float(np.std(baseline_risk))
    if sigma_baseline < 1e-8:
        sigma_baseline = 1e-6

    return sigma_baseline


# ─────────────────────────────────────────────────────────────────────────────
# Robust Adaptive Threshold (uses Reference Baseline sigma)
# ─────────────────────────────────────────────────────────────────────────────

def robust_adaptive_threshold(
    risk: np.ndarray,
    baseline_sigma: float,
    window: int = 100,
    slew_rate: float = 0.0005,
    gate: float = 1.5,
    sigma_factor: float = 3.0,
) -> np.ndarray:
    """Inertial adaptive threshold with gated adaptation + slew-rate limiting."""
    n = len(risk)
    threshold = np.zeros(n)
    seed = float(np.nanmean(risk[:min(window, n)])) + sigma_factor * baseline_sigma
    threshold[:min(window, n)] = seed

    for t in range(min(window, n), n):
        target = (float(np.nanmean(risk[max(0, t - window):t]))
                  + sigma_factor * baseline_sigma)
        # Gating: freeze if risk is anomalously high
        if risk[t] > gate * threshold[t - 1]:
            target = threshold[t - 1]
        change = np.clip(target - threshold[t - 1], -slew_rate, slew_rate)
        threshold[t] = threshold[t - 1] + change

    return threshold


# ─────────────────────────────────────────────────────────────────────────────
# 2. SUBSYSTEM CONFIDENCE (NEW)
# ─────────────────────────────────────────────────────────────────────────────

# Trust-weight lookup table
TRUST_WEIGHT_TABLE = {
    # (score_level, trust_level) → weight
    # score_level: "high" (score >= threshold) or "low" (score < threshold)
    ("high", "Reliable"):  1.00,
    ("high", "Degraded"):  0.75,
    ("high", "Unusable"):  0.40,
    ("low",  "Reliable"):  0.70,
    ("low",  "Degraded"):  0.50,
    ("low",  "Unusable"):  0.20,
}

# Override from config if provided
if hasattr(cfg, 'ae_confidence_weights') and cfg.ae_confidence_weights:
    TRUST_WEIGHT_TABLE.update(cfg.ae_confidence_weights)


def compute_subsystem_confidence(
    system_score: pd.Series,
    threshold: np.ndarray,
    sensors: List[str],
    sensor_trust: pd.DataFrame,
    running_mask: pd.Series,
) -> pd.Series:
    """
    Compute Subsystem Confidence at each timestamp.

    SubsystemConfidence_{i,t} = (1/M) * Σ Weight_{j,t}

    Where Weight_{j,t} depends on:
      - Whether system_score >= threshold (high/low)
      - The trust classification of sensor j at time t

    Returns:
        pd.Series of confidence values [0, 1] per timestamp
    """
    n = len(system_score)
    confidence = np.zeros(n, dtype=float)

    # Get trust columns that are available
    available_sensors = [s for s in sensors if s in sensor_trust.columns]
    if not available_sensors:
        return pd.Series(0.0, index=system_score.index)

    M = len(available_sensors)
    score_arr = system_score.values
    thresh_arr = threshold

    # Pre-extract trust arrays for each sensor
    trust_arrays = {}
    for s in available_sensors:
        trust_arrays[s] = sensor_trust[s].values

    for t in range(n):
        # Determine score level
        score_level = "high" if score_arr[t] >= thresh_arr[t] else "low"

        weight_sum = 0.0
        for s in available_sensors:
            trust_val = str(trust_arrays[s][t]) if t < len(trust_arrays[s]) else "Unusable"

            # Normalize trust classification
            if trust_val in ("Reliable", "reliable"):
                trust_key = "Reliable"
            elif trust_val in ("Degraded", "degraded"):
                trust_key = "Degraded"
            else:
                trust_key = "Unusable"

            w = TRUST_WEIGHT_TABLE.get((score_level, trust_key), 0.0)
            weight_sum += w

        confidence[t] = weight_sum / M

    # Zero out during downtime
    conf_series = pd.Series(confidence, index=system_score.index)
    conf_series.loc[~running_mask] = 0.0

    return conf_series


# ─────────────────────────────────────────────────────────────────────────────
# Alarm Persistence Logic (ON/OFF delay)
# ─────────────────────────────────────────────────────────────────────────────

def _on_off_delay_alarm(
    risk: np.ndarray, threshold: np.ndarray, on_delay: int, off_delay: int
) -> np.ndarray:
    """
    Alarm Persistence Logic.

    - Alarm activates after `on_delay` consecutive minutes where
      score >= threshold.
    - Alarm clears after `off_delay` consecutive minutes where
      score < threshold.
    """
    n = len(risk)
    alarms = np.zeros(n, dtype=bool)
    count = 0
    active = False

    for i in range(n):
        is_high = risk[i] >= threshold[i]

        if not active:
            if is_high:
                count += 1
                if count >= on_delay:
                    active = True
                    alarms[i] = True
                    count = 0
            else:
                count = 0
        else:
            alarms[i] = True
            if not is_high:
                count += 1
                if count >= off_delay:
                    active = False
                    count = 0
            else:
                count = 0

    return alarms


# ─────────────────────────────────────────────────────────────────────────────
# Sensor Selection for AE (min_sensors = 2 now)
# ─────────────────────────────────────────────────────────────────────────────

def select_ae_candidates(
    df: pd.DataFrame,
    running_mask: pd.Series,
    sensors: List[str],
    missing_max: float,
    min_abs_corr: float,
) -> List[str]:
    """
    Select sensors suitable for AE training within a subsystem.
    Now works with as few as 2 sensors.
    """
    present = [s for s in sensors if s in df.columns]
    if len(present) < 2:
        # Even with 1 sensor, return it (AE will handle single-sensor case)
        return present

    running_df = df.loc[running_mask, present].copy()
    if running_df.shape[0] < 100:
        return []

    # Drop sensors with too many missing values during running
    good = [s for s in present
            if running_df[s].isna().mean() <= missing_max
            and running_df[s].var(skipna=True) > 0]

    if len(good) < 2:
        return good

    # Check mean absolute correlation
    filled = running_df[good].ffill().bfill()
    mac = filled.corr().abs()
    mac_mean = mac.where(~np.eye(len(good), dtype=bool)).mean()

    selected = [s for s in good if float(mac_mean.get(s, 0.0)) >= min_abs_corr]

    # If correlation filter removes too many, keep at least 2
    if len(selected) < 2 and len(good) >= 2:
        # Keep the top-2 by mean abs correlation
        corr_vals = [(s, float(mac_mean.get(s, 0.0))) for s in good]
        corr_vals.sort(key=lambda x: x[1], reverse=True)
        selected = [s for s, _ in corr_vals[:2]]

    return selected


# ─────────────────────────────────────────────────────────────────────────────
# AE Risk Array Computation
# ─────────────────────────────────────────────────────────────────────────────

def _compute_ae_risk_array(
    raw_residuals: np.ndarray,
    sensors: List[str],
    baseline_sigmas: Dict[str, float],
    sigma_factor: float,
) -> np.ndarray:
    """
    Compute per-timestamp global risk score from raw residuals.
    Uses directional filtering + sigma scaling per sensor, then sum + MinMax.
    """
    n = raw_residuals.shape[0]
    sigma_scores = np.zeros((n, len(sensors)))

    for j, s in enumerate(sensors):
        direction = _get_direction(s)
        filtered = directional_residual(raw_residuals[:, j], direction)
        sigma = baseline_sigmas[s]
        sigma_scores[:, j] = filtered / (sigma_factor * sigma + EPS)

    # Global risk = sum of sigma-scaled directional errors, then [0,1]
    raw_risk = np.sum(sigma_scores, axis=1)
    # MinMax scale
    rmin, rmax = float(np.nanmin(raw_risk)), float(np.nanmax(raw_risk))
    if rmax - rmin < EPS:
        return np.zeros(n)
    return (raw_risk - rmin) / (rmax - rmin + EPS)


# ─────────────────────────────────────────────────────────────────────────────
# Train AE for a Subsystem
# ─────────────────────────────────────────────────────────────────────────────

def train_ae_subsystem(
    df: pd.DataFrame,
    running_mask: pd.Series,
    sensors: List[str],
    cfg,  # PipelineConfig
) -> Optional[Dict]:
    """
    Train an AutoEncoder on RUNNING data for the given sensor group.
    Now supports subsystems with as few as 2 sensors.

    Returns dict with model, scaler, baseline stats, sensor list, or None.
    """
    candidates = select_ae_candidates(
        df, running_mask, sensors, cfg.ae_missing_max, cfg.ae_mean_abs_corr_min
    )

    # CHANGED: min_sensors = 2
    if len(candidates) < cfg.ae_min_sensors:
        return None

    running_df = df.loc[running_mask, candidates].ffill().bfill().dropna()
    if len(running_df) < cfg.ae_min_training_rows:
        return None

    # Scale
    scaler = StandardScaler()
    X_train = scaler.fit_transform(running_df.values)
    train_tensor = torch.FloatTensor(X_train)

    input_dim = X_train.shape[1]
    latent_dim = min(cfg.ae_latent_dim, max(1, input_dim // 2))

    # For 2-sensor case, ensure latent_dim >= 1
    if latent_dim < 1:
        latent_dim = 1

    model = DeepAutoEncoder(input_dim, latent_dim)
    optimizer = optim.Adam(model.parameters(), lr=cfg.ae_lr)
    criterion = nn.MSELoss()

    # Training loop
    model.train()
    n_samples = train_tensor.shape[0]
    history = []
    for epoch in range(cfg.ae_epochs):
        perm = torch.randperm(n_samples)
        epoch_loss = 0.0
        n_batches = 0
        for start in range(0, n_samples, cfg.ae_batch_size):
            idx = perm[start:start + cfg.ae_batch_size]
            batch = train_tensor[idx]
            optimizer.zero_grad()
            output = model(batch)
            loss = criterion(output, batch)
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item()
            n_batches += 1
        history.append(epoch_loss / max(n_batches, 1))

    model.eval()

    # Compute baseline reconstruction residuals (per-sensor, directional)
    with torch.no_grad():
        recon_train = model(train_tensor).numpy()
    raw_resid_train = X_train - recon_train

    # Per-sensor baseline sigma (directional)
    baseline_sigmas = {}
    for j, s in enumerate(candidates):
        direction = _get_direction(s)
        filtered = directional_residual(raw_resid_train[:, j], direction)
        sigma = float(np.std(filtered)) if float(np.std(filtered)) > 0 else 1e-6
        baseline_sigmas[s] = sigma

    # Global risk on training data
    risk_train = _compute_ae_risk_array(
        raw_resid_train, candidates, baseline_sigmas, cfg.ae_risk_sigma_factor
    )
    risk_train_sigma = float(np.std(risk_train)) if float(np.std(risk_train)) > 0 else 1e-6

    return {
        "model": model,
        "scaler": scaler,
        "sensors": candidates,
        "latent_dim": latent_dim,
        "input_dim": input_dim,
        "baseline_sigmas": baseline_sigmas,
        "risk_train_sigma": risk_train_sigma,
        "history": history,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Score AE for a Subsystem (with Reference Baseline + Confidence)
# ─────────────────────────────────────────────────────────────────────────────

def score_ae_subsystem(
    df: pd.DataFrame,
    running_mask: pd.Series,
    ae_info: Dict,
    cfg,  # PipelineConfig
    sqs_df: pd.DataFrame,
    sensor_trust_df: pd.DataFrame,
) -> Dict:
    """
    Score the FULL timeline using trained AE.

    Returns dict with:
      - system_score:       pd.Series [0,1] per timestamp (0 during downtime)
      - per_sensor_sigma:   pd.DataFrame of sigma-scaled directional errors
      - threshold:          np.ndarray adaptive threshold
      - alarm:              pd.Series boolean alarm
      - confidence:         pd.Series [0,1] subsystem confidence
      - baseline_sigma:     float reference baseline sigma
      - risk_raw:           np.ndarray raw risk before clipping
    """
    model = ae_info["model"]
    scaler = ae_info["scaler"]
    sensors = ae_info["sensors"]
    baseline_sigmas = ae_info["baseline_sigmas"]

    # Prepare full data
    live_df = df[sensors].ffill().bfill()
    X_full = scaler.transform(live_df.values)
    full_tensor = torch.FloatTensor(X_full)

    model.eval()
    with torch.no_grad():
        recon_full = model(full_tensor).numpy()

    raw_resid_full = X_full - recon_full

    # Per-sensor sigma-scaled directional errors
    per_sensor_sigma = pd.DataFrame(index=df.index, columns=sensors, dtype=float)
    for j, s in enumerate(sensors):
        direction = _get_direction(s)
        filtered = directional_residual(raw_resid_full[:, j], direction)
        sigma = baseline_sigmas[s]
        per_sensor_sigma[s] = filtered / (cfg.ae_risk_sigma_factor * sigma + EPS)

    # Global risk
    risk_full = _compute_ae_risk_array(
        raw_resid_full, sensors, baseline_sigmas, cfg.ae_risk_sigma_factor
    )

    # ── 1. Reference Baseline Estimation (NEW) ──
    ref_baseline_sigma = compute_reference_baseline(
        risk_array=risk_full,
        running_mask=running_mask,
        sqs=sqs_df,
        sensors=sensors,
        n_samples=cfg.ae_baseline_n_samples,
        sqs_min=cfg.ae_baseline_sqs_min,
        drift_max=cfg.ae_baseline_drift_max,
    )

    # ── Adaptive threshold using reference baseline sigma ──
    threshold = robust_adaptive_threshold(
        risk_full,
        ref_baseline_sigma,  # ← uses reference baseline now
        window=cfg.ae_threshold_window,
        slew_rate=cfg.ae_slew_rate,
        gate=cfg.ae_gate_factor,
        sigma_factor=cfg.ae_risk_sigma_factor,
    )

    # ── Alarm persistence (ON=3min, OFF=5min) ──
    alarm = _on_off_delay_alarm(
        risk_full, threshold, cfg.ae_on_delay, cfg.ae_off_delay
    )

    # System score: risk clipped to [0,1], zeroed during downtime
    system_score = pd.Series(np.clip(risk_full, 0, 1), index=df.index)
    system_score.loc[~running_mask] = 0.0

    # ── 2. Subsystem Confidence (NEW) ──
    confidence = compute_subsystem_confidence(
        system_score=system_score,
        threshold=threshold,
        sensors=sensors,
        sensor_trust=sensor_trust_df,
        running_mask=running_mask,
    )

    return {
        "system_score": system_score,
        "per_sensor_sigma": per_sensor_sigma,
        "threshold": threshold,
        "alarm": pd.Series(alarm, index=df.index),
        "confidence": confidence,
        "baseline_sigma": ref_baseline_sigma,
        "risk_raw": risk_full,
    }


# =================== TRAIN & SCORE ALL SUBSYSTEMS ===================

ae_models: Dict[str, Dict] = {}      # sys_label -> ae_info
ae_results: Dict[str, Dict] = {}     # sys_label -> scoring results

for sys_label, sensors in catalog.items():
    if sys_label == "ISOLATED":
        print(f"   {sys_label}: skipped (no correlated group for AE)")
        continue

    present = [s for s in sensors if s in df.columns]

    # CHANGED: allow 2-sensor subsystems
    if len(present) < cfg.ae_min_sensors:
        print(f"   {sys_label}: skipped ({len(present)} sensors < "
              f"min {cfg.ae_min_sensors})")
        continue

    print(f"\n   Training AE for {sys_label} ({len(present)} sensors)...")
    ae_info = train_ae_subsystem(df, running, present, cfg)

    if ae_info is None:
        print(f"   {sys_label}: AE training failed "
              f"(insufficient data/candidates)")
        continue

    ae_models[sys_label] = ae_info

    # Score with reference baseline + confidence
    ae_result = score_ae_subsystem(
        df, running, ae_info, cfg,
        sqs_df=sqs,
        sensor_trust_df=sensor_trust,
    )
    ae_results[sys_label] = ae_result

    n_alarms = int(ae_result["alarm"].sum())
    score_mean = float(ae_result["system_score"].loc[running].mean())
    conf_mean = float(ae_result["confidence"].loc[running].mean())
    final_loss = ae_info["history"][-1] if ae_info["history"] else np.nan
    baseline_sig = ae_result["baseline_sigma"]

    print(f"   ✅ {sys_label} AE: {len(ae_info['sensors'])} sensors → "
          f"latent_dim={ae_info['latent_dim']} | "
          f"final_loss={final_loss:.6f} | "
          f"score_mean(running)={score_mean:.4f} | "
          f"confidence_mean={conf_mean:.4f} | "
          f"baseline_σ={baseline_sig:.6f} | "
          f"AE_alarms={n_alarms}")

# Save AE training curves
if ae_models:
    n_ae = len(ae_models)
    fig, axes = plt.subplots(1, n_ae, figsize=(5 * n_ae, 3.5), squeeze=False)
    axes = axes.flatten()
    for i, (sys_label, ae_info) in enumerate(ae_models.items()):
        axes[i].plot(ae_info["history"], color="navy")
        axes[i].set_title(f"{sys_label} Loss", fontsize=11, fontweight="bold")
        axes[i].set_xlabel("Epoch")
        axes[i].set_ylabel("MSE")
        axes[i].set_yscale("log")
        axes[i].grid(alpha=0.3)
    plt.suptitle("AutoEncoder Training Convergence",
                 fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    ae_loss_path = os.path.join(cfg.output_dir, "ae_training_loss.png")
    plt.savefig(ae_loss_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"   💾 Saved AE training loss plot: {ae_loss_path}")

print(f"\n✅ AutoEncoder complete for {len(ae_models)} subsystems.")


# %%
# =============================================================================
# Cell 12: Build Per-System Detailed Output File (UPDATED)
# =============================================================================
# Output format per system:
#   timestamp_utc | Downtime_flag |
#   System_Score | Score_Level (High/Low) | Adaptive_Threshold |
#   System_Confidence | System_Alarm |
#   S1-Invalid | S1-SQS | S1-EngA | S1-EngB | S1-Trust | ...
# =============================================================================
print("=" * 70)
print("BUILDING DETAILED PER-SYSTEM OUTPUT (with Trust, Confidence, Threshold)")
print("=" * 70)


def classify_score_level(
    system_score: pd.Series,
    threshold: np.ndarray,
) -> pd.Series:
    """
    Classify each timestamp as 'High' or 'Low' based on adaptive threshold.
      High → system_score >= adaptive_threshold
      Low  → system_score <  adaptive_threshold
    """
    score_arr = system_score.values
    levels = np.where(score_arr >= threshold, "High", "Low")
    return pd.Series(levels, index=system_score.index)


def enforce_downtime_zero(
    series: pd.Series,
    running_mask: pd.Series,
    fill_value=0,
) -> pd.Series:
    """Force a series to `fill_value` during downtime (where running=False)."""
    out = series.copy()
    out.loc[~running_mask] = fill_value
    return out


def build_detailed_system_output(
    df: pd.DataFrame,
    downtime: pd.Series,
    running: pd.Series,
    catalog: Dict[str, List[str]],
    ae_results: Dict[str, Dict],
    A: pd.DataFrame,
    B: pd.DataFrame,
    sqs: pd.DataFrame,
    base_bad: pd.DataFrame,
    sensor_trust: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build a wide DataFrame with hierarchical columns:
      timestamp_utc | downtime_flag |
      For each system:
        system_score | score_level | adaptive_threshold |
        system_confidence | system_alarm | baseline_sigma
      For each sensor:
        invalid_flag | sqs | engine_a | engine_b | trust
    """
    out = pd.DataFrame(index=df.index)
    out.index.name = "timestamp_utc"
    out["downtime_flag"] = downtime.astype(int)

    for sys_label in sorted(catalog.keys()):
        sensors = catalog[sys_label]

        if sys_label in ae_results:
            r = ae_results[sys_label]

            # ── System Score (zero during downtime) ──
            sys_score = enforce_downtime_zero(
                r["system_score"], running, fill_value=0.0
            )
            out[f"{sys_label}__System_Score"] = sys_score

            # ── Adaptive Threshold ──
            threshold_arr = r["threshold"]
            out[f"{sys_label}__Adaptive_Threshold"] = threshold_arr

            # ── Score Level: High / Low (based on threshold) ──
            score_level = classify_score_level(sys_score, threshold_arr)
            # During downtime → "Low" (no anomaly during downtime)
            score_level.loc[~running] = "Low"
            out[f"{sys_label}__Score_Level"] = score_level

            # ── System Confidence (zero during downtime) ──
            confidence = enforce_downtime_zero(
                r["confidence"], running, fill_value=0.0
            )
            out[f"{sys_label}__System_Confidence"] = confidence

            # ── System Alarm (ZERO during downtime) ──
            alarm = r["alarm"].copy()
            alarm.loc[~running] = False  # force alarm OFF during downtime
            out[f"{sys_label}__System_Alarm"] = alarm.astype(int)

            # ── Baseline Sigma (scalar, repeated) ──
            out[f"{sys_label}__Baseline_Sigma"] = r["baseline_sigma"]

        else:
            # No AE for this subsystem
            out[f"{sys_label}__System_Score"] = 0.0
            out[f"{sys_label}__Adaptive_Threshold"] = np.nan
            out[f"{sys_label}__Score_Level"] = "Low"
            out[f"{sys_label}__System_Confidence"] = 0.0
            out[f"{sys_label}__System_Alarm"] = 0
            out[f"{sys_label}__Baseline_Sigma"] = np.nan

        for s in sensors:
            prefix = f"{sys_label}__{s}"

            # Invalid flag
            if s in base_bad.columns:
                out[f"{prefix}__Invalid_Flag"] = base_bad[s].astype(int)
            else:
                out[f"{prefix}__Invalid_Flag"] = df[s].isna().astype(int)

            # SQS
            if s in sqs.columns:
                out[f"{prefix}__SQS"] = sqs[s]
            else:
                out[f"{prefix}__SQS"] = np.nan

            # Engine A
            if s in A.columns:
                out[f"{prefix}__Engine_A"] = A[s]
            else:
                out[f"{prefix}__Engine_A"] = np.nan

            # Engine B
            if s in B.columns:
                out[f"{prefix}__Engine_B"] = B[s]
            else:
                out[f"{prefix}__Engine_B"] = np.nan

            # Trust Classification
            if s in sensor_trust.columns:
                out[f"{prefix}__Trust"] = sensor_trust[s]
            else:
                out[f"{prefix}__Trust"] = "Unusable"

    return out


detailed_output = build_detailed_system_output(
    df, downtime, running, catalog, ae_results, A, B, sqs, base_bad, sensor_trust
)

# Save combined
detailed_path_csv = os.path.join(
    cfg.output_dir, "detailed_system_sensor_scores.csv"
)
detailed_path_parquet = os.path.join(
    cfg.output_dir, "detailed_system_sensor_scores.parquet"
)
detailed_output.to_csv(detailed_path_csv)
detailed_output.to_parquet(detailed_path_parquet)

print(f"   Detailed output shape: {detailed_output.shape}")
print(f"   💾 Saved: {detailed_path_csv}")
print(f"   💾 Saved: {detailed_path_parquet}")


# =============================================================================
# Build 7 separate files: EngA, EngB, Scores, SQS, Trust, Confidence, Alarm
# =============================================================================
print("\n--- Building 7 separate output files ---")


# --- 1) Engine A file ---
out_a = pd.DataFrame(index=df.index)
out_a.index.name = "timestamp_utc"
out_a["downtime_flag"] = downtime.astype(int)
for sys_label in sorted(catalog.keys()):
    for s in catalog[sys_label]:
        if s in A.columns:
            out_a[f"{sys_label}__{s}"] = A[s]
        else:
            out_a[f"{sys_label}__{s}"] = np.nan

a_csv = os.path.join(cfg.output_dir, "detailed_engine_a.csv")
a_parquet = os.path.join(cfg.output_dir, "detailed_engine_a.parquet")
out_a.to_csv(a_csv)
out_a.to_parquet(a_parquet)
print(f"   Engine A shape: {out_a.shape}")
print(f"   💾 Saved: {a_csv} / {a_parquet}")


# --- 2) Engine B file ---
out_b = pd.DataFrame(index=df.index)
out_b.index.name = "timestamp_utc"
out_b["downtime_flag"] = downtime.astype(int)
for sys_label in sorted(catalog.keys()):
    for s in catalog[sys_label]:
        if s in B.columns:
            out_b[f"{sys_label}__{s}"] = B[s]
        else:
            out_b[f"{sys_label}__{s}"] = np.nan

b_csv = os.path.join(cfg.output_dir, "detailed_engine_b.csv")
b_parquet = os.path.join(cfg.output_dir, "detailed_engine_b.parquet")
out_b.to_csv(b_csv)
out_b.to_parquet(b_parquet)
print(f"   Engine B shape: {out_b.shape}")
print(f"   💾 Saved: {b_csv} / {b_parquet}")


# --- 3) Subsystem Scores file (with Score_Level + Threshold) ---
out_sub = pd.DataFrame(index=df.index)
out_sub.index.name = "timestamp_utc"
out_sub["downtime_flag"] = downtime.astype(int)
for sys_label in sorted(catalog.keys()):
    if sys_label in ae_results:
        r = ae_results[sys_label]

        # Score (zero during downtime)
        sys_score = enforce_downtime_zero(
            r["system_score"], running, fill_value=0.0
        )
        out_sub[f"{sys_label}__System_Score"] = sys_score

        # Adaptive Threshold
        out_sub[f"{sys_label}__Adaptive_Threshold"] = r["threshold"]

        # Score Level
        score_level = classify_score_level(sys_score, r["threshold"])
        score_level.loc[~running] = "Low"
        out_sub[f"{sys_label}__Score_Level"] = score_level

        # Baseline Sigma
        out_sub[f"{sys_label}__Baseline_Sigma"] = r["baseline_sigma"]
    else:
        out_sub[f"{sys_label}__System_Score"] = 0.0
        out_sub[f"{sys_label}__Adaptive_Threshold"] = np.nan
        out_sub[f"{sys_label}__Score_Level"] = "Low"
        out_sub[f"{sys_label}__Baseline_Sigma"] = np.nan

sub_csv = os.path.join(cfg.output_dir, "detailed_subsystem_scores.csv")
sub_parquet = os.path.join(cfg.output_dir, "detailed_subsystem_scores.parquet")
out_sub.to_csv(sub_csv)
out_sub.to_parquet(sub_parquet)
print(f"   Subsystem Scores shape: {out_sub.shape}")
print(f"   💾 Saved: {sub_csv} / {sub_parquet}")


# --- 4) SQS file ---
out_sqs = pd.DataFrame(index=df.index)
out_sqs.index.name = "timestamp_utc"
out_sqs["downtime_flag"] = downtime.astype(int)
for sys_label in sorted(catalog.keys()):
    for s in catalog[sys_label]:
        if s in sqs.columns:
            out_sqs[f"{sys_label}__{s}"] = sqs[s]
        else:
            out_sqs[f"{sys_label}__{s}"] = np.nan

sqs_csv = os.path.join(cfg.output_dir, "detailed_sqs.csv")
sqs_parquet = os.path.join(cfg.output_dir, "detailed_sqs.parquet")
out_sqs.to_csv(sqs_csv)
out_sqs.to_parquet(sqs_parquet)
print(f"   SQS shape: {out_sqs.shape}")
print(f"   💾 Saved: {sqs_csv} / {sqs_parquet}")


# --- 5) Sensor Trust file ---
out_trust = pd.DataFrame(index=df.index)
out_trust.index.name = "timestamp_utc"
out_trust["downtime_flag"] = downtime.astype(int)
for sys_label in sorted(catalog.keys()):
    for s in catalog[sys_label]:
        if s in sensor_trust.columns:
            out_trust[f"{sys_label}__{s}"] = sensor_trust[s]
        else:
            out_trust[f"{sys_label}__{s}"] = "Unusable"

trust_csv = os.path.join(cfg.output_dir, "detailed_sensor_trust.csv")
trust_parquet = os.path.join(cfg.output_dir, "detailed_sensor_trust.parquet")
out_trust.to_csv(trust_csv)
out_trust.to_parquet(trust_parquet)
print(f"   Sensor Trust shape: {out_trust.shape}")
print(f"   💾 Saved: {trust_csv} / {trust_parquet}")


# --- 6) Subsystem Confidence file (zero during downtime) ---
out_conf = pd.DataFrame(index=df.index)
out_conf.index.name = "timestamp_utc"
out_conf["downtime_flag"] = downtime.astype(int)
for sys_label in sorted(catalog.keys()):
    if sys_label in ae_results:
        conf = enforce_downtime_zero(
            ae_results[sys_label]["confidence"], running, fill_value=0.0
        )
        out_conf[f"{sys_label}__Confidence"] = conf
    else:
        out_conf[f"{sys_label}__Confidence"] = 0.0

conf_csv = os.path.join(cfg.output_dir, "detailed_subsystem_confidence.csv")
conf_parquet = os.path.join(
    cfg.output_dir, "detailed_subsystem_confidence.parquet"
)
out_conf.to_csv(conf_csv)
out_conf.to_parquet(conf_parquet)
print(f"   Subsystem Confidence shape: {out_conf.shape}")
print(f"   💾 Saved: {conf_csv} / {conf_parquet}")


# --- 7) Subsystem Alarm file (ZERO during downtime enforced) ---
out_alarm = pd.DataFrame(index=df.index)
out_alarm.index.name = "timestamp_utc"
out_alarm["downtime_flag"] = downtime.astype(int)
for sys_label in sorted(catalog.keys()):
    if sys_label in ae_results:
        alarm_series = ae_results[sys_label]["alarm"].copy()
        # ── ENFORCE: alarm = 0 during downtime ──
        alarm_series.loc[~running] = False
        out_alarm[f"{sys_label}__Alarm"] = alarm_series.astype(int)

        # Also add score level for alarm context
        sys_score = enforce_downtime_zero(
            ae_results[sys_label]["system_score"], running, fill_value=0.0
        )
        score_level = classify_score_level(
            sys_score, ae_results[sys_label]["threshold"]
        )
        score_level.loc[~running] = "Low"
        out_alarm[f"{sys_label}__Score_Level_At_Alarm"] = score_level
    else:
        out_alarm[f"{sys_label}__Alarm"] = 0
        out_alarm[f"{sys_label}__Score_Level_At_Alarm"] = "Low"

alarm_csv = os.path.join(cfg.output_dir, "detailed_subsystem_alarms.csv")
alarm_parquet = os.path.join(
    cfg.output_dir, "detailed_subsystem_alarms.parquet"
)
out_alarm.to_csv(alarm_csv)
out_alarm.to_parquet(alarm_parquet)
print(f"   Subsystem Alarm shape: {out_alarm.shape}")
print(f"   💾 Saved: {alarm_csv} / {alarm_parquet}")


# ─────────────────────────────────────────────────────────────────────────────
# DOWNTIME ENFORCEMENT VERIFICATION
# ─────────────────────────────────────────────────────────────────────────────
print("\n--- Downtime enforcement verification ---")
n_downtime = int((~running).sum())
print(f"   Total downtime timestamps: {n_downtime}")

for sys_label in sorted(ae_results.keys()):
    r = ae_results[sys_label]

    # Verify score = 0 during downtime
    score_during_dt = r["system_score"].loc[~running]
    score_violations = int((score_during_dt != 0).sum())

    # Verify alarm = 0 during downtime
    alarm_during_dt = r["alarm"].loc[~running]
    alarm_violations = int(alarm_during_dt.sum())

    # Verify confidence = 0 during downtime
    conf_during_dt = r["confidence"].loc[~running]
    conf_violations = int((conf_during_dt != 0).sum())

    status = "✅" if (score_violations + alarm_violations + conf_violations) == 0 else "⚠️"
    print(f"   {status} {sys_label}: "
          f"score_violations={score_violations}, "
          f"alarm_violations={alarm_violations}, "
          f"confidence_violations={conf_violations}")


# ─────────────────────────────────────────────────────────────────────────────
# Preview columns structure
# ─────────────────────────────────────────────────────────────────────────────
sys_labels_in_output = sorted(catalog.keys())
print(f"\n   Systems in output: {sys_labels_in_output}")
for sys_label in sys_labels_in_output:
    sys_cols = [c for c in detailed_output.columns
                if c.startswith(f"{sys_label}__")]
    print(f"   {sys_label}: {len(sys_cols)} columns")
    if len(sys_cols) <= 16:
        for c in sys_cols:
            print(f"       {c}")
    else:
        for c in sys_cols[:10]:
            print(f"       {c}")
        print(f"       ... ({len(sys_cols) - 10} more)")


# ─────────────────────────────────────────────────────────────────────────────
# Summary table
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("SUBSYSTEM SUMMARY (with Threshold & Score Level)")
print("=" * 70)

summary_rows = []
for sys_label in sorted(catalog.keys()):
    n_sensors = len(catalog[sys_label])

    if sys_label in ae_results:
        r = ae_results[sys_label]
        n_ae_sensors = len(ae_models[sys_label]["sensors"])

        # Running-only stats
        run_mask = running.values
        score_run = r["system_score"].loc[running]
        thresh_run = r["threshold"][run_mask]

        score_mean = float(score_run.mean())
        thresh_mean = float(np.mean(thresh_run))
        conf_mean = float(r["confidence"].loc[running].mean())

        # Alarm count (only during running)
        alarm_run = r["alarm"].copy()
        alarm_run.loc[~running] = False
        n_alarms = int(alarm_run.sum())

        # Score level distribution (running only)
        score_level_run = classify_score_level(
            r["system_score"].loc[running],
            r["threshold"][run_mask]
        )
        n_high = int((score_level_run == "High").sum())
        n_low = int((score_level_run == "Low").sum())
        pct_high = 100.0 * n_high / max(n_high + n_low, 1)

        bsig = r["baseline_sigma"]

        summary_rows.append({
            "Subsystem": sys_label,
            "Sensors": f"{n_ae_sensors}/{n_sensors}",
            "Score_Mean": f"{score_mean:.4f}",
            "Thresh_Mean": f"{thresh_mean:.4f}",
            "High%": f"{pct_high:.1f}%",
            "High_Count": n_high,
            "Low_Count": n_low,
            "Confidence": f"{conf_mean:.4f}",
            "Baseline_σ": f"{bsig:.6f}",
            "Alarms": n_alarms,
        })
    else:
        summary_rows.append({
            "Subsystem": sys_label,
            "Sensors": f"0/{n_sensors}",
            "Score_Mean": "N/A",
            "Thresh_Mean": "N/A",
            "High%": "N/A",
            "High_Count": "N/A",
            "Low_Count": "N/A",
            "Confidence": "N/A",
            "Baseline_σ": "N/A",
            "Alarms": "N/A",
        })

summary_df = pd.DataFrame(summary_rows)
print(summary_df.to_string(index=False))

# Save summary
summary_csv = os.path.join(cfg.output_dir, "subsystem_summary.csv")
summary_df.to_csv(summary_csv, index=False)
print(f"\n   💾 Saved summary: {summary_csv}")

print("\n✅ Detailed system output complete "
      "(Score Level, Adaptive Threshold, Confidence, Alarm).")
print("   ⚡ All outputs enforce: Score=0, Alarm=0, Confidence=0 during DOWNTIME.")


