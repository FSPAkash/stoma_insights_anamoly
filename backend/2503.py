# %%
# =============================================================================
# Cell 1: Imports and Constants
# =============================================================================
from __future__ import annotations

import os
import shutil
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
    ae_min_sensors: int = 1
    ae_min_training_rows: int = 300
    ae_missing_max: float = 0.10
    ae_mean_abs_corr_min: float = 0.05
    ae_risk_sigma_factor: float = 3.0
    ae_slew_rate: float = 0.0005
    ae_gate_factor: float = 1.5
    ae_threshold_window: int = 100
    ae_on_delay: int = 3
    ae_off_delay: int = 5
    ae_baseline_n_samples: int = 500
    ae_baseline_sqs_min: float = 0.8
    ae_baseline_drift_max: float = 0.5
    ae_top_k_sensors: int = 5

    ae_confidence_weights: Dict[Tuple[str, str], float] = field(default_factory=lambda: {
        ("high", "Reliable"): 1.00,
        ("high", "Degraded"): 0.75,
        ("high", "Unusable"): 0.40,
        ("low", "Reliable"): 0.70,
        ("low", "Degraded"): 0.50,
        ("low", "Unusable"): 0.20,
    })

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
    beta_publish_dir: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_beta")
    BASELINE_FILE: str = "sensor_baseline_range_season_2024-2025.xlsx"

    #trust thresholds 
    trust_sqs_unusable: float = 0.60
    trust_sqs_reliable: float = 0.80
    trust_drift_reliable: float = 0.50
    trust_period_reliable: float = 0.50
    #   # --- Standalone Sensor Scoring (Step 16) ---
    standalone_sqs_min: float = 0.0          # min SQS to include sensor at timestamp
    standalone_on_delay: int = 3             # alarm on-delay (minutes)
    standalone_off_delay: int = 5            # alarm off-delay (minutes)
    standalone_threshold_window: int = 100   # rolling window for adaptive threshold
    standalone_slew_rate: float = 0.0005     # max threshold change per step
    standalone_gate_factor: float = 1.5      # anomaly gate (freeze threshold if exceeded)
    standalone_sigma_factor: float = 3.0     # threshold = mu_recent + 3 * sigma_baseline
    standalone_baseline_n_samples: int = 500 # healthy samples for baseline sigma
    standalone_baseline_sqs_min: float = 0.8 # SQS threshold for healthy samples
    standalone_baseline_drift_max: float = 0.5 # max drift score for healthy samples

    # --- Process Risk Fusion (Step 18) ---
    fusion_epsilon: float = 1e-8             # numerical stability constant
    fusion_significance_window: int = 1440   # rolling window for mu_hist / sigma_hist (minutes)
    


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

    # ✅ FIX: Read only the columns you need, nothing else
    needed_cols = [cfg.long_ts_col, cfg.long_sensor_col, cfg.long_value_col]

    import pyarrow.parquet as pq
    pf = pq.ParquetFile(cfg.parquet_path)

    print(f"   Total row groups: {pf.metadata.num_row_groups}")
    print(f"   Total rows      : {pf.metadata.num_rows:,}")

    chunks = []
    for i, batch in enumerate(pf.iter_batches(batch_size=500_000, columns=needed_cols)):
        chunk = batch.to_pandas()
        chunk[cfg.long_ts_col] = pd.to_datetime(chunk[cfg.long_ts_col], utc=True)
        chunk = chunk.dropna(subset=[cfg.long_ts_col, cfg.long_sensor_col])
        chunk[cfg.long_value_col] = pd.to_numeric(chunk[cfg.long_value_col], errors="coerce")
        chunks.append(chunk)
        print(f"   Batch {i+1}: {len(chunk):,} rows loaded")

    raw = pd.concat(chunks, ignore_index=True)
    del chunks  # free memory immediately
    print(f"   Raw rows: {len(raw):,}")
    print(f"   Unique sensors: {raw[cfg.long_sensor_col].nunique()}")
    print(f"   Time range: {raw[cfg.long_ts_col].min()} → {raw[cfg.long_ts_col].max()}")

    # ✅ Pivot with sort=False + inplace sorts (from previous fix)
    wide = raw.pivot_table(
        index=cfg.long_ts_col,
        columns=cfg.long_sensor_col,
        values=cfg.long_value_col,
        aggfunc="mean",
        sort=False,
    )
    del raw  # free long-format data before sort allocations
    wide.index.name = None
    wide.sort_index(axis=0, inplace=True)
    wide.sort_index(axis=1, inplace=True)

    # ✅ Downcast to float32 (~half the memory)
    wide = wide.astype("float32")

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
# Cell 10 : Dynamic System Discovery [Stability Plateau Threshold]
# =============================================================================
print("=" * 70)
print("DYNAMIC SYSTEM DISCOVERY [Stability Plateau Threshold]")
print("=" * 70)


def clean_for_clustering(
    df: pd.DataFrame,
    running_mask: pd.Series,
    sensor_trust: pd.DataFrame,        # from Cell 9.5
    null_threshold_pct: float,
    ffill_limit: int = 0,              # will be ignored anyway
) -> pd.DataFrame:
    """
    Prepare data for clustering — STRICT MODE:
    → If ANY sensor is 'Unusable' at a timestamp → drop the ENTIRE row.
    → Only running + all sensors Reliable/Degraded rows are kept.
    → Everything else unchanged (STATUS drop, high-null columns drop, no constant drop).
    """
    # 1. Start with only running timestamps
    df_running = df.loc[running_mask].copy()
    trust_running = sensor_trust.loc[running_mask]

    # 2. Find timestamps where AT LEAST ONE sensor is Unusable
    any_unusable = (trust_running == "Unusable").any(axis=1)
    n_dropped_rows = any_unusable.sum()
    
    # 3. Keep only rows where ALL sensors are Reliable or Degraded
    keep_rows = ~any_unusable
    df_clean = df_running.loc[keep_rows].copy()

    print(f"   Strict unusable row removal:")
    print(f"      • Running rows: {len(df_running)}")
    print(f"      • Rows with ≥1 Unusable sensor: {n_dropped_rows}")
    print(f"      • Rows kept (all sensors Reliable/Degraded): {len(df_clean)}")

    if len(df_clean) == 0:
        print("   No rows survived strict filtering! Clustering will be empty.")
        return pd.DataFrame()  # empty

    # 4. Drop STATUS columns
    status_cols = [c for c in df_clean.columns if "STATUS" in c.upper()]
    df_clean = df_clean.drop(columns=status_cols, errors="ignore")

    # 5. Drop columns with too many nulls (safety)
    null_pct = df_clean.isnull().mean() * 100
    high_null = null_pct[null_pct > null_threshold_pct].index.tolist()
    df_clean = df_clean.drop(columns=high_null, errors="ignore")

    print(f"   Dropped {len(status_cols)} STATUS cols, {len(high_null)} high-null cols")

    # NO constant column removal (as requested)
    # NO ffill, NO median fill — data is already clean per your rule

    print(f"   Final clustering input: {df_clean.shape[0]} rows × {df_clean.shape[1]} cols")
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
        print("   ⚠️ Fewer than 2 columns — cannot cluster.")
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

# ── Pass sensor_trust from Cell 9.5 into the clustering prep ──
df_for_clustering = clean_for_clustering(
    df, running,
    sensor_trust=sensor_trust,            # ← from Cell 9.5 (untouched)
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

def build_sensor_config_export(
    sensor_cfg: pd.DataFrame,
    df: pd.DataFrame,
    running_mask: pd.Series,
) -> pd.DataFrame:
    """Augment sensor baseline config with missingness stats for the beta API."""
    n_running_total = int(running_mask.sum())
    running_df = df.loc[running_mask]
    present_running = running_df.notna().sum(axis=0)

    sensor_config_export = sensor_cfg.copy()
    sensor_config_export["n_running"] = n_running_total
    sensor_config_export["n_present_running"] = sensor_config_export["sensor"].map(
        lambda s: int(present_running.get(s, 0))
    )
    sensor_config_export["missing_pct"] = sensor_config_export["sensor"].map(
        lambda s: float(1.0 - (present_running.get(s, 0) / max(n_running_total, 1)))
    )
    return sensor_config_export


def export_beta_chart_inputs(
    df: pd.DataFrame,
    catalog: Dict[str, List[str]],
    output_dir: str,
) -> Tuple[str, List[str]]:
    """
    Export cleaned minute-grid sensor data for the beta subsystem behavior charts.

    - df_chart_data.csv: full cleaned wide table with timestamp_utc
    - sensor_values_{system}.csv: per-subsystem sensor subsets
    """
    chart_df = df.copy()
    chart_df.index.name = "timestamp_utc"

    df_chart_data_path = os.path.join(output_dir, "df_chart_data.csv")
    chart_df.reset_index().to_csv(df_chart_data_path, index=False)

    sensor_value_paths: List[str] = []
    for sys_label, sensors in sorted(catalog.items()):
        keep_cols = [s for s in sensors if s in chart_df.columns]
        if not keep_cols:
            continue
        sys_path = os.path.join(output_dir, f"sensor_values_{sys_label}.csv")
        chart_df[keep_cols].reset_index().to_csv(sys_path, index=False)
        sensor_value_paths.append(sys_path)

    return df_chart_data_path, sensor_value_paths


def publish_beta_artifacts(
    output_dir: str,
    beta_publish_dir: str,
) -> List[str]:
    """Mirror the entire pipeline output directory into data_beta/ for the beta API."""
    if os.path.exists(beta_publish_dir):
        shutil.rmtree(beta_publish_dir)
    shutil.copytree(output_dir, beta_publish_dir)

    published: List[str] = []
    for root, _dirs, files in os.walk(beta_publish_dir):
        for f in files:
            published.append(os.path.join(root, f))
    return published


os.makedirs(cfg.output_dir, exist_ok=True)
system_summary_path = os.path.join(cfg.output_dir, "system_summary.csv")
system_detail_path = os.path.join(cfg.output_dir, "system_detail.csv")
dynamic_catalog_path = os.path.join(cfg.output_dir, "dynamic_catalog.csv")
dynamic_weights_path = os.path.join(cfg.output_dir, "dynamic_weights.csv")
sensor_config_path = os.path.join(cfg.output_dir, "sensor_config.csv")

if not sys_summary.empty:
    sys_summary.to_csv(system_summary_path, index=False)
if not sys_detail.empty:
    sys_detail.to_csv(system_detail_path, index=False)

sensor_config_export = build_sensor_config_export(sensor_cfg, df, running)
sensor_config_export.to_csv(sensor_config_path, index=False)

dynamic_weights_rows = []
for label, weight in sorted(dynamic_weights.items()):
    dynamic_weights_rows.append({"type": "fusion", "key": label, "weight": weight})
for label, weight in sorted(dynamic_risk_weights.items()):
    dynamic_weights_rows.append({"type": "risk", "key": label, "weight": weight})
pd.DataFrame(dynamic_weights_rows).to_csv(dynamic_weights_path, index=False)

catalog_rows = []
for label, sensors in catalog.items():
    for s in sensors:
        catalog_rows.append({"system": label, "sensor": s})
pd.DataFrame(catalog_rows).to_csv(dynamic_catalog_path, index=False)

df_chart_data_path, sensor_value_paths = export_beta_chart_inputs(
    df, catalog, cfg.output_dir
)
print(f"   Saved artifacts to {cfg.output_dir}")
print(f"   Saved: {sensor_config_path}")
print(f"   Saved: {dynamic_weights_path}")
print(f"   Saved: {df_chart_data_path}")
print(f"   Saved per-system raw feeds: {len(sensor_value_paths)} files")

# %%
# =============================================================================
# Cell 11: AutoEncoder Subsystem Scoring (v3 – Sensor Rankings + Downtime Exclusion)
# =============================================================================
# Deep AutoEncoder with Directional Residual Operator, Reference Baseline
# Estimation, Subsystem Confidence, Robust Adaptive Thresholding,
# and PER-TIMESTAMP SENSOR CONTRIBUTION RANKING.
#
# KEY CHANGES from v2:
#   1. Sensor Contribution Ranking: at each timestamp, sensors are ranked
#      by their sigma-scaled directional error contribution.
#   2. Downtime Exclusion: instead of computing scores then forcing to zero,
#      downtime timestamps are EXCLUDED from all computations upfront.
#      Output rows for downtime get NaN/None, not artificial zeros.
# =============================================================================

print("=" * 70)
print("AUTOENCODER SUBSYSTEM SCORING (v3 – Sensor Rankings + Downtime Exclusion)")
print("=" * 70)

# ─────────────────────────────────────────────────────────────────────────────
# DeepAutoEncoder Model (unchanged)
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
# Directional Residual Helpers (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _get_direction(sensor_name: str) -> str:
    return ANOMALY_DIRECTION_CONFIG.get(sensor_name, "both")


def directional_residual(raw_residual: np.ndarray, mode: str) -> np.ndarray:
    if mode == "pos":
        return np.maximum(0.0, raw_residual)
    elif mode == "neg":
        return np.maximum(0.0, -raw_residual)
    else:
        return np.abs(raw_residual)


# ─────────────────────────────────────────────────────────────────────────────
# Reference Baseline Estimation (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def compute_reference_baseline(
    risk_array: np.ndarray,
    sqs: pd.DataFrame,
    sensors: List[str],
    n_samples: int = 500,
    sqs_min: float = 0.8,
    drift_max: float = 0.5,
) -> float:
    """
    Reference Baseline from RUNNING-ONLY risk array.
    (No running_mask needed here since risk_array is already running-only.)
    """
    n_total = len(risk_array)
    healthy_mask = np.ones(n_total, dtype=bool)

    # SQS filter (on running-only subset)
    available_sqs_sensors = [s for s in sensors if s in sqs.columns]
    if available_sqs_sensors:
        mean_sqs = sqs[available_sqs_sensors].mean(axis=1).values[:n_total]
        healthy_mask &= np.nan_to_num(mean_sqs, nan=0.0) >= sqs_min

    # Drift filter
    try:
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
    except Exception:
        pass

    healthy_indices = np.where(healthy_mask)[0]
    if len(healthy_indices) < 20:
        healthy_indices = np.arange(min(n_samples, n_total))
        if len(healthy_indices) < 10:
            return float(np.std(risk_array)) if float(np.std(risk_array)) > 0 else 1e-6

    baseline_indices = healthy_indices[:min(n_samples, len(healthy_indices))]
    baseline_risk = risk_array[baseline_indices]

    sigma_baseline = float(np.std(baseline_risk))
    return sigma_baseline if sigma_baseline > 1e-8 else 1e-6


# ─────────────────────────────────────────────────────────────────────────────
# Robust Adaptive Threshold (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def robust_adaptive_threshold(
    risk: np.ndarray,
    baseline_sigma: float,
    window: int = 100,
    slew_rate: float = 0.0005,
    gate: float = 1.5,
    sigma_factor: float = 3.0,
) -> np.ndarray:
    n = len(risk)
    threshold = np.zeros(n)
    seed = float(np.nanmean(risk[:min(window, n)])) + sigma_factor * baseline_sigma
    threshold[:min(window, n)] = seed

    for t in range(min(window, n), n):
        target = (float(np.nanmean(risk[max(0, t - window):t]))
                  + sigma_factor * baseline_sigma)
        if risk[t] > gate * threshold[t - 1]:
            target = threshold[t - 1]
        change = np.clip(target - threshold[t - 1], -slew_rate, slew_rate)
        threshold[t] = threshold[t - 1] + change

    return threshold


# ─────────────────────────────────────────────────────────────────────────────
# Trust Weight Table
# ─────────────────────────────────────────────────────────────────────────────

TRUST_WEIGHT_TABLE = {
    ("high", "Reliable"):  1.00,
    ("high", "Degraded"):  0.75,
    ("high", "Unusable"):  0.40,
    ("low",  "Reliable"):  0.70,
    ("low",  "Degraded"):  0.50,
    ("low",  "Unusable"):  0.20,
}
if cfg.ae_confidence_weights:
    TRUST_WEIGHT_TABLE.update(cfg.ae_confidence_weights)


# ─────────────────────────────────────────────────────────────────────────────
# Subsystem Confidence (operates on running-only data)
# ─────────────────────────────────────────────────────────────────────────────

def compute_subsystem_confidence(
    system_score: np.ndarray,
    threshold: np.ndarray,
    sensors: List[str],
    sensor_trust_running: pd.DataFrame,  # already filtered to running rows
) -> np.ndarray:
    """
    Compute Subsystem Confidence for RUNNING timestamps only.
    Returns np.ndarray of length = number of running timestamps.
    """
    n = len(system_score)
    confidence = np.zeros(n, dtype=float)

    available_sensors = [s for s in sensors if s in sensor_trust_running.columns]
    if not available_sensors:
        return confidence

    M = len(available_sensors)

    trust_arrays = {}
    for s in available_sensors:
        trust_arrays[s] = sensor_trust_running[s].values

    for t in range(n):
        score_level = "high" if system_score[t] >= threshold[t] else "low"
        weight_sum = 0.0
        for s in available_sensors:
            trust_val = str(trust_arrays[s][t]) if t < len(trust_arrays[s]) else "Unusable"
            if trust_val in ("Reliable", "reliable"):
                trust_key = "Reliable"
            elif trust_val in ("Degraded", "degraded"):
                trust_key = "Degraded"
            else:
                trust_key = "Unusable"
            w = TRUST_WEIGHT_TABLE.get((score_level, trust_key), 0.0)
            weight_sum += w
        confidence[t] = weight_sum / M

    return confidence


# ─────────────────────────────────────────────────────────────────────────────
# Alarm Persistence (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _on_off_delay_alarm(
    risk: np.ndarray, threshold: np.ndarray, on_delay: int, off_delay: int
) -> np.ndarray:
    n = len(risk)
    alarms = np.zeros(n, dtype=bool)
    count = 0
    streak_start = 0
    active = False

    for i in range(n):
        is_high = risk[i] >= threshold[i]
        if not active:
            if is_high:
                if count == 0:
                    streak_start = i
                count += 1
                if count >= on_delay:
                    active = True
                    alarms[streak_start:i + 1] = True
                    count = 0
            else:
                count = 0
        else:
            alarms[i] = True
            if not is_high:
                if count == 0:
                    streak_start = i
                count += 1
                if count >= off_delay:
                    alarms[streak_start:i + 1] = False
                    active = False
                    count = 0
            else:
                count = 0
    return alarms


# ─────────────────────────────────────────────────────────────────────────────
# Sensor Selection (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def select_ae_candidates(
    df: pd.DataFrame,
    running_mask: pd.Series,
    sensors: List[str],
    missing_max: float,
    min_abs_corr: float,
) -> List[str]:
    present = [s for s in sensors if s in df.columns]
    if len(present) < 2:
        return present

    running_df = df.loc[running_mask, present].copy()
    if running_df.shape[0] < 100:
        return []

    good = [s for s in present
            if running_df[s].isna().mean() <= missing_max
            and running_df[s].var(skipna=True) > 0]

    if len(good) < 2:
        return good

    filled = running_df[good].ffill().bfill()
    mac = filled.corr().abs()
    mac_mean = mac.where(~np.eye(len(good), dtype=bool)).mean()

    selected = [s for s in good if float(mac_mean.get(s, 0.0)) >= min_abs_corr]

    if len(selected) < 2 and len(good) >= 2:
        corr_vals = [(s, float(mac_mean.get(s, 0.0))) for s in good]
        corr_vals.sort(key=lambda x: x[1], reverse=True)
        selected = [s for s, _ in corr_vals[:2]]

    return selected


# ─────────────────────────────────────────────────────────────────────────────
# NEW: Per-Sensor Contribution & Ranking
# ─────────────────────────────────────────────────────────────────────────────

def compute_sensor_contributions(
    raw_residuals: np.ndarray,
    sensors: List[str],
    baseline_sigmas: Dict[str, float],
    sigma_factor: float,
) -> pd.DataFrame:
    """
    Compute per-sensor sigma-scaled directional contribution at each timestamp.
    Returns DataFrame with columns = sensors, values = contribution scores.
    """
    n = raw_residuals.shape[0]
    contributions = np.zeros((n, len(sensors)))

    for j, s in enumerate(sensors):
        direction = _get_direction(s)
        filtered = directional_residual(raw_residuals[:, j], direction)
        sigma = baseline_sigmas[s]
        contributions[:, j] = filtered / (sigma_factor * sigma + EPS)

    return pd.DataFrame(contributions, columns=sensors)


def build_sensor_rankings(
    contributions_df: pd.DataFrame,
    top_k: int = 5,
) -> pd.DataFrame:
    """
    At each timestamp, rank sensors by their contribution (descending).

    Returns DataFrame with columns:
      Rank_1_Sensor, Rank_1_Score, Rank_2_Sensor, Rank_2_Score, ...
    up to top_k.
    """
    n = len(contributions_df)
    sensors = contributions_df.columns.tolist()
    k = min(top_k, len(sensors))

    # Pre-allocate result columns
    rank_data = {}
    for r in range(1, k + 1):
        rank_data[f"Rank_{r}_Sensor"] = [""] * n
        rank_data[f"Rank_{r}_Score"] = [0.0] * n
        rank_data[f"Rank_{r}_Pct"] = [0.0] * n  # % contribution

    values = contributions_df.values  # (n, n_sensors)

    for t in range(n):
        row = values[t]
        total = float(np.sum(row))

        # Sort indices by descending contribution
        sorted_idx = np.argsort(-row)

        for r in range(k):
            idx = sorted_idx[r]
            rank_data[f"Rank_{r+1}_Sensor"][t] = sensors[idx]
            rank_data[f"Rank_{r+1}_Score"][t] = float(row[idx])
            rank_data[f"Rank_{r+1}_Pct"][t] = (
                float(row[idx]) / total * 100.0 if total > EPS else 0.0
            )

    return pd.DataFrame(rank_data, index=contributions_df.index)


# ─────────────────────────────────────────────────────────────────────────────
# AE Risk Array Computation (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _compute_ae_risk_array(
    raw_residuals: np.ndarray,
    sensors: List[str],
    baseline_sigmas: Dict[str, float],
    sigma_factor: float,
) -> np.ndarray:
    n = raw_residuals.shape[0]
    sigma_scores = np.zeros((n, len(sensors)))

    for j, s in enumerate(sensors):
        direction = _get_direction(s)
        filtered = directional_residual(raw_residuals[:, j], direction)
        sigma = baseline_sigmas[s]
        sigma_scores[:, j] = filtered / (sigma_factor * sigma + EPS)

    raw_risk = np.sum(sigma_scores, axis=1)
    rmin, rmax = float(np.nanmin(raw_risk)), float(np.nanmax(raw_risk))
    if rmax - rmin < EPS:
        return np.zeros(n)
    return (raw_risk - rmin) / (rmax - rmin + EPS)


# ─────────────────────────────────────────────────────────────────────────────
# Train AE (unchanged except minor)
# ─────────────────────────────────────────────────────────────────────────────

def train_ae_subsystem(
    df: pd.DataFrame,
    running_mask: pd.Series,
    sensors: List[str],
    cfg,
) -> Optional[Dict]:
    candidates = select_ae_candidates(
        df, running_mask, sensors, cfg.ae_missing_max, cfg.ae_mean_abs_corr_min
    )

    # if len(candidates) < cfg.ae_min_sensors:
    #     return None

    running_df = df.loc[running_mask, candidates].ffill().bfill().dropna()
    if len(running_df) < cfg.ae_min_training_rows:
        return None

    scaler = StandardScaler()
    X_train = scaler.fit_transform(running_df.values)
    train_tensor = torch.FloatTensor(X_train)

    input_dim = X_train.shape[1]
    latent_dim = min(cfg.ae_latent_dim, max(1, input_dim // 2))
    if latent_dim < 1:
        latent_dim = 1

    model = DeepAutoEncoder(input_dim, latent_dim)
    optimizer = optim.Adam(model.parameters(), lr=cfg.ae_lr)
    criterion = nn.MSELoss()

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

    with torch.no_grad():
        recon_train = model(train_tensor).numpy()
    raw_resid_train = X_train - recon_train

    baseline_sigmas = {}
    for j, s in enumerate(candidates):
        direction = _get_direction(s)
        filtered = directional_residual(raw_resid_train[:, j], direction)
        sigma = float(np.std(filtered)) if float(np.std(filtered)) > 0 else 1e-6
        baseline_sigmas[s] = sigma

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
# Score AE: RUNNING-ONLY computation, then map back to full index
# ─────────────────────────────────────────────────────────────────────────────

def score_ae_subsystem(
    df: pd.DataFrame,
    running_mask: pd.Series,
    ae_info: Dict,
    cfg,
    sqs_df: pd.DataFrame,
    sensor_trust_df: pd.DataFrame,
) -> Dict:
    """
    Score ONLY running timestamps. Downtime timestamps get NaN naturally
    because we never compute them — no artificial zeroing needed.

    Returns dict with:
      - system_score:        pd.Series [0,1] (NaN during downtime)
      - per_sensor_sigma:    pd.DataFrame of sigma-scaled directional errors
      - sensor_contributions: pd.DataFrame (running rows only, mapped to full index)
      - sensor_rankings:     pd.DataFrame with Rank_1_Sensor, Rank_1_Score, etc.
      - threshold:           pd.Series adaptive threshold (NaN during downtime)
      - alarm:               pd.Series boolean alarm (False during downtime)
      - confidence:          pd.Series [0,1] (NaN during downtime)
      - baseline_sigma:      float reference baseline sigma
      - score_level:         pd.Series "High"/"Low" (NaN during downtime)
    """
    model = ae_info["model"]
    scaler = ae_info["scaler"]
    sensors = ae_info["sensors"]
    baseline_sigmas = ae_info["baseline_sigmas"]
    full_index = df.index

    # ──────────────────────────────────────────────────────────────────────
    # STEP 1: Extract RUNNING-ONLY data
    # ──────────────────────────────────────────────────────────────────────
    running_idx = running_mask[running_mask].index
    running_df = df.loc[running_idx, sensors].ffill().bfill()

    # Handle any remaining NaN after ffill/bfill (edge case)
    running_df = running_df.fillna(0.0)

    X_running = scaler.transform(running_df.values)
    running_tensor = torch.FloatTensor(X_running)

    model.eval()
    with torch.no_grad():
        recon_running = model(running_tensor).numpy()

    raw_resid_running = X_running - recon_running

    # ──────────────────────────────────────────────────────────────────────
    # STEP 2: Per-sensor contributions (RUNNING ONLY)
    # ──────────────────────────────────────────────────────────────────────
    contributions_running = compute_sensor_contributions(
        raw_resid_running, sensors, baseline_sigmas, cfg.ae_risk_sigma_factor
    )
    contributions_running.index = running_idx

    # ──────────────────────────────────────────────────────────────────────
    # STEP 3: Sensor rankings (RUNNING ONLY)
    # ──────────────────────────────────────────────────────────────────────
    rankings_running = build_sensor_rankings(
        contributions_running,
        top_k=min(cfg.ae_top_k_sensors, len(sensors)),
    )
    rankings_running.index = running_idx

    # ──────────────────────────────────────────────────────────────────────
    # STEP 4: Global risk score (RUNNING ONLY)
    # ──────────────────────────────────────────────────────────────────────
    risk_running = _compute_ae_risk_array(
        raw_resid_running, sensors, baseline_sigmas, cfg.ae_risk_sigma_factor
    )

    # ──────────────────────────────────────────────────────────────────────
    # STEP 5: Reference Baseline (from RUNNING data → already excluded DT)
    # ──────────────────────────────────────────────────────────────────────
    sqs_running = sqs_df.loc[running_idx] if running_idx[0] in sqs_df.index else sqs_df.iloc[:len(running_idx)]

    ref_baseline_sigma = compute_reference_baseline(
        risk_array=risk_running,
        sqs=sqs_running,
        sensors=sensors,
        n_samples=cfg.ae_baseline_n_samples,
        sqs_min=cfg.ae_baseline_sqs_min,
        drift_max=cfg.ae_baseline_drift_max,
    )

    # ──────────────────────────────────────────────────────────────────────
    # STEP 6: Adaptive threshold (RUNNING ONLY)
    # ──────────────────────────────────────────────────────────────────────
    threshold_running = robust_adaptive_threshold(
        risk_running,
        ref_baseline_sigma,
        window=cfg.ae_threshold_window,
        slew_rate=cfg.ae_slew_rate,
        gate=cfg.ae_gate_factor,
        sigma_factor=cfg.ae_risk_sigma_factor,
    )

    # ──────────────────────────────────────────────────────────────────────
    # STEP 7: Alarm persistence (RUNNING ONLY — no DT contamination)
    # ──────────────────────────────────────────────────────────────────────
    alarm_running = _on_off_delay_alarm(
        risk_running, threshold_running, cfg.ae_on_delay, cfg.ae_off_delay
    )

    # ──────────────────────────────────────────────────────────────────────
    # STEP 8: Subsystem Confidence (RUNNING ONLY)
    # ──────────────────────────────────────────────────────────────────────
    sensor_trust_running = sensor_trust_df.loc[running_idx]

    confidence_running = compute_subsystem_confidence(
        system_score=risk_running,
        threshold=threshold_running,
        sensors=sensors,
        sensor_trust_running=sensor_trust_running,
    )

    # ──────────────────────────────────────────────────────────────────────
    # STEP 9: Score Level (RUNNING ONLY)
    # ──────────────────────────────────────────────────────────────────────
    score_level_running = np.where(
        risk_running >= threshold_running, "High", "Low"
    )

    # ──────────────────────────────────────────────────────────────────────
    # STEP 10: Map results back to FULL INDEX (DT rows = NaN / False)
    # ──────────────────────────────────────────────────────────────────────

    # System Score: NaN during downtime
    system_score = pd.Series(np.nan, index=full_index, dtype=float)
    system_score.loc[running_idx] = np.clip(risk_running, 0, 1)

    # Per-sensor sigma scores (full index, NaN during DT)
    per_sensor_sigma = pd.DataFrame(
        np.nan, index=full_index, columns=sensors, dtype=float
    )
    for j, s in enumerate(sensors):
        direction = _get_direction(s)
        filtered = directional_residual(raw_resid_running[:, j], direction)
        sigma = baseline_sigmas[s]
        per_sensor_sigma.loc[running_idx, s] = filtered / (
            cfg.ae_risk_sigma_factor * sigma + EPS
        )

    # Sensor contributions (full index, NaN during DT)
    sensor_contributions_full = pd.DataFrame(
        np.nan, index=full_index, columns=sensors, dtype=float
    )
    sensor_contributions_full.loc[running_idx] = contributions_running.values

    # Sensor rankings (full index, empty strings / NaN during DT)
    ranking_cols = rankings_running.columns.tolist()
    sensor_rankings_full = pd.DataFrame(index=full_index, columns=ranking_cols)
    # Initialize with appropriate defaults
    for c in ranking_cols:
        if "Sensor" in c:
            sensor_rankings_full[c] = ""
        else:
            sensor_rankings_full[c] = np.nan
    sensor_rankings_full.loc[running_idx] = rankings_running.values

    # Threshold (NaN during DT)
    threshold_full = pd.Series(np.nan, index=full_index, dtype=float)
    threshold_full.loc[running_idx] = threshold_running

    # Alarm (False during DT — naturally, since we never computed it)
    alarm_full = pd.Series(False, index=full_index, dtype=bool)
    alarm_full.loc[running_idx] = alarm_running

    # Confidence (NaN during DT)
    confidence_full = pd.Series(np.nan, index=full_index, dtype=float)
    confidence_full.loc[running_idx] = confidence_running

    # Score Level (NaN during DT — not "Low", but genuinely not computed)
    score_level_full = pd.Series(np.nan, index=full_index, dtype=object)
    score_level_full.loc[running_idx] = score_level_running

    return {
        "system_score": system_score,
        "per_sensor_sigma": per_sensor_sigma,
        "sensor_contributions": sensor_contributions_full,
        "sensor_rankings": sensor_rankings_full,
        "threshold": threshold_full,
        "alarm": alarm_full,
        "confidence": confidence_full,
        "baseline_sigma": ref_baseline_sigma,
        "score_level": score_level_full,
        "risk_raw_running": risk_running,  # for diagnostics
    }


# =================== TRAIN & SCORE ALL SUBSYSTEMS ===================

ae_models: Dict[str, Dict] = {}
ae_results: Dict[str, Dict] = {}

for sys_label, sensors in catalog.items():
    if sys_label == "ISOLATED":
        print(f"   {sys_label}: skipped (no correlated group for AE)")
        continue

    present = [s for s in sensors if s in df.columns]

    # if len(present) < cfg.ae_min_sensors:
    #     print(len(present))
    #     print(f"   {sys_label}: skipped ({len(present)} sensors < "
    #           f"min {cfg.ae_min_sensors})")
    #     continue

    print(f"\n   Training AE for {sys_label} ({len(present)} sensors)...")
    ae_info = train_ae_subsystem(df, running, present, cfg)

    if ae_info is None:
        print(f"   {sys_label}: AE training failed "
              f"(insufficient data/candidates)")
        continue

    ae_models[sys_label] = ae_info

    ae_result = score_ae_subsystem(
        df, running, ae_info, cfg,
        sqs_df=sqs,
        sensor_trust_df=sensor_trust,
    )
    ae_results[sys_label] = ae_result

    # Stats (running-only)
    run_score = ae_result["system_score"].dropna()
    n_alarms = int(ae_result["alarm"].sum())
    score_mean = float(run_score.mean())
    conf_mean = float(ae_result["confidence"].dropna().mean())
    final_loss = ae_info["history"][-1] if ae_info["history"] else np.nan
    baseline_sig = ae_result["baseline_sigma"]

    # Score level stats
    sl = ae_result["score_level"].dropna()
    n_high = int((sl == "High").sum())
    pct_high = 100.0 * n_high / max(len(sl), 1)

    print(f"   ✅ {sys_label} AE: {len(ae_info['sensors'])} sensors → "
          f"latent_dim={ae_info['latent_dim']} | "
          f"final_loss={final_loss:.6f} | "
          f"score_mean(running)={score_mean:.4f} | "
          f"confidence_mean={conf_mean:.4f} | "
          f"baseline_σ={baseline_sig:.6f} | "
          f"High%={pct_high:.1f}% | "
          f"AE_alarms={n_alarms}")

    # Preview top contributing sensors (first alarm timestamp)
    alarm_times = ae_result["alarm"][ae_result["alarm"]].index
    if len(alarm_times) > 0:
        first_alarm = alarm_times[0]
        rank_row = ae_result["sensor_rankings"].loc[first_alarm]
        top_sensor = rank_row.get("Rank_1_Sensor", "?")
        top_score = rank_row.get("Rank_1_Score", 0.0)
        top_pct = rank_row.get("Rank_1_Pct", 0.0)
        print(f"       First alarm at {first_alarm}: "
              f"Top contributor = {top_sensor} "
              f"(score={top_score:.3f}, {top_pct:.1f}%)")

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
print(f"   ⚡ Downtime timestamps are EXCLUDED (NaN), not forced to zero.")
print(f"   📊 Sensor rankings generated at every running timestamp.")

# %%
# =============================================================================
# Cell 11a: Adaptive Decision Boundary – Per-Subsystem Visualization
# =============================================================================
# Reads directly from ae_results and ae_models (populated by Cell 9).
# Plots for each subsystem:
#   - Stacked area of per-sensor sigma contributions (root-cause color fill)
#   - Global Risk Score line
#   - Inertial Robust Threshold line
#   - Confirmed alarm shading
#   - Downtime NaN gaps are handled automatically by matplotlib
# =============================================================================

print("=" * 70)
print("CELL 9a: ADAPTIVE DECISION BOUNDARY (Per-Subsystem)")
print("=" * 70)

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns

# ── Plot style ────────────────────────────────────────────────────────────────
plt.style.use("seaborn-v0_8-paper")
plt.rcParams.update({
    "font.family":      "serif",
    "font.size":        11,
    "axes.labelsize":   12,
    "axes.titlesize":   13,
    "xtick.labelsize":  10,
    "ytick.labelsize":  10,
    "legend.fontsize":   9,
    "grid.alpha":        0.4,
    "grid.linestyle":   "--",
})

# ── Guard ─────────────────────────────────────────────────────────────────────
if not ae_results:
    print("⚠  ae_results is empty – run Cell 9 first.")
else:
    subsystems = list(ae_results.keys())
    n_sys = len(subsystems)

    fig, axes = plt.subplots(
        n_sys, 1,
        figsize=(18, 4.5 * n_sys),
        squeeze=False,
        sharex=False,
    )

    for ax, sys_label in zip(axes.flatten(), subsystems):
        result  = ae_results[sys_label]
        sensors = ae_models[sys_label]["sensors"]

        # ── Pull series (NaN during downtime → gaps in plot) ──────────────
        risk_score  = result["system_score"]        # pd.Series, full index
        threshold   = result["threshold"]            # pd.Series, full index
        alarm       = result["alarm"]                # pd.Series bool
        contrib_df  = result["sensor_contributions"] # pd.DataFrame, full index

        # ── Normalise contributions so the stack ≤ risk_score height ──────
        # contrib_df columns are already sigma-scaled; re-normalise to risk.
        total_contrib = contrib_df[sensors].sum(axis=1).replace(0, np.nan)
        norm_contrib  = contrib_df[sensors].div(total_contrib, axis=0).multiply(
            risk_score, axis=0
        )

        # ── Color palette (one color per sensor) ──────────────────────────
        colors = sns.color_palette("Spectral", len(sensors))

        # ── Stacked area (sensor contributions) ───────────────────────────
        # Work on running-only rows; gaps remain as NaN-breaks naturally.
        x = norm_contrib.index
        stacks = [norm_contrib[s].values for s in sensors]
        ax.stackplot(
            x, stacks,
            labels=sensors,
            alpha=0.55,
            colors=colors,
            edgecolor="grey",
            linewidth=0.15,
        )

        # ── Risk Score ────────────────────────────────────────────────────
        ax.plot(
            risk_score.index, risk_score.values,
            color="#1a252f", linewidth=1.3,
            label=r"Global Risk Score ($\mathcal{R}$)",
            alpha=0.92, zorder=8,
        )

        # ── Inertial Threshold ────────────────────────────────────────────
        ax.plot(
            threshold.index, threshold.values,
            color="#e67e22", linestyle="--", linewidth=2.2,
            label=r"Inertial Threshold ($T_t$)",
            zorder=10,
        )

        # ── Alarm shading ─────────────────────────────────────────────────
        # Fill between 0 and 1.05 wherever alarm is True
        ax.fill_between(
            alarm.index, 0, 1.05,
            where=alarm.values,
            color="red", alpha=0.10,
            label="Confirmed Alarm",
            zorder=1,
        )

        # ── Alarm event count annotation ──────────────────────────────────
        n_alarms = int(alarm.sum())
        sl        = result["score_level"].dropna()
        pct_high  = 100.0 * int((sl == "High").sum()) / max(len(sl), 1)
        ax.text(
            0.01, 0.97,
            f"Alarms: {n_alarms} samples | High%: {pct_high:.1f}%",
            transform=ax.transAxes,
            fontsize=9, va="top", ha="left",
            color="#2c3e50",
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="none", alpha=0.7),
        )

        # ── Axes formatting ───────────────────────────────────────────────
        ax.set_title(
            f"{sys_label}  —  Adaptive Decision Boundary",
            fontsize=13, fontweight="bold", pad=8,
        )
        ax.set_ylabel("Health Index [0, 1]", labelpad=8)
        ax.set_ylim(0, 1.08)
        ax.set_xlim(risk_score.index.min(), risk_score.index.max())
        ax.grid(True, which="major", color="#bdc3c7", linestyle="--", alpha=0.6)
        ax.minorticks_on()
        ax.grid(True, which="minor", color="#ecf0f1", linestyle=":", alpha=0.4)

        # Legend outside right
        ax.legend(
            loc="upper left",
            bbox_to_anchor=(1.0, 1.0),
            frameon=True, fancybox=True, shadow=False,
            fontsize=8,
        )

    axes.flatten()[-1].set_xlabel("Timestamp", labelpad=8)

    fig.suptitle(
        "Multivariate Anomaly Detection — Adaptive Decision Boundary",
        fontsize=15, fontweight="bold", y=1.002,
    )
    plt.tight_layout()

    # ── Save ──────────────────────────────────────────────────────────────
    out_path = os.path.join(cfg.output_dir, "ae_decision_boundary.png")
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.show()
    plt.close(fig)
    print(f"\n✅ Cell 9a complete.  Saved → {out_path}")
    print(f"   Subsystems plotted: {', '.join(subsystems)}")
    # =============================================================================
# Cell 11b: Topological Fault Characterization – Radar Fingerprint
# =============================================================================
# Section 8 methodology applied PER SUBSYSTEM.
# For each subsystem, finds the alarm event with the highest peak risk score,
# extracts mean per-sensor sigma-scaled directional errors over that window,
# and renders a radar (spider) chart — the "fault fingerprint".
#
# Source data (all from ae_results, populated by Cell 9):
#   result["per_sensor_sigma"]  – sigma-scaled directional errors, full index
#   result["alarm"]             – boolean alarm series, full index
#   result["system_score"]      – risk score, full index
# =============================================================================

print("=" * 70)
print("CELL 9b: RADAR FAULT FINGERPRINT (Per-Subsystem)")
print("=" * 70)

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from math import pi

# ── Plot style ────────────────────────────────────────────────────────────────
plt.style.use("seaborn-v0_8-paper")
plt.rcParams.update({
    "font.family":    "serif",
    "font.size":      11,
    "axes.titlesize": 13,
    "legend.fontsize": 9,
})


def _extract_alarm_events(alarm: pd.Series, risk: pd.Series) -> pd.DataFrame:
    """
    Parse contiguous alarm windows from a boolean Series.
    Returns a DataFrame with columns: start, end, duration, peak_risk.
    If no alarms exist, returns an empty DataFrame.
    """
    diff   = alarm.astype(int).diff()
    starts = alarm.index[diff == 1].tolist()
    ends   = alarm.index[diff == -1].tolist()

    # Handle alarm still active at end of series
    if alarm.iloc[-1]:
        ends.append(alarm.index[-1])

    # Handle alarm active at very start (no rising edge detected)
    if alarm.iloc[0] and (len(starts) == 0 or (len(ends) > 0 and ends[0] < starts[0])):
        starts.insert(0, alarm.index[0])

    if not starts or not ends:
        return pd.DataFrame(columns=["start", "end", "duration", "peak_risk"])

    events = []
    for s, e in zip(starts, ends):
        window_risk = risk.loc[s:e].dropna()
        peak        = float(window_risk.max()) if len(window_risk) else 0.0
        events.append({
            "start":     s,
            "end":       e,
            "duration":  (e - s) if not isinstance(s, (int, np.integer)) else int(e - s),
            "peak_risk": peak,
        })

    return pd.DataFrame(events)


def _plot_radar(
    ax,
    values:   np.ndarray,
    angles:   list,
    labels:   list,
    title:    str,
    peak_risk: float,
    event_dur,
    ref_sigma: float = 0.5,
) -> None:
    """
    Draw a single radar fingerprint on a polar Axes `ax`.
    """
    N = len(labels)
    v = values.tolist() + [values[0]]   # close the polygon
    a = angles + [angles[0]]

    # ── Background rings ──────────────────────────────────────────────────
    y_max  = max(float(np.max(values)) + 0.5, 1.5)
    ticks  = [t for t in [0.5, 1.0, 2.0, 3.0, 4.0] if t <= y_max]
    tick_labels = [f"{t}σ" for t in ticks]
    ax.set_ylim(0, y_max)
    ax.set_rlabel_position(0)
    ax.set_yticks(ticks)
    ax.set_yticklabels(tick_labels, color="grey", fontsize=8)

    # ── Axis labels (sensor names) ─────────────────────────────────────────
    ax.set_xticks(angles)
    ax.set_xticklabels(labels, color="#2c3e50", fontsize=9)

    # ── Normal baseline reference circle ──────────────────────────────────
    ref_v = [ref_sigma] * (N + 1)
    ax.plot(a, ref_v, color="#27ae60", linewidth=1.2, linestyle="--",
            label=f"Normal baseline ({ref_sigma}σ)", zorder=3)
    ax.fill(a, ref_v, color="#27ae60", alpha=0.07)

    # ── Fault fingerprint polygon ──────────────────────────────────────────
    ax.plot(a, v, linewidth=2, linestyle="solid", color="#e74c3c",
            label="Fault fingerprint", zorder=5)
    ax.fill(a, v, color="#e74c3c", alpha=0.22)

    # ── Sensor dots ───────────────────────────────────────────────────────
    ax.scatter(angles, values, color="#e74c3c", s=40, zorder=6)

    # ── Title ─────────────────────────────────────────────────────────────
    ax.set_title(
        f"{title}\nPeak risk={peak_risk:.3f}  |  Event duration={event_dur}",
        size=11, color="#2c3e50", y=1.14,
    )

    ax.legend(loc="upper right", bbox_to_anchor=(1.35, 1.12), fontsize=8)


# ── Guard ─────────────────────────────────────────────────────────────────────
if not ae_results:
    print("⚠  ae_results is empty – run Cell 9 first.")
else:
    subsystems = list(ae_results.keys())
    n_sys      = len(subsystems)

    # ── Layout: up to 3 radars per row ────────────────────────────────────
    ncols = min(3, n_sys)
    nrows = int(np.ceil(n_sys / ncols))

    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(7 * ncols, 6.5 * nrows),
        subplot_kw=dict(polar=True),
        squeeze=False,
    )

    plotted = 0

    for idx, sys_label in enumerate(subsystems):
        row = idx // ncols
        col = idx % ncols
        ax  = axes[row][col]

        result    = ae_results[sys_label]
        sensors   = ae_models[sys_label]["sensors"]
        sigma_df  = result["per_sensor_sigma"]   # full index, NaN during DT
        alarm_s   = result["alarm"]
        risk_s    = result["system_score"]

        # ── Find alarm events ──────────────────────────────────────────────
        events_df = _extract_alarm_events(alarm_s, risk_s)

        if events_df.empty:
            ax.set_title(f"{sys_label}\n(no alarms detected)", size=11)
            ax.set_axis_off()
            print(f"   {sys_label}: no alarms — radar skipped.")
            plotted += 1
            continue

        # ── Select event with highest peak risk ───────────────────────────
        top_event = events_df.loc[events_df["peak_risk"].idxmax()]
        t_start   = top_event["start"]
        t_end     = top_event["end"]

        # Mean sigma-scaled directional error per sensor in this window
        window_sigma = sigma_df.loc[t_start:t_end, sensors].dropna()
        if window_sigma.empty:
            # Fallback: use running mean across all alarm periods
            alarm_idx    = alarm_s[alarm_s].index
            window_sigma = sigma_df.loc[alarm_idx, sensors].dropna()

        if window_sigma.empty:
            ax.set_title(f"{sys_label}\n(insufficient data for radar)", size=11)
            ax.set_axis_off()
            print(f"   {sys_label}: no sigma data in alarm window — skipped.")
            plotted += 1
            continue

        mean_errors = window_sigma.mean().values   # shape (n_sensors,)
        labels      = sensors

        # ── Compute angles ────────────────────────────────────────────────
        N      = len(labels)
        angles = [n / float(N) * 2 * pi for n in range(N)]

        # ── Plot ──────────────────────────────────────────────────────────
        _plot_radar(
            ax=ax,
            values=mean_errors,
            angles=angles,
            labels=labels,
            title=sys_label,
            peak_risk=float(top_event["peak_risk"]),
            event_dur=top_event["duration"],
            ref_sigma=0.5,
        )

        # ── Print top contributing sensor ─────────────────────────────────
        top_idx    = int(np.argmax(mean_errors))
        top_sensor = labels[top_idx]
        print(
            f"   {sys_label}: top sensor = {top_sensor} "
            f"({mean_errors[top_idx]:.3f}σ)  |  "
            f"event [{t_start} → {t_end}]"
        )
        plotted += 1

    # ── Hide any unused axes (when n_sys < nrows*ncols) ───────────────────
    for idx in range(n_sys, nrows * ncols):
        axes[idx // ncols][idx % ncols].set_visible(False)

    fig.suptitle(
        "Topological Fault Characterization — Radar Fingerprints",
        fontsize=15, fontweight="bold", y=1.01,
    )
    plt.tight_layout()

    # ── Save ──────────────────────────────────────────────────────────────
    out_path = os.path.join(cfg.output_dir, "ae_radar_fingerprints.png")
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.show()
    plt.close(fig)

    print(f"\n✅ Cell 9b complete.  Saved → {out_path}")
    print(f"   Subsystems plotted: {plotted}/{n_sys}")
    print(
        "\n   Interpretation guide:\n"
        "   • Each axis = one sensor's mean sigma deviation during peak alarm.\n"
        "   • Polygon area = fault 'fingerprint' (shape identifies fault type).\n"
        "   • Green dashed ring = 0.5σ normal baseline reference.\n"
        "   • Sensors extending beyond 1σ ring are primary contributors.\n"
        "   • Symmetric spread → systemic shift; asymmetric spike → point fault."
    )

# %%
# =============================================================================
# Cell 12: Build Per-System Detailed Output File (v3 – Sensor Rankings + DT Exclusion)
# =============================================================================
# KEY CHANGES:
#   1. Downtime rows are NaN (not forced to zero) — they were never computed.
#   2. New sensor ranking columns per subsystem.
#   3. Score Level comes directly from scoring (not recomputed).
# =============================================================================

print("=" * 70)
print("BUILDING DETAILED PER-SYSTEM OUTPUT (v3 – Rankings + NaN Downtime)")
print("=" * 70)


def build_detailed_system_output_v3(
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
    top_k: int = 5,
) -> pd.DataFrame:
    """
    Build wide DataFrame. Downtime rows have NaN for all computed fields
    (score, threshold, confidence, alarm=False, rankings=empty).
    """
    out = pd.DataFrame(index=df.index)
    out.index.name = "timestamp_utc"
    out["downtime_flag"] = downtime.astype(int)

    for sys_label in sorted(catalog.keys()):
        sensors = catalog[sys_label]

        if sys_label in ae_results:
            r = ae_results[sys_label]

            # System Score (NaN during downtime — already correct)
            out[f"{sys_label}__System_Score"] = r["system_score"]

            # Adaptive Threshold (NaN during downtime)
            out[f"{sys_label}__Adaptive_Threshold"] = r["threshold"]

            # Score Level (NaN during downtime — not "Low")
            out[f"{sys_label}__Score_Level"] = r["score_level"]

            # Confidence (NaN during downtime)
            out[f"{sys_label}__System_Confidence"] = r["confidence"]

            # Alarm (False during downtime — never computed for DT)
            out[f"{sys_label}__System_Alarm"] = r["alarm"].astype(int)

            # Baseline Sigma (scalar, repeated)
            out[f"{sys_label}__Baseline_Sigma"] = r["baseline_sigma"]

            # ── NEW: Sensor Rankings ──
            rankings_df = r["sensor_rankings"]
            for col in rankings_df.columns:
                out[f"{sys_label}___{col}"] = rankings_df[col]

        else:
            out[f"{sys_label}__System_Score"] = np.nan
            out[f"{sys_label}__Adaptive_Threshold"] = np.nan
            out[f"{sys_label}__Score_Level"] = np.nan
            out[f"{sys_label}__System_Confidence"] = np.nan
            out[f"{sys_label}__System_Alarm"] = 0
            out[f"{sys_label}__Baseline_Sigma"] = np.nan

        # Per-sensor detail columns
        for s in sensors:
            prefix = f"{sys_label}__{s}"

            if s in base_bad.columns:
                out[f"{prefix}__Invalid_Flag"] = base_bad[s].astype(int)
            else:
                out[f"{prefix}__Invalid_Flag"] = df[s].isna().astype(int) if s in df.columns else 1

            if s in sqs.columns:
                out[f"{prefix}__SQS"] = sqs[s]
            else:
                out[f"{prefix}__SQS"] = np.nan

            if s in A.columns:
                out[f"{prefix}__Engine_A"] = A[s]
            else:
                out[f"{prefix}__Engine_A"] = np.nan

            if s in B.columns:
                out[f"{prefix}__Engine_B"] = B[s]
            else:
                out[f"{prefix}__Engine_B"] = np.nan

            if s in sensor_trust.columns:
                out[f"{prefix}__Trust"] = sensor_trust[s]
            else:
                out[f"{prefix}__Trust"] = "Unusable"

            # Per-sensor contribution score (from sensor_contributions)
            if sys_label in ae_results:
                contrib_df = ae_results[sys_label]["sensor_contributions"]
                if s in contrib_df.columns:
                    out[f"{prefix}__AE_Contribution"] = contrib_df[s]
                else:
                    out[f"{prefix}__AE_Contribution"] = np.nan
            else:
                out[f"{prefix}__AE_Contribution"] = np.nan

            # Per-sensor sigma-scaled directional error (for radar fingerprints)
            if sys_label in ae_results:
                sigma_df = ae_results[sys_label]["per_sensor_sigma"]
                if s in sigma_df.columns:
                    out[f"{prefix}__Sigma_Score"] = sigma_df[s]
                else:
                    out[f"{prefix}__Sigma_Score"] = np.nan
            else:
                out[f"{prefix}__Sigma_Score"] = np.nan

    return out


detailed_output = build_detailed_system_output_v3(
    df, downtime, running, catalog, ae_results, A, B, sqs, base_bad, sensor_trust,
    top_k=cfg.ae_top_k_sensors,
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
# Build 8 separate files (was 7, now + Rankings)
# =============================================================================
print("\n--- Building 8 separate output files ---")


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
print(f"   Engine A shape: {out_a.shape}  💾 {a_csv}")


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
print(f"   Engine B shape: {out_b.shape}  💾 {b_csv}")


# --- 3) Subsystem Scores file ---
out_sub = pd.DataFrame(index=df.index)
out_sub.index.name = "timestamp_utc"
out_sub["downtime_flag"] = downtime.astype(int)
for sys_label in sorted(catalog.keys()):
    if sys_label in ae_results:
        r = ae_results[sys_label]
        out_sub[f"{sys_label}__System_Score"] = r["system_score"]
        out_sub[f"{sys_label}__Adaptive_Threshold"] = r["threshold"]
        out_sub[f"{sys_label}__Score_Level"] = r["score_level"]
        out_sub[f"{sys_label}__Baseline_Sigma"] = r["baseline_sigma"]
    else:
        out_sub[f"{sys_label}__System_Score"] = np.nan
        out_sub[f"{sys_label}__Adaptive_Threshold"] = np.nan
        out_sub[f"{sys_label}__Score_Level"] = np.nan
        out_sub[f"{sys_label}__Baseline_Sigma"] = np.nan

sub_csv = os.path.join(cfg.output_dir, "detailed_subsystem_scores.csv")
sub_parquet = os.path.join(cfg.output_dir, "detailed_subsystem_scores.parquet")
out_sub.to_csv(sub_csv)
out_sub.to_parquet(sub_parquet)
print(f"   Subsystem Scores shape: {out_sub.shape}  💾 {sub_csv}")


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
print(f"   SQS shape: {out_sqs.shape}  💾 {sqs_csv}")


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
print(f"   Sensor Trust shape: {out_trust.shape}  💾 {trust_csv}")


# --- 6) Subsystem Confidence file ---
out_conf = pd.DataFrame(index=df.index)
out_conf.index.name = "timestamp_utc"
out_conf["downtime_flag"] = downtime.astype(int)
for sys_label in sorted(catalog.keys()):
    if sys_label in ae_results:
        out_conf[f"{sys_label}__Confidence"] = ae_results[sys_label]["confidence"]
    else:
        out_conf[f"{sys_label}__Confidence"] = np.nan

conf_csv = os.path.join(cfg.output_dir, "detailed_subsystem_confidence.csv")
conf_parquet = os.path.join(cfg.output_dir, "detailed_subsystem_confidence.parquet")
out_conf.to_csv(conf_csv)
out_conf.to_parquet(conf_parquet)
print(f"   Subsystem Confidence shape: {out_conf.shape}  💾 {conf_csv}")


# --- 7) Subsystem Alarm file ---
out_alarm = pd.DataFrame(index=df.index)
out_alarm.index.name = "timestamp_utc"
out_alarm["downtime_flag"] = downtime.astype(int)
for sys_label in sorted(catalog.keys()):
    if sys_label in ae_results:
        out_alarm[f"{sys_label}__Alarm"] = ae_results[sys_label]["alarm"].astype(int)
        out_alarm[f"{sys_label}__Score_Level_At_Alarm"] = ae_results[sys_label]["score_level"]
    else:
        out_alarm[f"{sys_label}__Alarm"] = 0
        out_alarm[f"{sys_label}__Score_Level_At_Alarm"] = np.nan

alarm_csv = os.path.join(cfg.output_dir, "detailed_subsystem_alarms.csv")
alarm_parquet = os.path.join(cfg.output_dir, "detailed_subsystem_alarms.parquet")
out_alarm.to_csv(alarm_csv)
out_alarm.to_parquet(alarm_parquet)
print(f"   Subsystem Alarm shape: {out_alarm.shape}  💾 {alarm_csv}")


# --- 8) NEW: Sensor Rankings file ---
out_rank = pd.DataFrame(index=df.index)
out_rank.index.name = "timestamp_utc"
out_rank["downtime_flag"] = downtime.astype(int)
for sys_label in sorted(catalog.keys()):
    if sys_label in ae_results:
        rankings_df = ae_results[sys_label]["sensor_rankings"]
        for col in rankings_df.columns:
            out_rank[f"{sys_label}___{col}"] = rankings_df[col]

        # Also include per-sensor contribution scores
        contrib_df = ae_results[sys_label]["sensor_contributions"]
        for s in contrib_df.columns:
            out_rank[f"{sys_label}__{s}__Contribution"] = contrib_df[s]

rank_csv = os.path.join(cfg.output_dir, "detailed_sensor_rankings.csv")
rank_parquet = os.path.join(cfg.output_dir, "detailed_sensor_rankings.parquet")
out_rank.to_csv(rank_csv)
out_rank.to_parquet(rank_parquet)
print(f"   Sensor Rankings shape: {out_rank.shape}  💾 {rank_csv}")


# ─────────────────────────────────────────────────────────────────────────────
# DOWNTIME EXCLUSION VERIFICATION (now checking NaN, not zeros)
# ─────────────────────────────────────────────────────────────────────────────
print("\n--- Downtime exclusion verification ---")
n_downtime = int((~running).sum())
n_running = int(running.sum())
print(f"   Total timestamps: {len(df)}")
print(f"   Running: {n_running} | Downtime: {n_downtime}")

for sys_label in sorted(ae_results.keys()):
    r = ae_results[sys_label]

    # Score should be NaN during downtime
    score_dt = r["system_score"].loc[~running]
    score_nan_count = int(score_dt.isna().sum())
    score_non_nan = int(score_dt.notna().sum())

    # Alarm should be False during downtime
    alarm_dt = r["alarm"].loc[~running]
    alarm_true_count = int(alarm_dt.sum())

    # Confidence should be NaN during downtime
    conf_dt = r["confidence"].loc[~running]
    conf_nan_count = int(conf_dt.isna().sum())
    conf_non_nan = int(conf_dt.notna().sum())

    # Rankings should be empty during downtime
    rank_dt = r["sensor_rankings"].loc[~running]
    rank_1_non_empty = int((rank_dt.get("Rank_1_Sensor", pd.Series("")) != "").sum())

    all_ok = (score_non_nan == 0 and alarm_true_count == 0
              and conf_non_nan == 0 and rank_1_non_empty == 0)
    status = "✅" if all_ok else "⚠️"

    print(f"   {status} {sys_label}: "
          f"DT_score_non_NaN={score_non_nan}, "
          f"DT_alarm_true={alarm_true_count}, "
          f"DT_conf_non_NaN={conf_non_nan}, "
          f"DT_rankings_non_empty={rank_1_non_empty}")


# ─────────────────────────────────────────────────────────────────────────────
# Preview columns structure
# ─────────────────────────────────────────────────────────────────────────────
sys_labels_in_output = sorted(catalog.keys())
print(f"\n   Systems in output: {sys_labels_in_output}")
for sys_label in sys_labels_in_output:
    sys_cols = [c for c in detailed_output.columns
                if c.startswith(f"{sys_label}__")]
    print(f"   {sys_label}: {len(sys_cols)} columns")
    # Show ranking columns specifically
    rank_cols = [c for c in sys_cols if "Rank_" in c]
    contrib_cols = [c for c in sys_cols if "Contribution" in c]
    other_cols = [c for c in sys_cols if c not in rank_cols and c not in contrib_cols]

    if other_cols:
        print(f"       System/Sensor cols: {len(other_cols)}")
        for c in other_cols[:8]:
            print(f"           {c}")
        if len(other_cols) > 8:
            print(f"           ... ({len(other_cols) - 8} more)")

    if rank_cols:
        print(f"       Ranking cols: {len(rank_cols)}")
        for c in rank_cols[:6]:
            print(f"           {c}")

    if contrib_cols:
        print(f"       Contribution cols: {len(contrib_cols)}")
        for c in contrib_cols[:4]:
            print(f"           {c}")
        if len(contrib_cols) > 4:
            print(f"           ... ({len(contrib_cols) - 4} more)")


# ─────────────────────────────────────────────────────────────────────────────
# Summary table (with rankings info)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("SUBSYSTEM SUMMARY (v3 – with Rankings & Downtime Exclusion)")
print("=" * 70)

summary_rows = []
for sys_label in sorted(catalog.keys()):
    n_sensors = len(catalog[sys_label])

    if sys_label in ae_results:
        r = ae_results[sys_label]
        n_ae_sensors = len(ae_models[sys_label]["sensors"])

        # Running-only stats (values are already NaN for downtime)
        score_run = r["system_score"].dropna()
        score_mean = float(score_run.mean()) if len(score_run) > 0 else 0.0

        thresh_run = r["threshold"].dropna()
        thresh_mean = float(thresh_run.mean()) if len(thresh_run) > 0 else 0.0

        conf_run = r["confidence"].dropna()
        conf_mean = float(conf_run.mean()) if len(conf_run) > 0 else 0.0

        n_alarms = int(r["alarm"].sum())

        sl = r["score_level"].dropna()
        n_high = int((sl == "High").sum())
        n_low = int((sl == "Low").sum())
        pct_high = 100.0 * n_high / max(n_high + n_low, 1)

        bsig = r["baseline_sigma"]

        # Most frequent top-1 contributor across alarms
        alarm_mask = r["alarm"]
        top_contributor = "N/A"
        if n_alarms > 0:
            alarm_rankings = r["sensor_rankings"].loc[alarm_mask]
            if "Rank_1_Sensor" in alarm_rankings.columns:
                top_counts = alarm_rankings["Rank_1_Sensor"].value_counts()
                if len(top_counts) > 0:
                    top_contributor = f"{top_counts.index[0]} ({top_counts.iloc[0]}x)"

        summary_rows.append({
            "Subsystem": sys_label,
            "Sensors": f"{n_ae_sensors}/{n_sensors}",
            "Score_Mean": f"{score_mean:.4f}",
            "Thresh_Mean": f"{thresh_mean:.4f}",
            "High%": f"{pct_high:.1f}%",
            "High_Count": n_high,
            "Confidence": f"{conf_mean:.4f}",
            "Baseline_σ": f"{bsig:.6f}",
            "Alarms": n_alarms,
            "Top_Alarm_Contributor": top_contributor,
        })
    else:
        summary_rows.append({
            "Subsystem": sys_label,
            "Sensors": f"0/{n_sensors}",
            "Score_Mean": "N/A",
            "Thresh_Mean": "N/A",
            "High%": "N/A",
            "High_Count": "N/A",
            "Confidence": "N/A",
            "Baseline_σ": "N/A",
            "Alarms": "N/A",
            "Top_Alarm_Contributor": "N/A",
        })

summary_df = pd.DataFrame(summary_rows)
print(summary_df.to_string(index=False))

summary_csv = os.path.join(cfg.output_dir, "subsystem_summary.csv")
summary_df.to_csv(summary_csv, index=False)
print(f"\n   Saved summary: {summary_csv}")


# ─────────────────────────────────────────────────────────────────────────────
# ALERT EPISODE TABLES (for beta dashboard)
# ─────────────────────────────────────────────────────────────────────────────

def summarize_beta_episode_sensors(
    contribution_df: pd.DataFrame,
    start_idx: int,
    end_idx: int,
) -> pd.DataFrame:
    out_cols = ["sensor", "sensor_rank", "sensor_peak_score", "sensor_mean_score"]
    if contribution_df is None or contribution_df.empty:
        return pd.DataFrame(columns=out_cols)

    seg = contribution_df.iloc[start_idx:end_idx + 1]
    if seg.empty:
        return pd.DataFrame(columns=out_cols)

    peak_by_sensor = seg.max(axis=0, skipna=True)
    if peak_by_sensor.notna().sum() == 0:
        return pd.DataFrame(columns=out_cols)

    mean_by_sensor = seg.mean(axis=0, skipna=True)
    summary = pd.DataFrame({
        "sensor": peak_by_sensor.index.astype(str),
        "sensor_peak_score": peak_by_sensor.values.astype(float),
        "sensor_mean_score": mean_by_sensor.reindex(peak_by_sensor.index).values.astype(float),
    })
    summary = summary[summary["sensor_peak_score"].notna()]
    if summary.empty:
        return pd.DataFrame(columns=out_cols)

    positive = summary[summary["sensor_peak_score"] > 0]
    if not positive.empty:
        summary = positive

    summary = summary.sort_values(
        ["sensor_peak_score", "sensor_mean_score", "sensor"],
        ascending=[False, False, True],
        kind="stable",
    ).reset_index(drop=True)
    summary["sensor_rank"] = np.arange(1, len(summary) + 1, dtype=int)
    return summary[out_cols]


def build_beta_alert_tables(
    ts_index: pd.Index,
    ae_results: Dict[str, Dict],
    min_duration: int,
    merge_gap: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    level_order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
    episode_cols = [
        "start_ts", "end_ts", "duration_minutes", "severity", "class",
        "sensor_id", "sensor_max_score", "sensor_mean_score",
        "affected_sensor_count", "affected_sensors", "max_score", "mean_score",
        "threshold",
    ]
    sensor_alert_cols = [
        "start_ts", "end_ts", "duration_minutes", "severity", "class",
        "sensor", "sensor_rank", "sensor_peak_score", "sensor_mean_score",
        "alert_max_score", "alert_mean_score", "threshold",
    ]

    alerts_rows = []
    sensor_rows = []

    for sys_label in sorted(ae_results.keys()):
        result = ae_results[sys_label]
        alarm_series = pd.Series(result["alarm"], index=ts_index).fillna(False).astype(bool)

        intervals: List[Tuple[int, int]] = []
        start = None
        for i, is_on in enumerate(alarm_series.values):
            if is_on and start is None:
                start = i
            if (not is_on) and start is not None:
                intervals.append((start, i - 1))
                start = None
        if start is not None:
            intervals.append((start, len(alarm_series) - 1))

        merged: List[List[int]] = []
        for a, b in intervals:
            if not merged:
                merged.append([a, b])
            elif a - merged[-1][1] - 1 <= merge_gap:
                merged[-1][1] = b
            else:
                merged.append([a, b])

        contribution_df = result.get("sensor_contributions", pd.DataFrame(index=ts_index))
        score_series = pd.Series(result["system_score"], index=ts_index)
        score_level_series = pd.Series(result.get("score_level"), index=ts_index)

        for a, b in merged:
            dur = b - a + 1
            if dur < min_duration:
                continue

            score_seg = pd.to_numeric(score_series.iloc[a:b + 1], errors="coerce")
            if score_seg.notna().sum() == 0:
                continue

            alert_max = float(score_seg.max())
            alert_mean = float(score_seg.mean())
            level_seg = score_level_series.iloc[a:b + 1]
            level_alarm_seg = level_seg.loc[alarm_series.iloc[a:b + 1].values]
            source_levels = level_alarm_seg if not level_alarm_seg.empty else level_seg
            normalized_levels = [
                str(level).strip().upper()
                for level in source_levels.tolist()
                if pd.notna(level) and str(level).strip().upper() in level_order
            ]
            severity = max(normalized_levels, key=lambda lvl: level_order[lvl]) if normalized_levels else "UNKNOWN"
            sensor_summary = summarize_beta_episode_sensors(contribution_df, a, b)

            if sensor_summary.empty:
                sensor_id = "UNKNOWN"
                sensor_max_score = np.nan
                sensor_mean_score = np.nan
                affected_sensor_count = 0
                affected_sensors = "UNKNOWN"
            else:
                top_sensor = sensor_summary.iloc[0]
                sensor_id = str(top_sensor["sensor"])
                sensor_max_score = float(top_sensor["sensor_peak_score"])
                sensor_mean_score = float(top_sensor["sensor_mean_score"])
                affected_sensor_count = int(len(sensor_summary))
                affected_sensors = "|".join(sensor_summary["sensor"].astype(str).tolist())

            alerts_rows.append({
                "start_ts": ts_index[a],
                "end_ts": ts_index[b],
                "duration_minutes": dur,
                "severity": severity,
                "class": sys_label,
                "sensor_id": sensor_id,
                "sensor_max_score": sensor_max_score,
                "sensor_mean_score": sensor_mean_score,
                "affected_sensor_count": affected_sensor_count,
                "affected_sensors": affected_sensors,
                "max_score": alert_max,
                "mean_score": alert_mean,
                "threshold": "ADAPTIVE",
            })

            if not sensor_summary.empty:
                for row in sensor_summary.itertuples(index=False):
                    sensor_rows.append({
                        "start_ts": ts_index[a],
                        "end_ts": ts_index[b],
                        "duration_minutes": dur,
                        "severity": severity,
                        "class": sys_label,
                        "sensor": str(row.sensor),
                        "sensor_rank": int(row.sensor_rank),
                        "sensor_peak_score": float(row.sensor_peak_score),
                        "sensor_mean_score": float(row.sensor_mean_score),
                        "alert_max_score": alert_max,
                        "alert_mean_score": alert_mean,
                        "threshold": "ADAPTIVE",
                    })

    return (
        pd.DataFrame(alerts_rows, columns=episode_cols),
        pd.DataFrame(sensor_rows, columns=sensor_alert_cols),
    )


alerts_df, alerts_sensor_df = build_beta_alert_tables(
    ts_index=df.index,
    ae_results=ae_results,
    min_duration=cfg.alert_min_duration_min,
    merge_gap=cfg.alert_merge_gap_min,
)

alerts_csv = os.path.join(cfg.output_dir, "alerts.csv")
alerts_parquet = os.path.join(cfg.output_dir, "alerts.parquet")
alerts_df.to_csv(alerts_csv, index=False)
alerts_df.to_parquet(alerts_parquet, index=False)
print(f"   Alerts shape: {alerts_df.shape}  {alerts_csv}")

alerts_sensor_csv = os.path.join(cfg.output_dir, "alerts_sensor_level.csv")
alerts_sensor_parquet = os.path.join(cfg.output_dir, "alerts_sensor_level.parquet")
alerts_sensor_df.to_csv(alerts_sensor_csv, index=False)
alerts_sensor_df.to_parquet(alerts_sensor_parquet, index=False)
print(f"   Sensor alert rows: {alerts_sensor_df.shape}  {alerts_sensor_csv}")

print("\n Detailed system output complete (v3).")
print("   Downtime rows: NaN (excluded from computation, not forced to zero)")
print("   Sensor rankings: per-timestamp contribution ranking generated")
print("   Output files: 8 separate files + 1 combined + 1 summary")
print("   Beta chart inputs: df_chart_data.csv + sensor_values_{system}.csv")

# %% [markdown]
# # STANDALONE AND PROCESS LEVEL ALERTS

# %%

# =============================================================================
# Cell 16: Standalone Sensor Scoring
# =============================================================================
# Purpose: Score ALL sensors in df individually using:
#            Evidence_j,t = max(DriftScore_j,t, PeriodicityScore_j,t) × SQS_j,t
#          This is a sensor-level view complementary to AE subsystem scoring.
#          AE captures cross-sensor reconstruction anomalies; standalone captures
#          single-sensor drift/periodicity evidence. Both views feed process fusion.
#          Sensors already in AE subsystems are scored here too — no exclusions.
#
# Downtime: ALL downtime rows produce NaN evidence / False alarm.
#           Evidence and all derived fields are ONLY computed for running timestamps.
#
# Outputs (saved to standalone_outputs/ subfolder):
#   standalone_evidence.csv / .parquet   — raw Evidence_j,t per sensor
#   standalone_scores.csv   / .parquet   — evidence + threshold + score_level + context
#   standalone_alarms.csv   / .parquet   — alarm flag + score level at alarm
#   standalone_summary.csv               — per-sensor scalar summary
# =============================================================================

print("=" * 70)
print("CELL 16: STANDALONE SENSOR SCORING")
print("=" * 70)

print(f"   Config: standalone_on_delay       = {cfg.standalone_on_delay} min")
print(f"   Config: standalone_off_delay      = {cfg.standalone_off_delay} min")
print(f"   Config: standalone_sigma_factor   = {cfg.standalone_sigma_factor}")
print(f"   Config: standalone_threshold_win  = {cfg.standalone_threshold_window}")
print(f"   Config: standalone_gate_factor    = {cfg.standalone_gate_factor}")
print(f"   Config: standalone_slew_rate      = {cfg.standalone_slew_rate}")
print(f"   Config: standalone_baseline_sqs   = {cfg.standalone_baseline_sqs_min}")
print(f"   Config: standalone_baseline_drift = {cfg.standalone_baseline_drift_max}")

# ── Identify standalone sensors ──────────────────────────────────────────────
# ALL sensors in df get standalone scoring — this is sensor-level independent
# evidence regardless of whether the sensor is also part of an AE subsystem.
# Subsystem AE scoring captures multi-sensor cross-reconstruction anomalies;
# standalone scoring captures single-sensor drift/periodicity evidence.
# Both views are complementary and used together in process risk fusion.

standalone_sensors: List[str] = list(df.columns)

# For information only — track which sensors are also in AE subsystems
ae_subsystem_sensors: set = set()
for sys_label, sensors_list in catalog.items():
    if sys_label != "ISOLATED":
        ae_subsystem_sensors.update(sensors_list)
isolated_sensors_set: set = set(catalog.get("ISOLATED", []))
ae_only_sensors = [s for s in standalone_sensors if s in ae_subsystem_sensors]
isolated_only   = [s for s in standalone_sensors if s in isolated_sensors_set]
uncatalogued    = [s for s in standalone_sensors
                   if s not in ae_subsystem_sensors and s not in isolated_sensors_set]

print(f"\n   Total df sensors            : {len(df.columns)}")
print(f"   Also in AE subsystems       : {len(ae_only_sensors)}")
print(f"   Isolated (no subsystem)     : {len(isolated_only)}")
print(f"   Uncatalogued                : {len(uncatalogued)}")
print(f"   Total standalone scored     : {len(standalone_sensors)}  (= ALL sensors)")


# ── Core functions ────────────────────────────────────────────────────────────

def compute_standalone_evidence(
    df: pd.DataFrame,
    sensors: List[str],
    A: pd.DataFrame,
    B: pd.DataFrame,
    sqs: pd.DataFrame,
    running_mask: pd.Series,
    sqs_min: float,
) -> pd.DataFrame:
    """
    Evidence_j,t = max(DriftScore_j,t, PeriodicityScore_j,t) × SQS_j,t
    Only computed for RUNNING timestamps; downtime rows stay NaN.
    SQS acts as a quality gate: if SQS < sqs_min → evidence = 0.
    """
    evidence = pd.DataFrame(np.nan, index=df.index, columns=sensors, dtype=float)

    for s in sensors:
        drift = A[s].copy() if s in A.columns else pd.Series(0.0, index=df.index)
        period = B[s].copy() if s in B.columns else pd.Series(0.0, index=df.index)
        sq = sqs[s].copy() if s in sqs.columns else pd.Series(1.0, index=df.index)

        # Fill NaN with 0 for computation (conservative)
        drift_v = drift.fillna(0.0)
        period_v = period.fillna(0.0)
        sq_v = sq.fillna(0.0)

        # max(drift, periodicity)
        raw_evidence = np.maximum(drift_v.values, period_v.values)

        # Weight by SQS (gate: below threshold → 0)
        sq_gate = np.where(sq_v.values >= sqs_min, sq_v.values, 0.0)
        ev = raw_evidence * sq_gate

        # Only running timestamps
        ev_series = pd.Series(ev, index=df.index)
        ev_series.loc[~running_mask] = np.nan

        evidence[s] = ev_series

    return evidence


def compute_standalone_baseline_sigma(
    evidence_running: np.ndarray,
    sqs_running: np.ndarray,
    drift_running: np.ndarray,
    n_samples: int,
    sqs_min: float,
    drift_max: float,
) -> float:
    """
    Reference baseline sigma from healthy running samples.
    Healthy = SQS >= sqs_min AND max(drift, period) < drift_max.
    Uses the first n_samples healthy indices.
    """
    n = len(evidence_running)
    healthy = (
        np.nan_to_num(sqs_running, nan=0.0) >= sqs_min
    ) & (
        np.nan_to_num(drift_running, nan=1.0) < drift_max
    )

    healthy_idx = np.where(healthy)[0]

    if len(healthy_idx) < 20:
        # Fallback: use all samples
        healthy_idx = np.arange(min(n_samples, n))
        if len(healthy_idx) < 5:
            sigma = float(np.nanstd(evidence_running))
            return sigma if sigma > 1e-8 else 1e-6

    baseline_idx = healthy_idx[:min(n_samples, len(healthy_idx))]
    sigma = float(np.nanstd(evidence_running[baseline_idx]))
    return sigma if sigma > 1e-8 else 1e-6


def standalone_adaptive_threshold(
    evidence: np.ndarray,
    baseline_sigma: float,
    window: int,
    slew_rate: float,
    gate: float,
    sigma_factor: float,
) -> np.ndarray:
    """
    Adaptive threshold with:
      1. Reference baseline: threshold = mu_recent + sigma_factor * sigma_baseline
      2. Slew-rate limiting: max change per step = slew_rate
      3. Gating: if evidence > gate * threshold_{t-1}, freeze adaptation
    """
    n = len(evidence)
    threshold = np.zeros(n)

    seed_window = min(window, n)
    seed = float(np.nanmean(evidence[:seed_window])) + sigma_factor * baseline_sigma
    threshold[:seed_window] = seed

    for t in range(seed_window, n):
        # Target = rolling mean + sigma baseline
        target = (
            float(np.nanmean(evidence[max(0, t - window):t]))
            + sigma_factor * baseline_sigma
        )
        # Gate check: if large anomaly, freeze
        if evidence[t] > gate * threshold[t - 1]:
            target = threshold[t - 1]
        # Slew-rate limiting
        change = np.clip(target - threshold[t - 1], -slew_rate, slew_rate)
        threshold[t] = threshold[t - 1] + change

    return threshold


def standalone_alarm_logic(
    evidence: np.ndarray,
    threshold: np.ndarray,
    on_delay: int,
    off_delay: int,
) -> np.ndarray:
    """On/off delay alarm persistence — same as AE subsystem."""
    n = len(evidence)
    alarms = np.zeros(n, dtype=bool)
    count = 0
    active = False

    for i in range(n):
        is_high = evidence[i] >= threshold[i]
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


def score_standalone_sensors(
    df: pd.DataFrame,
    running_mask: pd.Series,
    sensors: List[str],
    evidence_df: pd.DataFrame,
    A: pd.DataFrame,
    B: pd.DataFrame,
    sqs: pd.DataFrame,
    cfg,
) -> Dict[str, Dict]:
    """
    For each standalone sensor compute:
      - baseline_sigma (from healthy running samples)
      - adaptive threshold
      - alarm
      - score_level ("High" / "Low", NaN during downtime)

    Returns dict keyed by sensor name.
    """
    results = {}
    running_idx = running_mask[running_mask].index
    full_index = df.index

    for s in sensors:
        ev_full = evidence_df[s]  # NaN during downtime already
        ev_running = ev_full.loc[running_idx].values

        if np.all(np.isnan(ev_running)) or len(ev_running) == 0:
            results[s] = {
                "evidence": ev_full,
                "threshold": pd.Series(np.nan, index=full_index),
                "alarm": pd.Series(False, index=full_index),
                "score_level": pd.Series(np.nan, index=full_index, dtype=object),
                "baseline_sigma": np.nan,
            }
            continue

        # Build helper arrays for baseline sigma estimation
        sq_running = (
            sqs[s].loc[running_idx].values
            if s in sqs.columns
            else np.ones(len(running_idx))
        )
        drift_running = (
            A[s].loc[running_idx].values
            if s in A.columns
            else np.zeros(len(running_idx))
        )

        baseline_sigma = compute_standalone_baseline_sigma(
            evidence_running=np.nan_to_num(ev_running, nan=0.0),
            sqs_running=sq_running,
            drift_running=np.nan_to_num(drift_running, nan=1.0),
            n_samples=cfg.standalone_baseline_n_samples,
            sqs_min=cfg.standalone_baseline_sqs_min,
            drift_max=cfg.standalone_baseline_drift_max,
        )

        threshold_running = standalone_adaptive_threshold(
            evidence=np.nan_to_num(ev_running, nan=0.0),
            baseline_sigma=baseline_sigma,
            window=cfg.standalone_threshold_window,
            slew_rate=cfg.standalone_slew_rate,
            gate=cfg.standalone_gate_factor,
            sigma_factor=cfg.standalone_sigma_factor,
        )

        alarm_running = standalone_alarm_logic(
            evidence=np.nan_to_num(ev_running, nan=0.0),
            threshold=threshold_running,
            on_delay=cfg.standalone_on_delay,
            off_delay=cfg.standalone_off_delay,
        )

        score_level_running = np.where(
            np.nan_to_num(ev_running, nan=0.0) >= threshold_running,
            "High", "Low"
        )

        # Map back to full index
        threshold_full = pd.Series(np.nan, index=full_index, dtype=float)
        threshold_full.loc[running_idx] = threshold_running

        alarm_full = pd.Series(False, index=full_index, dtype=bool)
        alarm_full.loc[running_idx] = alarm_running

        score_level_full = pd.Series(np.nan, index=full_index, dtype=object)
        score_level_full.loc[running_idx] = score_level_running

        results[s] = {
            "evidence": ev_full,
            "threshold": threshold_full,
            "alarm": alarm_full,
            "score_level": score_level_full,
            "baseline_sigma": baseline_sigma,
        }

    return results


# ── Execute ───────────────────────────────────────────────────────────────────

# Step 1: Compute evidence for all standalone sensors
standalone_evidence_df = compute_standalone_evidence(
    df=df,
    sensors=standalone_sensors,
    A=A,
    B=B,
    sqs=sqs,
    running_mask=running,
    sqs_min=cfg.standalone_sqs_min,
)
print(f"\n   Evidence computed: {standalone_evidence_df.shape}")

# Step 2: Score each sensor
standalone_results = score_standalone_sensors(
    df=df,
    running_mask=running,
    sensors=standalone_sensors,
    evidence_df=standalone_evidence_df,
    A=A,
    B=B,
    sqs=sqs,
    cfg=cfg,
)
print(f"   Scored {len(standalone_results)} standalone sensors")

# ── Build output DataFrames ───────────────────────────────────────────────────

# Output directory
standalone_out_dir = os.path.join(cfg.output_dir, "standalone_outputs")
os.makedirs(standalone_out_dir, exist_ok=True)

# 1) Evidence file
out_ev = pd.DataFrame(index=df.index)
out_ev.index.name = "timestamp_utc"
out_ev["downtime_flag"] = downtime.astype(int)
for s in standalone_sensors:
    out_ev[s] = standalone_evidence_df[s]

ev_csv = os.path.join(standalone_out_dir, "standalone_evidence.csv")
ev_parquet = os.path.join(standalone_out_dir, "standalone_evidence.parquet")
out_ev.to_csv(ev_csv)
out_ev.to_parquet(ev_parquet)
print(f"\n   Evidence shape: {out_ev.shape}  💾 {ev_csv}")

# 2) Scores (evidence + threshold + score_level)
out_sc = pd.DataFrame(index=df.index)
out_sc.index.name = "timestamp_utc"
out_sc["downtime_flag"] = downtime.astype(int)
for s in standalone_sensors:
    r = standalone_results[s]
    out_sc[f"{s}__Evidence"]     = r["evidence"]
    out_sc[f"{s}__Threshold"]    = r["threshold"]
    out_sc[f"{s}__Score_Level"]  = r["score_level"]
    out_sc[f"{s}__Baseline_Sigma"] = r["baseline_sigma"]
    # Also attach SQS, EngineA, EngineB for context
    out_sc[f"{s}__SQS"]          = sqs[s] if s in sqs.columns else np.nan
    out_sc[f"{s}__Engine_A"]     = A[s] if s in A.columns else np.nan
    out_sc[f"{s}__Engine_B"]     = B[s] if s in B.columns else np.nan
    out_sc[f"{s}__Trust"]        = sensor_trust[s] if s in sensor_trust.columns else "Unusable"

sc_csv = os.path.join(standalone_out_dir, "standalone_scores.csv")
sc_parquet = os.path.join(standalone_out_dir, "standalone_scores.parquet")
out_sc.to_csv(sc_csv)
out_sc.to_parquet(sc_parquet)
print(f"   Scores shape: {out_sc.shape}  💾 {sc_csv}")

# 3) Alarms file
out_al = pd.DataFrame(index=df.index)
out_al.index.name = "timestamp_utc"
out_al["downtime_flag"] = downtime.astype(int)
for s in standalone_sensors:
    r = standalone_results[s]
    out_al[f"{s}__Alarm"]            = r["alarm"].astype(int)
    out_al[f"{s}__Score_Level_At_Alarm"] = r["score_level"]

al_csv = os.path.join(standalone_out_dir, "standalone_alarms.csv")
al_parquet = os.path.join(standalone_out_dir, "standalone_alarms.parquet")
out_al.to_csv(al_csv)
out_al.to_parquet(al_parquet)
print(f"   Alarms shape: {out_al.shape}  💾 {al_csv}")

# 4) Summary
summary_rows_sa = []
for s in standalone_sensors:
    r = standalone_results[s]
    ev_run = r["evidence"].dropna()
    ev_mean = float(ev_run.mean()) if len(ev_run) > 0 else np.nan
    ev_max  = float(ev_run.max())  if len(ev_run) > 0 else np.nan
    n_alarms = int(r["alarm"].sum())
    sl = r["score_level"].dropna()
    n_high = int((sl == "High").sum())
    pct_high = 100.0 * n_high / max(len(sl), 1)
    in_sqs = s in sqs.columns
    in_a   = s in A.columns
    in_b   = s in B.columns

    # Which AE subsystem does this sensor belong to (if any)?
    sensor_subsystem = "NONE"
    for sys_lbl, sys_sensors in catalog.items():
        if s in sys_sensors:
            sensor_subsystem = sys_lbl
            break

    summary_rows_sa.append({
        "Sensor":           s,
        "AE_Subsystem":     sensor_subsystem,
        "Has_SQS":          in_sqs,
        "Has_EngineA":      in_a,
        "Has_EngineB":      in_b,
        "Evidence_Mean":    round(ev_mean, 5) if not np.isnan(ev_mean) else "N/A",
        "Evidence_Max":     round(ev_max, 5)  if not np.isnan(ev_max)  else "N/A",
        "High_Pct":         f"{pct_high:.1f}%",
        "High_Count":       n_high,
        "Alarm_Count":      n_alarms,
        "Baseline_Sigma":   round(r["baseline_sigma"], 6)
                            if not np.isnan(r["baseline_sigma"]) else "N/A",
    })

standalone_summary_df = pd.DataFrame(summary_rows_sa)
sum_csv = os.path.join(standalone_out_dir, "standalone_summary.csv")
standalone_summary_df.to_csv(sum_csv, index=False)

print(f"\n{'=' * 70}")
print("STANDALONE SENSOR SUMMARY")
print(f"{'=' * 70}")
if not standalone_summary_df.empty:
    print(standalone_summary_df.to_string(index=False))
else:
    print("   (No standalone sensors found)")

print(f"\n   💾 Saved summary: {sum_csv}")

# ── Downtime exclusion verification ──────────────────────────────────────────
print("\n--- Standalone downtime exclusion verification ---")
n_downtime_sa = int((~running).sum())
issues = 0
for s in standalone_sensors:
    r = standalone_results[s]
    ev_dt_non_nan  = int(r["evidence"].loc[~running].notna().sum())
    alarm_dt_true  = int(r["alarm"].loc[~running].sum())
    if ev_dt_non_nan > 0 or alarm_dt_true > 0:
        print(f"   ⚠️  {s}: DT_evidence_non_NaN={ev_dt_non_nan}, "
              f"DT_alarm_true={alarm_dt_true}")
        issues += 1

if issues == 0:
    print(f"   ✅ All {len(standalone_sensors)} standalone sensors: "
          f"downtime properly excluded (NaN / False)")

print(f"\n✅ Cell 16 complete.")
print(f"   Standalone sensors scored : {len(standalone_sensors)}")
print(f"   Output dir                : {standalone_out_dir}")
print(f"   Files saved               : 4 (evidence, scores, alarms, summary)")
print(f"   ⚡ Downtime rows: NaN evidence / False alarm (never computed)")


# =============================================================================
# Cell 18: Process Level Risk Fusion and Significance
# =============================================================================
# Purpose:
#   Step 18A — Fuse all subsystem AE scores + standalone evidence into a
#               single process-level risk score using confidence-weighted
#               averaging.
#
#   Step 18B — Normalize the fused score into a historical significance
#               measure:  Significance_t = (ProcessRisk_t - mu_hist) / sigma_hist
#               Interpretation bands: <1 normal, 1-2 mild, 2-3 strong, >3 critical
#
# Outputs (saved to process_risk_outputs/ subfolder):
#   process_risk.csv / .parquet          — risk + significance + confidence
#   process_risk_dominant.csv / .parquet — dominant subsystem per minute
#   process_risk_summary.csv             — scalar summary statistics
# =============================================================================

print("=" * 70)
print("CELL 18: PROCESS LEVEL RISK FUSION AND SIGNIFICANCE")
print("=" * 70)

# ── Config defaults ───────────────────────────────────────────────────────────
if not hasattr(cfg, 'fusion_epsilon'):
    cfg.fusion_epsilon = 1e-8
if not hasattr(cfg, 'fusion_significance_window'):
    cfg.fusion_significance_window = 1440  # 24h of minutes

print(f"   Config: fusion_epsilon              = {cfg.fusion_epsilon}")
print(f"   Config: fusion_significance_window  = {cfg.fusion_significance_window} min")

# ── Collect all source signals for fusion ────────────────────────────────────
# Sources:
#   1. AE subsystem scores  → ae_results[sys_label]["system_score"]  (NaN=DT)
#      Weight               → ae_results[sys_label]["confidence"]    (NaN=DT)
#   2. Standalone sensors   → standalone_results[s]["evidence"]      (NaN=DT)
#      Weight               → SQS * (1 if alarm else 0.5) as proxy confidence

# Build per-source score and weight series (all aligned on df.index)

fusion_sources: Dict[str, Tuple[pd.Series, pd.Series]] = {}

# --- AE subsystems ---
for sys_label in sorted(ae_results.keys()):
    score  = ae_results[sys_label]["system_score"]    # [0,1], NaN=DT
    weight = ae_results[sys_label]["confidence"]       # [0,1], NaN=DT
    # Replace NaN weights with 0 for fusion (downtime contributes zero weight)
    fusion_sources[sys_label] = (score, weight)

# --- Standalone sensors ---
# Aggregate standalone sensors into a single "STANDALONE" composite score
# using mean evidence weighted by mean SQS.
if standalone_sensors:
    ev_cols   = [standalone_results[s]["evidence"] for s in standalone_sensors]
    sq_cols   = [
        sqs[s] if s in sqs.columns else pd.Series(1.0, index=df.index)
        for s in standalone_sensors
    ]
    # Stack and compute weighted mean across standalone sensors
    ev_mat  = pd.concat(ev_cols,  axis=1).fillna(0.0)
    sq_mat  = pd.concat(sq_cols,  axis=1).fillna(0.0)

    # Weight each sensor by its SQS
    ev_weighted = ev_mat.values * sq_mat.values
    sq_sum      = sq_mat.values.sum(axis=1, keepdims=True)
    sq_sum      = np.where(sq_sum < cfg.fusion_epsilon, cfg.fusion_epsilon, sq_sum)
    sa_score_arr = ev_weighted.sum(axis=1) / sq_sum.squeeze()

    # Composite confidence for standalone = mean SQS across sensors
    sa_conf_arr  = sq_mat.values.mean(axis=1)

    standalone_score_series = pd.Series(sa_score_arr, index=df.index)
    standalone_conf_series  = pd.Series(sa_conf_arr,  index=df.index)

    # Zero out downtime (NaN → 0 for score, NaN → 0 for weight)
    standalone_score_series.loc[~running] = np.nan
    standalone_conf_series.loc[~running]  = np.nan

    fusion_sources["STANDALONE"] = (standalone_score_series, standalone_conf_series)
    print(f"\n   Standalone composite: {len(standalone_sensors)} sensors → 1 source")
else:
    print("\n   No standalone sensors — fusion from AE subsystems only")

print(f"   Fusion sources total: {len(fusion_sources)} "
      f"({list(fusion_sources.keys())})")


# ── Step 18A: Process Level Risk Fusion ──────────────────────────────────────

def compute_process_risk_fusion(
    fusion_sources: Dict[str, Tuple[pd.Series, pd.Series]],
    running_mask: pd.Series,
    epsilon: float,
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    ProcessRisk_t = Σ(SubsystemScore_{i,t} × Weight_{i,t}) / (Σ Weight_{i,t} + ε)

    Returns:
      process_risk  : pd.Series [0,1] (NaN during downtime)
      total_weight  : pd.Series total weight (NaN during downtime)
      dominant_sys  : pd.Series name of dominant contributing subsystem
    """
    if not fusion_sources:
        nan_series = pd.Series(np.nan, index=running_mask.index)
        return nan_series, nan_series, pd.Series("N/A", index=running_mask.index)

    full_index = running_mask.index
    n = len(full_index)

    source_labels = list(fusion_sources.keys())
    score_mat  = np.full((n, len(source_labels)), np.nan)
    weight_mat = np.full((n, len(source_labels)), np.nan)

    for j, label in enumerate(source_labels):
        sc, wt = fusion_sources[label]
        score_mat[:, j]  = np.nan_to_num(sc.values,  nan=0.0)
        weight_mat[:, j] = np.nan_to_num(wt.values,  nan=0.0)

    # Zero out downtime in both matrices
    dt_mask_arr = (~running_mask).values
    score_mat[dt_mask_arr, :]  = 0.0
    weight_mat[dt_mask_arr, :] = 0.0

    weighted_sum  = np.sum(score_mat * weight_mat, axis=1)
    weight_total  = np.sum(weight_mat, axis=1)
    process_risk  = weighted_sum / (weight_total + epsilon)
    process_risk  = np.clip(process_risk, 0.0, 1.0)

    # Dominant contributing subsystem
    # = argmax(Score_i,t × Weight_i,t)
    contrib_mat = score_mat * weight_mat
    dominant_idx = np.argmax(contrib_mat, axis=1)
    dominant_arr = np.array([source_labels[i] for i in dominant_idx])

    # Set downtime rows back to NaN
    pr_series = pd.Series(process_risk, index=full_index, dtype=float)
    pr_series.loc[~running_mask] = np.nan

    wt_series = pd.Series(weight_total, index=full_index, dtype=float)
    wt_series.loc[~running_mask] = np.nan

    dom_series = pd.Series(dominant_arr, index=full_index, dtype=object)
    dom_series.loc[~running_mask] = np.nan

    return pr_series, wt_series, dom_series


process_risk, total_weight, dominant_subsystem = compute_process_risk_fusion(
    fusion_sources=fusion_sources,
    running_mask=running,
    epsilon=cfg.fusion_epsilon,
)

print(f"\n   ProcessRisk computed: shape={len(process_risk)}")
pr_run = process_risk.dropna()
print(f"   ProcessRisk (running): mean={pr_run.mean():.4f}  "
      f"max={pr_run.max():.4f}  "
      f"min={pr_run.min():.4f}")


# ── Step 18A: Overall Confidence ─────────────────────────────────────────────
#   Confidence_t = (Σ Weight_{i,t}) / N
#   where N = number of sources

N_sources = len(fusion_sources)

fusion_confidence = total_weight / max(N_sources, 1)
fusion_confidence = fusion_confidence.clip(0.0, 1.0)
fusion_confidence.loc[~running] = np.nan

print(f"   Fusion confidence (running): "
      f"mean={fusion_confidence.dropna().mean():.4f}")


# ── Step 18B: Process Risk Significance ──────────────────────────────────────

def compute_process_risk_significance(
    process_risk: pd.Series,
    running_mask: pd.Series,
    window: int,
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    Significance_t = (ProcessRisk_t - mu_hist) / sigma_hist

    mu_hist    = rolling mean of ProcessRisk over `window` running minutes
    sigma_hist = rolling std  of ProcessRisk over `window` running minutes

    Returns:
      significance : pd.Series (NaN during downtime)
      mu_hist      : pd.Series rolling mean
      sigma_hist   : pd.Series rolling std
    """
    full_index = running_mask.index

    # Compute rolling stats only on running timestamps, then reindex
    pr_running = process_risk.copy()  # NaN during downtime already

    mu_hist    = pr_running.rolling(window=window, min_periods=max(10, window // 10)).mean()
    sigma_hist = pr_running.rolling(window=window, min_periods=max(10, window // 10)).std()

    # Avoid division by zero
    sigma_safe = sigma_hist.copy()
    sigma_safe = sigma_safe.where(sigma_safe > 1e-8, other=1e-8)

    significance = (pr_running - mu_hist) / sigma_safe

    # Downtime rows stay NaN
    significance.loc[~running_mask] = np.nan
    mu_hist.loc[~running_mask]      = np.nan
    sigma_hist.loc[~running_mask]   = np.nan

    return significance, mu_hist, sigma_hist


significance, mu_hist, sigma_hist = compute_process_risk_significance(
    process_risk=process_risk,
    running_mask=running,
    window=cfg.fusion_significance_window,
)

sig_run = significance.dropna()
print(f"\n   Significance (running): mean={sig_run.mean():.4f}  "
      f"max={sig_run.max():.4f}  "
      f"p99={np.nanquantile(sig_run.values, 0.99):.4f}")

# Severity band counts
n_normal   = int((sig_run < 1).sum())
n_mild     = int(((sig_run >= 1) & (sig_run < 2)).sum())
n_strong   = int(((sig_run >= 2) & (sig_run < 3)).sum())
n_critical = int((sig_run >= 3).sum())
n_total_run = len(sig_run)

print(f"\n   Significance band counts (running minutes):")
print(f"     Normal   (< 1) : {n_normal:6d}  ({100*n_normal/max(n_total_run,1):.1f}%)")
print(f"     Mild   (1-2)   : {n_mild:6d}  ({100*n_mild/max(n_total_run,1):.1f}%)")
print(f"     Strong (2-3)   : {n_strong:6d}  ({100*n_strong/max(n_total_run,1):.1f}%)")
print(f"     Critical (>3)  : {n_critical:6d}  ({100*n_critical/max(n_total_run,1):.1f}%)")


# ── Build output DataFrames ───────────────────────────────────────────────────

process_risk_out_dir = os.path.join(cfg.output_dir, "process_risk_outputs")
os.makedirs(process_risk_out_dir, exist_ok=True)

# 1) Main process risk file
out_pr = pd.DataFrame(index=df.index)
out_pr.index.name = "timestamp_utc"
out_pr["downtime_flag"]        = downtime.astype(int)
out_pr["ProcessRisk"]          = process_risk
out_pr["Significance"]         = significance
out_pr["Significance_MuHist"]  = mu_hist
out_pr["Significance_SigHist"] = sigma_hist
out_pr["Fusion_Confidence"]    = fusion_confidence
out_pr["Total_Weight"]         = total_weight

# Severity label
def _sig_label(s):
    if pd.isna(s):
        return np.nan
    if s < 1:   return "Normal"
    if s < 2:   return "Mild"
    if s < 3:   return "Strong"
    return "Critical"

out_pr["Severity_Label"] = significance.map(_sig_label)

# Individual subsystem scores & weights (for traceability)
for label, (sc, wt) in fusion_sources.items():
    out_pr[f"Source_{label}__Score"]  = sc
    out_pr[f"Source_{label}__Weight"] = wt

pr_csv     = os.path.join(process_risk_out_dir, "process_risk.csv")
pr_parquet = os.path.join(process_risk_out_dir, "process_risk.parquet")
out_pr.to_csv(pr_csv)
out_pr.to_parquet(pr_parquet)
print(f"\n   Process Risk shape: {out_pr.shape}  💾 {pr_csv}")

# 2) Dominant subsystem file
out_dom = pd.DataFrame(index=df.index)
out_dom.index.name = "timestamp_utc"
out_dom["downtime_flag"]       = downtime.astype(int)
out_dom["ProcessRisk"]         = process_risk
out_dom["Dominant_Subsystem"]  = dominant_subsystem
out_dom["Fusion_Confidence"]   = fusion_confidence
out_dom["Severity_Label"]      = out_pr["Severity_Label"]

# Per-source contribution (score × weight, unnormalized) for traceability
for label, (sc, wt) in fusion_sources.items():
    contrib = sc.fillna(0.0) * wt.fillna(0.0)
    contrib.loc[~running] = np.nan
    out_dom[f"{label}__Contribution"] = contrib

dom_csv     = os.path.join(process_risk_out_dir, "process_risk_dominant.csv")
dom_parquet = os.path.join(process_risk_out_dir, "process_risk_dominant.parquet")
out_dom.to_csv(dom_csv)
out_dom.to_parquet(dom_parquet)
print(f"   Dominant shape: {out_dom.shape}  💾 {dom_csv}")

# 3) Summary statistics
dominant_counts = dominant_subsystem.dropna().value_counts()
top_dom = dominant_counts.index[0] if len(dominant_counts) > 0 else "N/A"
top_dom_pct = (
    100.0 * dominant_counts.iloc[0] / max(dominant_counts.sum(), 1)
    if len(dominant_counts) > 0 else 0.0
)

summary_pr = {
    "Total_Timestamps":          len(df),
    "Running_Timestamps":        int(running.sum()),
    "Downtime_Timestamps":       int((~running).sum()),
    "ProcessRisk_Mean":          round(float(pr_run.mean()), 5),
    "ProcessRisk_Max":           round(float(pr_run.max()), 5),
    "ProcessRisk_P95":           round(float(np.nanquantile(pr_run.values, 0.95)), 5),
    "ProcessRisk_P99":           round(float(np.nanquantile(pr_run.values, 0.99)), 5),
    "Significance_Mean":         round(float(sig_run.mean()), 4),
    "Significance_Max":          round(float(sig_run.max()), 4),
    "Significance_P99":          round(float(np.nanquantile(sig_run.values, 0.99)), 4),
    "N_Normal":                  n_normal,
    "N_Mild":                    n_mild,
    "N_Strong":                  n_strong,
    "N_Critical":                n_critical,
    "Pct_Normal":                round(100.0 * n_normal   / max(n_total_run, 1), 2),
    "Pct_Mild":                  round(100.0 * n_mild     / max(n_total_run, 1), 2),
    "Pct_Strong":                round(100.0 * n_strong   / max(n_total_run, 1), 2),
    "Pct_Critical":              round(100.0 * n_critical / max(n_total_run, 1), 2),
    "Fusion_Confidence_Mean":    round(float(fusion_confidence.dropna().mean()), 4),
    "N_Fusion_Sources":          N_sources,
    "Fusion_Source_Labels":      str(list(fusion_sources.keys())),
    "Top_Dominant_Subsystem":    top_dom,
    "Top_Dominant_Pct":          round(top_dom_pct, 2),
}

summary_pr_df = pd.DataFrame([summary_pr]).T.reset_index()
summary_pr_df.columns = ["Metric", "Value"]
sum_pr_csv = os.path.join(process_risk_out_dir, "process_risk_summary.csv")
summary_pr_df.to_csv(sum_pr_csv, index=False)
print(f"   Summary shape: {summary_pr_df.shape}  💾 {sum_pr_csv}")

# ── Downtime exclusion verification ──────────────────────────────────────────
print("\n--- Process risk downtime exclusion verification ---")
pr_dt_non_nan   = int(process_risk.loc[~running].notna().sum())
sig_dt_non_nan  = int(significance.loc[~running].notna().sum())
conf_dt_non_nan = int(fusion_confidence.loc[~running].notna().sum())
all_ok_pr = (pr_dt_non_nan == 0 and sig_dt_non_nan == 0 and conf_dt_non_nan == 0)
status_pr = "✅" if all_ok_pr else "⚠️"
print(f"   {status_pr} DT_ProcessRisk_non_NaN={pr_dt_non_nan}, "
      f"DT_Significance_non_NaN={sig_dt_non_nan}, "
      f"DT_Confidence_non_NaN={conf_dt_non_nan}")

# ── Final print summary ───────────────────────────────────────────────────────
print(f"\n{'=' * 70}")
print("PROCESS RISK FUSION SUMMARY")
print(f"{'=' * 70}")
print(summary_pr_df.to_string(index=False))

print(f"\n   Dominant subsystem breakdown (running minutes):")
for label, cnt in dominant_counts.items():
    pct = 100.0 * cnt / max(dominant_counts.sum(), 1)
    print(f"     {label}: {cnt} min ({pct:.1f}%)")

print(f"\n✅ Cell 18 complete.")
print(f"   Process Risk & Significance computed for {int(running.sum())} running minutes")
print(f"   Fusion sources: {list(fusion_sources.keys())}")
print(f"   Output dir: {process_risk_out_dir}")
print(f"   Files saved: 3 (process_risk, dominant, summary)")
print(f"   ⚡ Downtime rows: NaN (excluded from fusion, not forced to zero)")
print(f"   📊 Significance bands — Normal:<1  Mild:1-2  Strong:2-3  Critical:>3")

# %%
# =============================================================================
# Cell 19: Publish All Beta Artifacts
# =============================================================================
# This runs LAST so that standalone_outputs/ and process_risk_outputs/ exist.

published_beta_paths = publish_beta_artifacts(
    output_dir=cfg.output_dir,
    beta_publish_dir=cfg.beta_publish_dir,
)
print(f"\n{'=' * 70}")
print("BETA PUBLISH")
print(f"{'=' * 70}")
print(f"   Published {len(published_beta_paths)} artifacts to {cfg.beta_publish_dir}")
print(f"   Includes: standalone_outputs/, process_risk_outputs/, sensor_values, alerts, configs")

# %%



