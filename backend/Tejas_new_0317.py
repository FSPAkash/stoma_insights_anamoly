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
    ae_min_sensors: int = 3
    ae_min_training_rows: int = 300
    ae_missing_max: float = 0.10
    ae_mean_abs_corr_min: float = 0.05
    ae_risk_sigma_factor: float = 3.0
    ae_slew_rate: float = 0.0005
    ae_gate_factor: float = 1.5
    ae_threshold_window: int = 100
    ae_on_delay: int = 10
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
    output_dir: str = "pipeline_outputs_PLAY"
    BASELINE_FILE: str = "sensor_baseline_range_season_2024-2025.xlsx"


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
# Cell 6: Load Baseline + Compute Missing Sensors
# =============================================================================
BASELINE_FILE = cfg.BASELINE_FILE


def compute_single_sensor_bounds(x, n_min):
    x = x.dropna()
    if len(x) < n_min:
        return np.nan, np.nan, np.nan
    p01 = float(np.nanquantile(x.values, 0.001))
    p999 = float(np.nanquantile(x.values, 0.999))
    dx = x.diff().abs().dropna()
    roc999 = (
        float(np.nanquantile(dx.values, 0.999))
        if len(dx) >= n_min
        else np.nan
    )
    return p01, p999, roc999


def load_sensor_cfg_with_logging(df, running_mask, cfg):
    running_df = df.loc[running_mask]

    if os.path.exists(BASELINE_FILE):
        print(f"📂 Loading baseline: {BASELINE_FILE}")
        baseline = pd.read_excel(BASELINE_FILE)
    else:
        print("⚠️ Baseline file not found — all sensors will be computed")
        baseline = pd.DataFrame(columns=["sensor", "p0_1", "p99_9", "roc_p99_9"])

    baseline["sensor_key"] = baseline["sensor"].str.strip().str.lower()
    baseline_lookup = baseline.set_index("sensor_key")

    rows = []
    sensors_from_baseline = []
    sensors_computed = []

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
            x = running_df[s]
            p01, p999, roc999 = compute_single_sensor_bounds(x, cfg.n_stable_min)
            rows.append({
                "sensor": s,
                "p0_1": p01,
                "p99_9": p999,
                "roc_p99_9": roc999,
            })
            sensors_computed.append(s)

    sensor_cfg = pd.DataFrame(rows)

    print("\n================ SENSOR BASELINE SUMMARY ================")
    print(f"Total sensors in df: {len(df.columns)}")
    print(f"Using baseline:     {len(sensors_from_baseline)}")
    print(f"Computed new:       {len(sensors_computed)}")
    if len(sensors_from_baseline) > 0:
        print("\n📌 Sensors loaded from baseline:")
        print(", ".join(sensors_from_baseline))
    if len(sensors_computed) > 0:
        print("\n⚙️ Sensors computed from data:")
        print(", ".join(sensors_computed))
    print("=========================================================\n")

    return sensor_cfg


sensor_cfg = load_sensor_cfg_with_logging(df, running, cfg)
print(f"✅ Sensor config ready: {len(sensor_cfg)} sensors")

# %%
# =============================================================================
# Cell 7: SQS — Simplified Single-Pass (RUNNING only)
# =============================================================================
def compute_sqs(df: pd.DataFrame, sensor_cfg: pd.DataFrame,
                running_mask: pd.Series) -> pd.DataFrame:
    """
    Single-pass SQS: each sensor scored against its own learned bounds.
    Downtime rows get SQS = NaN (excluded from all analysis).
    """
    cfg_idx = sensor_cfg.set_index("sensor")
    sqs = pd.DataFrame(np.nan, index=df.index, columns=df.columns, dtype=float)

    for s in df.columns:
        x = df[s]
        score = pd.Series(np.nan, index=df.index, dtype=float)
        # Only score RUNNING rows
        present = x.notna() & running_mask
        score.loc[present] = 1.0

        if s not in cfg_idx.index:
            sqs[s] = score
            continue

        row = cfg_idx.loc[s]
        p0, p9, roc = row["p0_1"], row["p99_9"], row["roc_p99_9"]

        if np.isfinite(p0) and np.isfinite(p9):
            margin = (p9 - p0) * 0.01
            oob = present & ((x < p0 - margin) | (x > p9 + margin))
            score.loc[oob] *= 0.60

        if np.isfinite(roc):
            jump = present & (x.diff().abs() > (roc * 1.25))
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
        "sqs_mean": sqs_mean, "sqs_p10": sqs_p10,
        "sqs_valid_frac": sqs_valid_frac,
    }, index=sqs.index)


sqs = compute_sqs(df, sensor_cfg, running)
sqs_summary = summarize_sqs(sqs)

print(f"   SQS shape: {sqs.shape}")
print(f"   SQS global mean (running): {sqs_summary.loc[running, 'sqs_mean'].mean():.4f}")
print("✅ SQS complete (downtime excluded).")

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
# Deep AutoEncoder with Directional Residual Operator and Robust Adaptive
# Thresholding.  Trained on RUNNING data only.  Produces one system-level
# risk score per subsystem per timestamp.
# =============================================================================

print("=" * 70)
print("AUTOENCODER SUBSYSTEM SCORING")
print("=" * 70)


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
        target = float(np.nanmean(risk[max(0, t - window):t])) + sigma_factor * baseline_sigma
        # Gating: freeze if risk is anomalously high
        if risk[t] > gate * threshold[t - 1]:
            target = threshold[t - 1]
        change = np.clip(target - threshold[t - 1], -slew_rate, slew_rate)
        threshold[t] = threshold[t - 1] + change

    return threshold


def select_ae_candidates(
    df: pd.DataFrame,
    running_mask: pd.Series,
    sensors: List[str],
    missing_max: float,
    min_abs_corr: float,
) -> List[str]:
    """Select sensors suitable for AE training within a subsystem."""
    present = [s for s in sensors if s in df.columns]
    if len(present) < 2:
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
    return [s for s in good if float(mac_mean.get(s, 0.0)) >= min_abs_corr]


def train_ae_subsystem(
    df: pd.DataFrame,
    running_mask: pd.Series,
    sensors: List[str],
    cfg: PipelineConfig,
) -> Optional[Dict]:
    """
    Train an AutoEncoder on RUNNING data for the given sensor group.

    Returns dict with model, scaler, baseline stats, sensor list, or None.
    """
    candidates = select_ae_candidates(
        df, running_mask, sensors, cfg.ae_missing_max, cfg.ae_mean_abs_corr_min
    )
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

    model = DeepAutoEncoder(input_dim, latent_dim)
    optimizer = optim.Adam(model.parameters(), lr=cfg.ae_lr)
    criterion = nn.MSELoss()

    # Training loop
    model.train()
    n_samples = train_tensor.shape[0]
    history = []
    for epoch in range(cfg.ae_epochs):
        # Mini-batch
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

    # Global risk on training data (for adaptive threshold seeding)
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


def score_ae_subsystem(
    df: pd.DataFrame,
    running_mask: pd.Series,
    ae_info: Dict,
    cfg: PipelineConfig,
) -> Dict:
    """
    Score the FULL timeline using trained AE.
    Returns dict with:
      - system_score: pd.Series [0,1] per timestamp (0 during downtime)
      - per_sensor_sigma: pd.DataFrame of sigma-scaled directional errors
      - threshold: np.ndarray adaptive threshold
      - alarm: pd.Series boolean alarm
    """
    model = ae_info["model"]
    scaler = ae_info["scaler"]
    sensors = ae_info["sensors"]
    baseline_sigmas = ae_info["baseline_sigmas"]
    risk_train_sigma = ae_info["risk_train_sigma"]

    # Prepare full data (fill for continuity, but score only running)
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

    # Adaptive threshold (trained on running baseline risk)
    threshold = robust_adaptive_threshold(
        risk_full,
        risk_train_sigma,
        window=cfg.ae_threshold_window,
        slew_rate=cfg.ae_slew_rate,
        gate=cfg.ae_gate_factor,
        sigma_factor=cfg.ae_risk_sigma_factor,
    )

    # On/Off delay alarm
    alarm = _on_off_delay_alarm(
        risk_full, threshold, cfg.ae_on_delay, cfg.ae_off_delay
    )

    # System score: risk clipped to [0,1], zeroed during downtime
    system_score = pd.Series(np.clip(risk_full, 0, 1), index=df.index)
    system_score.loc[~running_mask] = 0.0

    return {
        "system_score": system_score,
        "per_sensor_sigma": per_sensor_sigma,
        "threshold": threshold,
        "alarm": pd.Series(alarm, index=df.index),
        "risk_raw": risk_full,
    }


def _on_off_delay_alarm(
    risk: np.ndarray, threshold: np.ndarray, on_delay: int, off_delay: int
) -> np.ndarray:
    """State-machine: alarm ON after on_delay consecutive above-threshold."""
    n = len(risk)
    alarms = np.zeros(n, dtype=bool)
    count = 0
    active = False
    for i in range(n):
        is_high = risk[i] > threshold[i]
        if not active:
            if is_high:
                count += 1
                if count >= on_delay:
                    active = True
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


# =================== TRAIN & SCORE ALL SUBSYSTEMS ===================

ae_models: Dict[str, Dict] = {}      # sys_label -> ae_info
ae_results: Dict[str, Dict] = {}     # sys_label -> scoring results

for sys_label, sensors in catalog.items():
    if sys_label == "ISOLATED":
        # No AE for isolated sensors (no correlated group)
        print(f"   {sys_label}: skipped (no correlated group for AE)")
        continue

    present = [s for s in sensors if s in df.columns]
    if len(present) < cfg.ae_min_sensors:
        print(f"   {sys_label}: skipped ({len(present)} sensors < min {cfg.ae_min_sensors})")
        continue

    print(f"\n   Training AE for {sys_label} ({len(present)} sensors)...")
    ae_info = train_ae_subsystem(df, running, present, cfg)

    if ae_info is None:
        print(f"   {sys_label}: AE training failed (insufficient data/candidates)")
        continue

    ae_models[sys_label] = ae_info
    ae_result = score_ae_subsystem(df, running, ae_info, cfg)
    ae_results[sys_label] = ae_result

    n_alarms = int(ae_result["alarm"].sum())
    score_mean = float(ae_result["system_score"].loc[running].mean())
    final_loss = ae_info["history"][-1] if ae_info["history"] else np.nan
    print(f"   ✅ {sys_label} AE: {len(ae_info['sensors'])} sensors → "
          f"latent_dim={ae_info['latent_dim']} | "
          f"final_loss={final_loss:.6f} | "
          f"score_mean(running)={score_mean:.4f} | "
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
    plt.suptitle("AutoEncoder Training Convergence", fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    ae_loss_path = os.path.join(cfg.output_dir, "ae_training_loss.png")
    plt.savefig(ae_loss_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"   💾 Saved AE training loss plot: {ae_loss_path}")

print(f"\n✅ AutoEncoder complete for {len(ae_models)} subsystems.")

# %%

# %%
# =============================================================================
# Cell 10: Engine A — Drift Detection (RUNNING only, real sensors only)
# =============================================================================
# Includes ALL sensors from catalog (systems + ISOLATED).
# NO virtual/residual sensors. Downtime periods excluded.
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


def get_all_real_sensors(catalog: Dict[str, List[str]]) -> List[str]:
    """Get all unique real sensors across ALL systems including ISOLATED."""
    all_sensors = []
    seen = set()
    for sensors in catalog.values():
        for s in sensors:
            if s not in seen:
                all_sensors.append(s)
                seen.add(s)
    return all_sensors


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


print("--- Engine A (all real sensors, RUNNING only) ---")
A_sensors = get_all_real_sensors(catalog)
print(f"   All real sensors for Engine A: {len(A_sensors)}")

A, baseline_cache = compute_engineA(
    df, sqs, running, A_sensors,
    cfg.engineA_baseline_win, cfg.engineA_mad_win, cfg.engineA_sqs_min, cfg.engineA_score_k,
)
print(f"   Engine A shape: {A.shape}")
print(f"   Engine A mean score (running): {A.loc[running].mean().mean():.4f}")
print("✅ Engine A complete (downtime excluded, real sensors only).")

# %%
# =============================================================================
# Cell 11: Engine B — Periodicity Detection (RUNNING only, real sensors only)
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
    """Select sensors for Engine B from ALL real sensors."""
    eligible = []
    for s in all_sensors:
        if s not in df.columns:
            continue
        if s not in sqs.columns:
            continue
        ok = running_mask & df[s].notna() & (sqs[s] >= sqs_min)
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
        for t in range(win, len(df)):
            if not running_mask.iloc[t]:
                continue
            w_arr = u.iloc[t - win:t].values
            if s not in sqs.columns:
                continue
            sq = sqs[s].iloc[t - win:t].values
            valid = np.isfinite(w_arr) & (sq >= sqs_min)
            if valid.mean() < valid_frac_min:
                continue
            ratio = spectral_energy_ratio(w_arr[valid], period_min, period_max)
            score = (ratio - e_low) / (e_high - e_low + EPS)
            B.iloc[t, B.columns.get_loc(s)] = float(np.clip(score, 0, 1))
    return B


print("--- Engine B (all real sensors, RUNNING only) ---")
B_sensors = select_engineB_sensors(df, running, sqs, cfg.engineB_sqs_min, A_sensors)
print(f"   Eligible sensors for Engine B: {len(B_sensors)}")

B = compute_engineB(
    df, sqs, running, B_sensors, baseline_cache,
    cfg.engineB_win, cfg.engineB_sqs_min, cfg.engineB_valid_frac_min,
    cfg.engineB_period_min, cfg.engineB_period_max,
)
print(f"   Engine B shape: {B.shape}, non-null: {B.notna().sum().sum():,}")
print("✅ Engine B complete (downtime excluded, real sensors only).")


# %%
# =============================================================================
# Cell 12: Build Per-System Detailed Output File
# =============================================================================
# Output format per the requirement:
#
# timestamp_utc | Downtime_flag |
#   System_1 | System_Score | S1-Invalid | S1-SQS | S1-EngA | S1-EngB | ...
#   System_2 | System_Score | S2-Invalid | S2-SQS | S2-EngA | S2-EngB | ...
#   ...
# =============================================================================

print("--- Building detailed per-system output file ---")


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
) -> pd.DataFrame:
    """
    Build a wide DataFrame with hierarchical columns:
      timestamp_utc | downtime_flag |
        For each system:  system_score |
          For each sensor: invalid_flag | sqs | engine_a | engine_b
    """
    out = pd.DataFrame(index=df.index)
    out.index.name = "timestamp_utc"
    out["downtime_flag"] = downtime.astype(int)

    for sys_label in sorted(catalog.keys()):
        sensors = catalog[sys_label]

        # System score from AE (0 for ISOLATED or systems without AE)
        if sys_label in ae_results:
            out[f"{sys_label}__System_Score"] = ae_results[sys_label]["system_score"]
        else:
            out[f"{sys_label}__System_Score"] = 0.0

        for s in sensors:
            prefix = f"{sys_label}__{s}"

            # Invalid flag: from base_bad (1 = invalid, 0 = valid)
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

    return out


detailed_output = build_detailed_system_output(
    df, downtime, running, catalog, ae_results, A, B, sqs, base_bad
)

# Save combined
detailed_path_csv = os.path.join(cfg.output_dir, "detailed_system_sensor_scores.csv")
detailed_path_parquet = os.path.join(cfg.output_dir, "detailed_system_sensor_scores.parquet")
detailed_output.to_csv(detailed_path_csv)
detailed_output.to_parquet(detailed_path_parquet)

print(f"   Detailed output shape: {detailed_output.shape}")
print(f"   💾 Saved: {detailed_path_csv}")
print(f"   💾 Saved: {detailed_path_parquet}")

# =============================================================================
# Build 4 separate files: Engine_A, Engine_B, Subsystem_Scores, SQS
# =============================================================================
print("\n--- Building 4 separate output files ---")

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
print(f"   💾 Saved: {a_csv}")
print(f"   💾 Saved: {a_parquet}")

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
print(f"   💾 Saved: {b_csv}")
print(f"   💾 Saved: {b_parquet}")

# --- 3) Subsystem Scores file ---
out_sub = pd.DataFrame(index=df.index)
out_sub.index.name = "timestamp_utc"
out_sub["downtime_flag"] = downtime.astype(int)
for sys_label in sorted(catalog.keys()):
    if sys_label in ae_results:
        out_sub[f"{sys_label}__System_Score"] = ae_results[sys_label]["system_score"]
    else:
        out_sub[f"{sys_label}__System_Score"] = 0.0

sub_csv = os.path.join(cfg.output_dir, "detailed_subsystem_scores.csv")
sub_parquet = os.path.join(cfg.output_dir, "detailed_subsystem_scores.parquet")
out_sub.to_csv(sub_csv)
out_sub.to_parquet(sub_parquet)
print(f"   Subsystem Scores shape: {out_sub.shape}")
print(f"   💾 Saved: {sub_csv}")
print(f"   💾 Saved: {sub_parquet}")

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
print(f"   💾 Saved: {sqs_csv}")
print(f"   💾 Saved: {sqs_parquet}")

# Preview columns structure
sys_labels_in_output = sorted(catalog.keys())
print(f"\n   Systems in output: {sys_labels_in_output}")
for sys_label in sys_labels_in_output:
    sys_cols = [c for c in detailed_output.columns if c.startswith(f"{sys_label}__")]
    print(f"   {sys_label}: {len(sys_cols)} columns")
    if len(sys_cols) <= 10:
        for c in sys_cols:
            print(f"      {c}")
    else:
        for c in sys_cols[:5]:
            print(f"      {c}")
        print(f"      ... ({len(sys_cols) - 5} more)")

print("✅ Detailed system output complete.")

# %%
# =============================================================================
# Cell 13: Block Scoring → Fusion → Classification → Risk (Dynamic)
# =============================================================================
# System-level score = AE score per subsystem
# Sensor-level scores = Engine A + Engine B
# Fusion: weighted combination of system scores
# =============================================================================

def merge_engine_scores(A, B):
    """Merge Engine A and B: take max(A, B) per sensor per timestamp."""
    out = A.copy()
    if B is None or B.empty:
        return out
    for s in out.columns:
        if s in B.columns:
            out[s] = np.nanmax(np.vstack([out[s].values, B[s].values]), axis=0)
    for s in B.columns:
        if s not in out.columns:
            out[s] = B[s]
    return out


def topk_mean(score_df, sensors, k):
    """Average of top-k most anomalous sensors."""
    sensors = [s for s in sensors if s in score_df.columns]
    if not sensors:
        return pd.Series(np.nan, index=score_df.index)
    arr = score_df[sensors].values
    result = np.full(arr.shape[0], np.nan)
    for i in range(arr.shape[0]):
        row = arr[i, :]
        valid = row[np.isfinite(row)]
        if len(valid) == 0:
            continue
        topk = np.sort(valid)[::-1][:min(k, len(valid))]
        result[i] = np.mean(topk)
    return pd.Series(result, index=score_df.index)


print("--- Block Scoring (Dynamic Systems) ---")
combined = merge_engine_scores(A, B)

# Ensure ISOLATED has weights
if "ISOLATED" not in dynamic_weights:
    dynamic_weights["ISOLATED"] = 0.05
if "ISOLATED" not in dynamic_risk_weights:
    dynamic_risk_weights["ISOLATED"] = 0.05

system_scores: Dict[str, pd.Series] = {}

for sys_label, sensors in catalog.items():
    present = [s for s in sensors if s in df.columns]
    if not present:
        continue

    if sys_label in ae_results:
        # Use AE system score
        ae_score = ae_results[sys_label]["system_score"]
        # Also compute sensor evidence from Engine A+B
        sensor_evidence = topk_mean(combined, present, cfg.block_score_topk)
        # Combine: AE system-level + sensor-level evidence
        ae_vals = ae_score.fillna(0).values
        se_vals = sensor_evidence.fillna(0).values
        score = np.clip(0.5 * ae_vals + 0.5 * se_vals, 0, 1)
        score = pd.Series(score, index=df.index)
    else:
        # ISOLATED or systems without AE: use sensor evidence only
        score = topk_mean(combined, present, cfg.block_score_topk)

    score.loc[downtime] = 0.0
    system_scores[sys_label] = score

    has_ae = "✓" if sys_label in ae_results else "✗"
    print(f"   {sys_label} [AE:{has_ae}] score (running mean): "
          f"{score[running].mean():.4f}")


# --- Dynamic Fusion ---
def dynamic_fuse(system_scores, dynamic_weights, index):
    parts, weights = [], []
    for label, score_s in system_scores.items():
        w = dynamic_weights.get(label, 0.0)
        parts.append(score_s.fillna(0).values)
        weights.append(w)
    if not parts:
        return pd.Series(0.0, index=index)
    W = np.array(weights)
    X = np.vstack(parts)
    return pd.Series((W @ X) / (W.sum() + EPS), index=index)


subsystem_score = dynamic_fuse(system_scores, dynamic_weights, df.index)


# --- Dynamic Classification ---
def dynamic_classify(system_scores, subsystem_score, cfg):
    label = pd.Series("NORMAL", index=subsystem_score.index)
    s = subsystem_score.fillna(0)

    if not system_scores:
        return label

    score_matrix = pd.DataFrame(
        {k: v.fillna(0) for k, v in system_scores.items()},
        index=subsystem_score.index,
    )
    dominant = score_matrix.idxmax(axis=1)

    active = s >= cfg.medium
    for sys_label in system_scores:
        sys_mask = active & (dominant == sys_label)
        label.loc[sys_mask] = sys_label

    non_isolated_scores = {k: v for k, v in system_scores.items() if k != "ISOLATED"}
    if len(non_isolated_scores) >= 2:
        elevated_count = pd.Series(0, index=subsystem_score.index)
        for v in non_isolated_scores.values():
            elevated_count += (v.fillna(0) >= cfg.medium * 0.6).astype(int)
        process_mask = active & (elevated_count >= 2)
        label.loc[process_mask] = "PROCESS"

    return label


cls = dynamic_classify(system_scores, subsystem_score, cfg)


# --- Dynamic Risk Scores ---
def dynamic_risk_scores(mode, system_scores, sqs_summary, dynamic_risk_weights):
    gate = pd.Series(1.0, index=mode.index)
    gate.loc[mode == "DOWNTIME"] = 0.0
    conf = pd.Series(
        np.clip(sqs_summary["sqs_mean"].fillna(0).values, 0, 1), index=mode.index
    )

    risk_parts = {}
    weights, parts_arr = [], []
    for label, score_s in system_scores.items():
        w = dynamic_risk_weights.get(label, 0.0)
        risk_parts[f"risk_{label}"] = score_s.fillna(0) * conf * gate
        weights.append(w)
        parts_arr.append(score_s.fillna(0).values)

    if not parts_arr:
        risk_parts["risk_score"] = pd.Series(0.0, index=mode.index)
        return pd.DataFrame(risk_parts, index=mode.index)

    W = np.array(weights)
    X = np.vstack(parts_arr)
    risk_parts["risk_score"] = pd.Series((W @ X) / (W.sum() + EPS), index=mode.index)
    return pd.DataFrame(risk_parts, index=mode.index)


risk_df = dynamic_risk_scores(mode, system_scores, sqs_summary, dynamic_risk_weights)


# --- Sensor-level risk decomposition ---
def build_dynamic_sensor_risk_decomposition(
    index, combined_scores, system_scores_dict,
    catalog, ae_results,
    sqs_summary, mode, dynamic_risk_weights, cfg,
):
    score_df = combined_scores.copy()
    score_df.columns = score_df.columns.map(str)

    conf = pd.Series(np.clip(sqs_summary["sqs_mean"].fillna(0).values, 0, 1), index=index)
    gate = pd.Series(1.0, index=index)
    gate.loc[mode == "DOWNTIME"] = 0.0

    rw_sum = sum(dynamic_risk_weights.values())

    sys_specs = []
    for label, sensors_list in catalog.items():
        if label not in system_scores_dict:
            continue
        w = dynamic_risk_weights.get(label, 0.0) / (rw_sum + EPS)
        present = [str(s) for s in sensors_list if str(s) in score_df.columns]
        has_ae = label in ae_results
        sys_specs.append((label, present, system_scores_dict[label], w, has_ae))

    rows = []
    k = cfg.block_score_topk

    for i, ts in enumerate(index):
        conf_i = float(conf.iloc[i])
        gate_i = float(gate.iloc[i])
        if gate_i <= 0:
            continue  # Skip downtime

        for subsystem, sensors, final_series, w_sub, has_ae in sys_specs:
            final_val = float(final_series.iloc[i]) if np.isfinite(final_series.iloc[i]) else 0.0
            final_val = max(0.0, final_val)
            if final_val <= 0:
                continue

            row_scores = score_df.iloc[i]
            values = []
            for s in sensors:
                v = row_scores.get(s, np.nan)
                if np.isfinite(v) and v > 0:
                    values.append((s, float(v)))

            if not values:
                continue

            values_sorted = sorted(values, key=lambda x: x[1], reverse=True)
            top = values_sorted[:min(k, len(values_sorted))]
            denom = float(sum(v for _, v in top)) if top else 1.0

            # AE contribution
            ae_component = 0.0
            if has_ae and subsystem in ae_results:
                ae_score_val = float(ae_results[subsystem]["system_score"].iloc[i])
                ae_component = ae_score_val if np.isfinite(ae_score_val) else 0.0

            for s, v in values_sorted:
                if v <= 0:
                    continue
                alloc = final_val * (v / (denom + EPS))
                rows.append({
                    "timestamp_utc": ts, "sensor_id": s,
                    "subsystem": subsystem,
                    "sensor_score_component": alloc,
                    "ae_system_score": ae_component,
                    "risk_weight": w_sub, "confidence_factor": conf_i,
                    "gate_factor": gate_i,
                    "risk_score_component": alloc * w_sub * conf_i * gate_i,
                })

    return pd.DataFrame(rows)


risk_sensor_decomposition = build_dynamic_sensor_risk_decomposition(
    df.index, combined, system_scores,
    catalog, ae_results,
    sqs_summary, mode, dynamic_risk_weights, cfg,
)

print(f"\n   Subsystem score (running mean): {subsystem_score[running].mean():.4f}")
print(f"   Classification:\n{cls.value_counts().to_string()}")
print(f"   Risk score mean: {risk_df['risk_score'].mean():.4f}")
print(f"   Risk > MEDIUM: {(risk_df['risk_score'] > cfg.medium).sum():,} min")
print(f"   Risk > HIGH:   {(risk_df['risk_score'] > cfg.high).sum():,} min")
print(f"   Decomposition rows: {len(risk_sensor_decomposition):,}")
print("✅ Dynamic scoring complete.")

# =============================================================================
# Cell 14: Alert Episodes
# =============================================================================

def build_alert_episodes(
    ts, score, label, sensor_score_df, sensor_catalog,
    min_duration, merge_gap, threshold,
):
    def class_to_sensor_candidates(main_class, sensor_catalog):
        for sys_label, sensors in sensor_catalog.items():
            if main_class == sys_label:
                return sensors
        if main_class == "PROCESS":
            return [s for k, v in sensor_catalog.items() if k != "ISOLATED" for s in v]
        return [s for v in sensor_catalog.values() for s in v]

    def summarize_episode_sensors(score_df, candidates, start_idx, end_idx):
        available = [s for s in candidates if s in score_df.columns]
        if not available:
            return pd.DataFrame(columns=[
                "sensor", "sensor_rank", "sensor_peak_score", "sensor_mean_score",
            ])
        seg = score_df.iloc[start_idx:end_idx + 1][available]
        if seg.empty:
            return pd.DataFrame(columns=[
                "sensor", "sensor_rank", "sensor_peak_score", "sensor_mean_score",
            ])
        peak = seg.max(axis=0, skipna=True)
        mean_ = seg.mean(axis=0, skipna=True)
        summary = pd.DataFrame({
            "sensor": peak.index.astype(str),
            "sensor_peak_score": peak.values.astype(float),
            "sensor_mean_score": mean_.reindex(peak.index).values.astype(float),
        })
        summary = summary[summary["sensor_peak_score"].notna()]
        positive = summary[summary["sensor_peak_score"] > 0]
        if not positive.empty:
            summary = positive
        summary = summary.sort_values(
            ["sensor_peak_score", "sensor_mean_score"], ascending=[False, False]
        ).reset_index(drop=True)
        summary["sensor_rank"] = np.arange(1, len(summary) + 1)
        return summary[["sensor", "sensor_rank", "sensor_peak_score", "sensor_mean_score"]]

    sensor_score_df = sensor_score_df.copy()
    sensor_score_df.columns = sensor_score_df.columns.map(str)
    sensor_catalog_str = {k: [str(s) for s in v] for k, v in sensor_catalog.items()}

    is_on = (score >= threshold).fillna(False).values
    intervals = []
    start = None
    for i, on in enumerate(is_on):
        if on and start is None:
            start = i
        if not on and start is not None:
            intervals.append((start, i - 1))
            start = None
    if start is not None:
        intervals.append((start, len(is_on) - 1))

    merged = []
    for a, b in intervals:
        if not merged:
            merged.append([a, b])
        elif a - merged[-1][1] - 1 <= merge_gap:
            merged[-1][1] = b
        else:
            merged.append([a, b])

    episodes, sensor_rows = [], []
    for a, b in merged:
        dur = b - a + 1
        if dur < min_duration:
            continue
        seg = label.iloc[a:b + 1]
        main = seg.value_counts().index[0] if len(seg) else "UNKNOWN"
        cands = class_to_sensor_candidates(main, sensor_catalog_str)
        ss = summarize_episode_sensors(sensor_score_df, cands, a, b)

        if ss.empty:
            sid, smax, smean, cnt, aff = "UNKNOWN", np.nan, np.nan, 0, "UNKNOWN"
        else:
            top = ss.iloc[0]
            sid = str(top["sensor"])
            smax = float(top["sensor_peak_score"])
            smean = float(top["sensor_mean_score"])
            cnt = len(ss)
            aff = "|".join(ss["sensor"].astype(str).tolist())

        alert_max = float(score.iloc[a:b + 1].max())
        alert_mean = float(score.iloc[a:b + 1].mean())
        severity = "HIGH" if alert_max >= 0.85 else "MEDIUM"

        # Include AE alarm info for the dominant system
        ae_alarm_count = 0
        if main in ae_results:
            ae_alarm_seg = ae_results[main]["alarm"].iloc[a:b + 1]
            ae_alarm_count = int(ae_alarm_seg.sum())

        episodes.append({
            "start_ts": ts[a], "end_ts": ts[b], "duration_minutes": dur,
            "severity": severity, "class": main,
            "sensor_id": sid,
            "sensor_max_score": smax, "sensor_mean_score": smean,
            "affected_sensor_count": cnt,
            "affected_sensors": aff,
            "max_score": alert_max, "mean_score": alert_mean,
            "ae_alarm_minutes": ae_alarm_count,
        })
        for row in ss.itertuples(index=False):
            sensor_rows.append({
                "start_ts": ts[a], "end_ts": ts[b], "duration_minutes": dur,
                "severity": severity, "class": main,
                "sensor": str(row.sensor), "sensor_rank": int(row.sensor_rank),
                "sensor_peak_score": float(row.sensor_peak_score),
                "sensor_mean_score": float(row.sensor_mean_score),
                "alert_max_score": alert_max, "alert_mean_score": alert_mean,
            })

    return pd.DataFrame(episodes), pd.DataFrame(sensor_rows)


print("--- Alerts ---")
alerts_med, alerts_sensor_med = build_alert_episodes(
    df.index, risk_df["risk_score"], cls, combined, catalog,
    cfg.alert_min_duration_min, cfg.alert_merge_gap_min, cfg.medium,
)
alerts_high, alerts_sensor_high = build_alert_episodes(
    df.index, risk_df["risk_score"], cls, combined, catalog,
    cfg.alert_min_duration_min, cfg.alert_merge_gap_min, cfg.high,
)
alerts_med["threshold"] = "MEDIUM"
alerts_high["threshold"] = "HIGH"
alerts_sensor_med["threshold"] = "MEDIUM"
alerts_sensor_high["threshold"] = "HIGH"
alerts = pd.concat([alerts_med, alerts_high], ignore_index=True)
alerts_sensor = pd.concat([alerts_sensor_med, alerts_sensor_high], ignore_index=True)

print(f"   MEDIUM: {len(alerts_med)}, HIGH: {len(alerts_high)}, Total: {len(alerts)}")
print(f"   Sensor-level alert rows: {len(alerts_sensor)}")
if len(alerts) > 0:
    print(alerts.head(20).to_string(index=False))
print("✅ Alerts complete.")

# %%
# =============================================================================
# Cell 15: Assemble + Save
# =============================================================================
scores = pd.DataFrame(index=df.index)
scores.index.name = "timestamp_utc"
scores["mode"] = mode
scores["class"] = cls

for sys_label, score_s in system_scores.items():
    scores[f"score_{sys_label}"] = score_s
    if sys_label in ae_results:
        scores[f"ae_score_{sys_label}"] = ae_results[sys_label]["system_score"]
        scores[f"ae_alarm_{sys_label}"] = ae_results[sys_label]["alarm"].astype(int)

scores["subsystem_score"] = subsystem_score

scores = pd.concat([scores, sqs_summary, risk_df], axis=1)

output_dir = cfg.output_dir
os.makedirs(output_dir, exist_ok=True)

scores.to_parquet(os.path.join(output_dir, "scores.parquet"))
scores.to_csv(os.path.join(output_dir, "scores.csv"))

if not risk_sensor_decomposition.empty:
    risk_sensor_decomposition.to_parquet(
        os.path.join(output_dir, "risk_sensor_decomposition.parquet"), index=False
    )
    risk_sensor_decomposition.to_csv(
        os.path.join(output_dir, "risk_sensor_decomposition.csv"), index=False
    )

if not alerts.empty:
    alerts.to_parquet(os.path.join(output_dir, "alerts.parquet"), index=False)
    alerts.to_csv(os.path.join(output_dir, "alerts.csv"), index=False)
    try:
        alerts.to_excel(os.path.join(output_dir, "alerts.xlsx"), index=False)
    except Exception as exc:
        print(f"⚠️  Could not write alerts.xlsx ({exc})")

if not alerts_sensor.empty:
    alerts_sensor.to_parquet(os.path.join(output_dir, "alerts_sensor_level.parquet"), index=False)
    alerts_sensor.to_csv(os.path.join(output_dir, "alerts_sensor_level.csv"), index=False)

sensor_cfg_out = sensor_cfg.copy()
sensor_cfg_out["config_start_ts"] = df.index.min()
sensor_cfg_out["config_end_ts"] = df.index.max()
sensor_cfg_out.to_parquet(os.path.join(output_dir, "sensor_config.parquet"), index=False)
sensor_cfg_out.to_csv(os.path.join(output_dir, "sensor_config.csv"), index=False)

weights_df = pd.DataFrame(
    [{"type": "fusion", "key": k, "weight": v} for k, v in dynamic_weights.items()]
    + [{"type": "risk", "key": k, "weight": v} for k, v in dynamic_risk_weights.items()]
)
weights_df.to_csv(os.path.join(output_dir, "dynamic_weights.csv"), index=False)

# Save AE model metadata
ae_meta_rows = []
for sys_label, ae_info in ae_models.items():
    ae_meta_rows.append({
        "subsystem": sys_label,
        "n_sensors": len(ae_info["sensors"]),
        "input_dim": ae_info["input_dim"],
        "latent_dim": ae_info["latent_dim"],
        "final_loss": ae_info["history"][-1] if ae_info["history"] else np.nan,
        "risk_train_sigma": ae_info["risk_train_sigma"],
        "sensors_used": "|".join(ae_info["sensors"]),
    })
if ae_meta_rows:
    ae_meta_df = pd.DataFrame(ae_meta_rows)
    ae_meta_df.to_csv(os.path.join(output_dir, "ae_model_metadata.csv"), index=False)
    print(f"✅ AE metadata: {len(ae_meta_df)} models")

print(f"\n✅ Scores: {scores.shape}")
print(f"✅ Detailed output: {detailed_output.shape}")
print(f"✅ Alerts: {len(alerts)}")
print(f"✅ Sensor-level alert rows: {len(alerts_sensor)}")
print(f"✅ Risk decomposition rows: {len(risk_sensor_decomposition)}")
print(f"✅ All outputs saved to: {output_dir}")

print(f"\n{'='*70}")
print("PIPELINE SUMMARY")
print(f"{'='*70}")
for label, sensors in catalog.items():
    w_f = dynamic_weights.get(label, 0.0)
    w_r = dynamic_risk_weights.get(label, 0.0)
    has_ae = "✓" if label in ae_models else "✗"
    n_in_scores = 1 if label in system_scores else 0
    print(f"  {label:20s}: {len(sensors):3d} sensors | AE:{has_ae}"
          f" | fusion_w={w_f:.3f} | risk_w={w_r:.3f}"
          f" | scored:{'✓' if n_in_scores else '✗'}")
print(f"\n  Clustering threshold: {threshold_used:.4f}")
print(f"  Running minutes: {running.sum():,}")
print(f"  Downtime minutes: {downtime.sum():,}")
print(f"  AE subsystems: {len(ae_models)}")
print(f"  Scored subsystems (incl ISOLATED): {len(system_scores)}")
print(f"  Risk alerts (MEDIUM+): {len(alerts)}")
print("✅ Pipeline complete.")

# %%


# %%


# %%



