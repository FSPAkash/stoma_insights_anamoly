# %%
# %% [code]
# =============================================================================
# Cell 1: Imports and Constants
# =============================================================================
# Industrial IoT Sensor Health & Anomaly Detection Pipeline
# Processes shredder sensor data to detect mechanical, electrical,
# and thermal anomalies using drift detection, periodicity analysis,
# PCA multivariate methods, and physics-based validation.
# =============================================================================

from __future__ import annotations

import os
import warnings
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import HuberRegressor

warnings.filterwarnings("ignore")

EPS = 1e-9

print("âœ… Imports complete.")

# %%
# %% [code]
# =============================================================================
# Cell 2: Pipeline Configuration
# =============================================================================
# Central configuration for all pipeline parameters including
# thresholds, window sizes, weights, and file paths.
# =============================================================================

@dataclass
class PipelineConfig:
    # --- Source ---
    parquet_path: str = r"data_analysis_data_bq-results-iot-staging-shredder-sensor-2026_02_01-2026_02_07.parquet"
    long_ts_col: str = "START_T"
    long_sensor_col: str = "SENSOR_ID"
    long_value_col: str = "QUANTITY_VALUE_D"

    # --- Time grid ---
    freq: str = "1min"

    # --- Stable discovery ---
    n_stable_min: int = 300

    # --- Regression model ---
    reg_sqs_min: float = 0.80
    reg_min_train_points: int = 300

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

    # --- Multivariate PCA ---
    mv_sqs_min: float = 0.70
    mv_missing_max: float = 0.10
    mv_mean_abs_corr_min: float = 0.05
    mv_explained_var: float = 0.95
    mv_score_q: float = 0.99

    # --- Block scoring ---
    block_score_topk: int = 3

    # --- Block fusion weights (severity) ---
    w_mech: float = 0.35
    w_elec: float = 0.35
    w_therm: float = 0.15
    w_physics: float = 0.15

    # --- Risk weights ---
    rw_mech: float = 0.35
    rw_elec: float = 0.35
    rw_therm: float = 0.15
    rw_instrument: float = 0.15

    # --- Alert thresholds ---
    high: float = 0.80
    medium: float = 0.55

    # --- Episode controls ---
    alert_min_duration_min: int = 5
    alert_merge_gap_min: int = 3

    # --- Physics ---
    physics_exclude_downtime: bool = True

    # --- Audit ---
    audit_xlsx_path: str = "data/pipeline_audit.xlsx"


cfg = PipelineConfig()
print("âœ… Configuration loaded.")
print(f"   Parquet path: {cfg.parquet_path}")
print(f"   n_stable_min = {cfg.n_stable_min}")

# %%
# %% [code]
# =============================================================================
# Cell 3: Sensor Catalog
# =============================================================================
# Defines which sensors belong to mechanical, thermal, and electrical
# subsystems for block-level scoring and fault classification.
# =============================================================================

def build_sensor_catalog() -> Dict[str, List[str]]:
    mech_vibration = [
        "VT5449011H", "VT5449012H", "VT5449012A", "VT5449013H",
        "VT5449016H", "VT5449017H", "VT5449017A", "VT5449018H",
    ]
    therm_temp = [
        "TT5449012H", "TT5449017H", "TT5449018H",
        "DESF_TA__RTD_1", "DESF_TA__RTD_2", "DESF_TA__RTD_3",
        "DESF_TA__RTD_4", "DESF_TA__RTD_5", "DESF_TA__RTD_6",
        "DESF_TA__RTD_7", "DESF_TA__RTD_8",
    ]
    elec_core = [
        "DESF_TA__J1_IA", "DESF_TA__J1_IB", "DESF_TA__J1_IC",
        "DESF_TA__J1_I_AVG",
        "DESF_TA__J2_VAN", "DESF_TA__J2_VBN", "DESF_TA__J2_VCN",
        "DESF_TA__J2_V_AVG_LN",
        "DESF_TA__J2_VAB", "DESF_TA__J2_VBC", "DESF_TA__J2_VCA",
        "DESF_TA__J2_V_AVG_LL",
        "DESF_TA__FREC",
        "DESF_TA__KW_TOT", "DESF_TA__KVAR_TOT", "DESF_TA__KVA_TOT",
        "DESF_TA__PF", "DESF_TA__MWH_DEL",
        "DESF_TA__KW_A", "DESF_TA__KW_B", "DESF_TA__KW_C",
        "DESF_TA__KVAR_A", "DESF_TA__KVAR_B", "DESF_TA__KVAR_C",
        "DESF_TA__KVA_A", "DESF_TA__KVA_B", "DESF_TA__KVA_C",
        "DESF_TA__PF_A", "DESF_TA__PF_B", "DESF_TA__PF_C",
    ]
    return {"MECH_VIB": mech_vibration, "THERM_TEMP": therm_temp, "ELEC": elec_core}


# Core column constants
COL_KW   = "DESF_TA__KW_TOT"
COL_PF   = "DESF_TA__PF"
COL_VLL  = "DESF_TA__J2_V_AVG_LL"
COL_IAVG = "DESF_TA__J1_I_AVG"
COL_FREC = "DESF_TA__FREC"

DER_KW_PRED = "DERIVED.kw_expected"
DER_KW_RES  = "DERIVED.kw_residual"

catalog = build_sensor_catalog()
all_catalog_sensors = catalog["MECH_VIB"] + catalog["THERM_TEMP"] + catalog["ELEC"]

print(f"âœ… Sensor catalog: {len(all_catalog_sensors)} sensors across 3 blocks")
print(f"   MECH_VIB:    {len(catalog['MECH_VIB'])} sensors")
print(f"   THERM_TEMP:  {len(catalog['THERM_TEMP'])} sensors")
print(f"   ELEC:        {len(catalog['ELEC'])} sensors")

# %%
# %% [code]
# =============================================================================
# Cell 4: Read Parquet + Pivot + Resample
# =============================================================================
# Reads raw long-format parquet data, pivots to wide format, and
# resamples to a 1-minute grid. Zeros are excluded from aggregation
# so only real non-zero readings contribute to the minute mean.
# =============================================================================

def read_parquet_long_to_wide(cfg: PipelineConfig) -> pd.DataFrame:
    print(f"ðŸ“‚ Reading parquet: {cfg.parquet_path}")
    raw = pd.read_parquet(cfg.parquet_path)
    print(f"   Raw rows: {len(raw):,}")
    print(f"   Unique sensors: {raw[cfg.long_sensor_col].nunique()}")
    print(f"   Time range: {raw[cfg.long_ts_col].min()} â†’ {raw[cfg.long_ts_col].max()}")

    raw[cfg.long_ts_col] = pd.to_datetime(raw[cfg.long_ts_col], utc=True)
    raw = raw.dropna(subset=[cfg.long_ts_col, cfg.long_sensor_col])
    raw[cfg.long_value_col] = pd.to_numeric(raw[cfg.long_value_col], errors="coerce")

    wide = raw.pivot_table(
        index=cfg.long_ts_col,
        columns=cfg.long_sensor_col,
        values=cfg.long_value_col,
        aggfunc="mean"
    ).sort_index()
    wide.index.name = None

    print(f"   Wide shape after pivot: {wide.shape}")
    return wide


def detect_frequency(df: pd.DataFrame) -> pd.Timedelta:
    """Filter out zero-second diffs before computing mode."""
    diffs = pd.Series(df.index).diff().dropna()
    diffs = diffs[diffs > pd.Timedelta(0)]

    if diffs.empty:
        print("   âš ï¸ All diffs are zero; defaulting to 1min")
        return pd.Timedelta("1min")

    diffs_rounded = diffs.dt.round("1s")
    diffs_rounded = diffs_rounded[diffs_rounded > pd.Timedelta(0)]

    if diffs_rounded.empty:
        print("   âš ï¸ No positive diffs after rounding; defaulting to 1min")
        return pd.Timedelta("1min")

    mode_freq = diffs_rounded.mode()
    detected = mode_freq.iloc[0] if len(mode_freq) > 0 else pd.Timedelta("1min")

    print(f"   Time diff distribution (top 5):")
    top5 = diffs_rounded.value_counts().head(5)
    for td, cnt in top5.items():
        print(f"     {td}: {cnt:,} occurrences ({cnt / len(diffs_rounded):.1%})")

    print(f"   Detected native frequency: {detected}")
    return detected


def clean_resample_mean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Resample to 1-min grid.
    Replace zeros with NaN before aggregation so they don't affect mean.
    Only real non-zero readings contribute to the minute mean.
    """
    df_clean = df.replace(0.0, np.nan)

    n_zeros = (df == 0.0).sum().sum()
    print(f"   Zeros excluded from aggregation: {int(n_zeros):,}")

    df_resampled = df_clean.resample("1min").mean()

    n_nan_after = df_resampled.isna().sum().sum()
    total_cells = df_resampled.shape[0] * df_resampled.shape[1]
    print(f"   After resample: {int(n_nan_after):,} NaN cells out of {int(total_cells):,} "
          f"({n_nan_after/total_cells:.1%})")

    return df_resampled


def resample_to_minute_grid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect native frequency, resample to 1-min grid with zeros/NaN
    excluded from mean computation, and fill to complete time grid.
    """
    native_freq = detect_frequency(df)
    target = pd.Timedelta("1min")

    if native_freq < target:
        print(f"   â†“ Downsampling from ~{native_freq} to 1min (zeros/NaN excluded from mean)")
        df_resampled = clean_resample_mean(df)
    elif native_freq > target:
        print(f"   â†‘ Upsampling from ~{native_freq} to 1min (ffill limit=1)")
        df_resampled = df.resample("1min").ffill(limit=1)
    else:
        print(f"   â‰¡ Data is ~1min; aggregating with zeros/NaN excluded")
        df_resampled = clean_resample_mean(df)

    full_idx = pd.date_range(
        df_resampled.index.min(), df_resampled.index.max(),
        freq="1min", tz=df_resampled.index.tz
    )
    df_resampled = df_resampled.reindex(full_idx)

    print(f"   Final grid: {df_resampled.shape[0]:,} minutes "
          f"({df_resampled.index.min()} â†’ {df_resampled.index.max()})")
    return df_resampled


# =================== EXECUTE ===================

raw_wide = read_parquet_long_to_wide(cfg)
df = resample_to_minute_grid(raw_wide)

# Verify core columns
missing_core = [c for c in [COL_KW, COL_PF, COL_VLL, COL_IAVG, COL_FREC] if c not in df.columns]
if missing_core:
    raise RuntimeError(f"âŒ Missing required core columns: {missing_core}")

print(f"\nâœ… All core columns present.")
print(f"âœ… Minute-grid data: {df.shape[0]:,} rows Ã— {df.shape[1]} columns")
print(f"\n   NaN fraction (core columns):")
for c in [COL_KW, COL_PF, COL_VLL, COL_IAVG, COL_FREC]:
    print(f"     {c}: {df[c].isna().mean():.2%}")

# %%
# %% [code]
# =============================================================================
# Cell 5: Scale Inference
# =============================================================================
# Infers the correct scale factors for power factor (PF) and
# line-to-line voltage (VLL) to make the kW identity equation work.
# =============================================================================

def infer_pf_scale(pf_raw: pd.Series) -> float:
    med = np.nanmedian(pf_raw.dropna().values)
    scale = 0.1 if np.isfinite(med) and med > 1.2 else 1.0
    print(f"   PF: median={med:.3f}, scale={scale}")
    return scale


def infer_vll_scale_for_identity(
    kw: pd.Series, vll: pd.Series, iavg: pd.Series, pf_unit: pd.Series
) -> int:
    """Use minutes where all values are present and non-NaN for scale inference."""
    valid = kw.notna() & vll.notna() & iavg.notna() & pf_unit.notna()
    running = valid & (kw > 10) & (iavg > 1)

    if running.sum() < 100:
        print("   âš ï¸ Not enough valid minutes for VLL scale inference; defaulting to 1")
        return 1

    kw_r = kw[running]
    vll_r = vll[running]
    iavg_r = iavg[running]
    pf_r = pf_unit[running]

    candidates = [1, 10, 100, 1000, 10000]
    best_scale = 1
    best_med = float("inf")

    for s in candidates:
        kw_est = (np.sqrt(3) * (vll_r * s) * iavg_r * pf_r) / 1000.0
        resid = (kw_r - kw_est).abs() / (kw_r.abs() + EPS)
        med = np.nanmedian(resid.values)
        if np.isfinite(med):
            print(f"     VLL scale={s:>5d}: median residual = {med:.4f}")
            if med < best_med:
                best_med = med
                best_scale = s

    print(f"   â†’ Best VLL scale: {best_scale} (residual: {best_med:.4f})")
    return best_scale


pf_scale = infer_pf_scale(df[COL_PF])
pf_unit = df[COL_PF] * pf_scale
vll_scale = infer_vll_scale_for_identity(df[COL_KW], df[COL_VLL], df[COL_IAVG], pf_unit)

print(f"\nâœ… Scale inference: PF_scale={pf_scale}, VLL_scale={vll_scale}")

# %%
# %% [code]
# =============================================================================
# Cell 6: Base Invalid Mask + Bad Cell Nullification (No Sensor Dropping)
# =============================================================================
# Builds per-cell bad flags based on physical plausibility rules.
# No sensors are dropped â€” all sensors are kept in the dataframe.
# Bad-flagged cells are set to NaN so they don't corrupt calculations.
# =============================================================================

def base_invalid_mask(df: pd.DataFrame, pf_scale: float) -> pd.DataFrame:
    """Build per-cell bad flag. True = invalid."""
    bad = df.isna().copy()

    # Voltages must be > 0
    for c in [x for x in df.columns if "J2_V" in x]:
        bad[c] = bad[c] | (df[c] <= 0)

    # Frequency: 40â€“70 Hz
    for c in [x for x in df.columns if "FREC" in x]:
        f = df[c]
        bad[c] = bad[c] | (f < 40) | (f > 70)

    # PF plausibility
    max_raw = 10.5 if pf_scale == 0.1 else 1.05
    if COL_PF in df.columns:
        bad[COL_PF] = bad[COL_PF] | (df[COL_PF] <= 0) | (df[COL_PF] > max_raw)

    # Currents: negative is invalid
    for c in [x for x in df.columns if "_I" in x and "STATUS" not in x]:
        bad[c] = bad[c] | (df[c] <= 0)

    # Powers: negative is invalid
    for c in [x for x in df.columns if any(k in c for k in ["KW_", "KVA_", "KVAR_", "MWH"])]:
        bad[c] = bad[c] | (df[c] < 0)

    # Vibration: negative is invalid
    for c in [x for x in df.columns if "VT" in x]:
        bad[c] = bad[c] | (df[c] <= 0)

    # Temperature: unreasonable ranges
    for c in [x for x in df.columns if any(k in x for k in ["TT5449", "RTD_"])]:
        bad[c] = bad[c] | (df[c] < -10) | (df[c] > 200)

    return bad


def apply_bad_mask_nullify_cells(
    df: pd.DataFrame, bad: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    No sensors are dropped. Bad-flagged cells are set to NaN
    so they don't enter any downstream calculation.
    Returns: cleaned df, bad mask
    """
    bad_pct = bad.mean()

    print(f"\n   Bad % per sensor (showing sensors with >1% bad):")
    for s in bad_pct.sort_values(ascending=False).index:
        pct = bad_pct[s]
        if pct > 0.01:
            print(f"     {s}: {pct:.1%}")

    # For all sensors: set bad-flagged cells to NaN
    n_nullified = 0
    df_clean = df.copy()
    for c in df_clean.columns:
        if c in bad.columns:
            mask = bad[c] & df_clean[c].notna()
            n = mask.sum()
            if n > 0:
                df_clean.loc[mask, c] = np.nan
                n_nullified += n

    print(f"\n   Bad-flagged cells set to NaN: {n_nullified:,}")
    print(f"   All sensors retained: {len(df_clean.columns)}")

    return df_clean, bad


# =================== EXECUTE ===================

base_bad = base_invalid_mask(df, pf_scale)

# Show overall stats
invalid_pct = base_bad.mean().mean()
print(f"   Overall invalid fraction: {invalid_pct:.2%}")
for label, sensors in catalog.items():
    cols = [s for s in sensors if s in base_bad.columns]
    if cols:
        pct = base_bad[cols].mean().mean()
        print(f"     {label}: {pct:.2%} invalid")

# Apply: nullify bad cells, keep all sensors
df, base_bad = apply_bad_mask_nullify_cells(df, base_bad)

all_catalog_sensors = catalog["MECH_VIB"] + catalog["THERM_TEMP"] + catalog["ELEC"]

# Verify core columns still exist
missing_core = [c for c in [COL_KW, COL_PF, COL_VLL, COL_IAVG, COL_FREC] if c not in df.columns]
if missing_core:
    print(f"   âš ï¸ WARNING: Core columns missing: {missing_core}")

print(f"\nâœ… Base invalid mask applied. No sensors dropped.")
print(f"   Final df shape: {df.shape}")
print(f"   Catalog sensors: {len(all_catalog_sensors)}")

# %%
# %% [code]
# =============================================================================
# Cell 7: Downtime Detection (Electrical Signals Only)
# =============================================================================
# Detects periods when the machine is off using only electrical signals:
# kW, I_AVG, V_LL, FREC. No status column is used.
# Bad-flagged cells are already NaN so they naturally count as "off".
# =============================================================================

def build_downtime_mask(
    df: pd.DataFrame,
    col_kw: str, col_iavg: str, col_vll: str, col_frec: str
) -> pd.DataFrame:
    """Build per-signal off indicators using only electrical signals."""

    def qlow(s: pd.Series, q: float, floor: float) -> float:
        """Compute low quantile from valid non-NaN positive values."""
        pos = s[s > 0].dropna()
        if len(pos) > 100:
            v = float(np.nanquantile(pos.values, q))
            if np.isfinite(v):
                return max(v, floor)
        return floor

    kw_off_thr  = qlow(df[col_kw],   0.02, 0.5)  if col_kw   in df.columns else 0.5
    i_off_thr   = qlow(df[col_iavg], 0.02, 1.0)  if col_iavg in df.columns else 1.0
    vll_off_thr = qlow(df[col_vll],  0.02, 10.0) if col_vll  in df.columns else 10.0
    f_off_thr   = qlow(df[col_frec], 0.02, 1.0)  if col_frec in df.columns else 1.0

    print(f"   OFF thresholds: kWâ‰¤{kw_off_thr:.2f}, Iâ‰¤{i_off_thr:.2f}, "
          f"VLLâ‰¤{vll_off_thr:.2f}, Frecâ‰¤{f_off_thr:.2f}")

    result = pd.DataFrame(index=df.index)

    if col_kw in df.columns:
        result["kw_off"] = (df[col_kw] <= kw_off_thr) | df[col_kw].isna()
    else:
        result["kw_off"] = True

    if col_iavg in df.columns:
        result["i_off"] = (df[col_iavg] <= i_off_thr) | df[col_iavg].isna()
    else:
        result["i_off"] = True

    if col_vll in df.columns:
        result["vll_off"] = (df[col_vll] <= vll_off_thr) | df[col_vll].isna()
    else:
        result["vll_off"] = True

    if col_frec in df.columns:
        result["frec_off"] = (df[col_frec] <= f_off_thr) | df[col_frec].isna()
    else:
        result["frec_off"] = True

    return result


def global_downtime(off_mask: pd.DataFrame) -> pd.Series:
    """
    Downtime when all electrical signals off, OR
    kW AND current both off (strongest motor-off indicator).
    """
    all_off = off_mask.all(axis=1)
    power_current_off = off_mask["kw_off"] & off_mask["i_off"]
    return all_off | power_current_off


# =================== EXECUTE ===================

downtime_signals = build_downtime_mask(df, COL_KW, COL_IAVG, COL_VLL, COL_FREC)
downtime = global_downtime(downtime_signals)

# Running mask: everything that is not downtime
running = ~downtime

print(f"\n   Downtime minutes: {downtime.sum():,} / {len(downtime):,} "
      f"({downtime.mean():.1%})")
print(f"   Running minutes:  {running.sum():,} ({running.mean():.1%})")

for c in downtime_signals.columns:
    print(f"     {c}: {downtime_signals[c].sum():,} minutes off ({downtime_signals[c].mean():.1%})")

# Mode: only RUNNING or DOWNTIME (no steady/transient distinction)
mode = pd.Series("RUNNING", index=df.index)
mode.loc[downtime] = "DOWNTIME"

print(f"\n   Mode distribution:")
print(mode.value_counts().to_string())
print(f"âœ… Downtime detection complete.")

# %%
# %% [code]
# =============================================================================
# Cell 8: Learn Bounds + ROC
# =============================================================================
# Statistics are computed on running data only (downtime excluded).
# Bad-flagged cells are already NaN so .dropna() naturally excludes them.
# =============================================================================

def kw_identity_residual(
    df: pd.DataFrame,
    kw_col: str, vll_col: str, iavg_col: str, pf_col: str,
    pf_scale: float, vll_scale: int
) -> pd.Series:
    kw = df[kw_col]
    vll = df[vll_col]
    iavg = df[iavg_col]
    pf = df[pf_col] * pf_scale
    kw_est = (np.sqrt(3) * (vll * vll_scale) * iavg * pf) / 1000.0
    return (kw - kw_est).abs() / (kw.abs() + EPS)


def learn_bounds_roc(df: pd.DataFrame, running_mask: pd.Series, n_min: int) -> pd.DataFrame:
    """Learn bounds from running periods only."""
    running_df = df.loc[running_mask]
    rows = []
    for c in df.columns:
        x = running_df[c].dropna()
        if len(x) >= n_min:
            p01  = float(np.nanquantile(x.values, 0.001))
            p999 = float(np.nanquantile(x.values, 0.999))
            dx = x.diff().abs().dropna()
            roc999 = float(np.nanquantile(dx.values, 0.999)) if len(dx) >= n_min else np.nan
        else:
            p01, p999, roc999 = np.nan, np.nan, np.nan
        rows.append({
            "sensor": c, "p0_1": p01, "p99_9": p999, "roc_p99_9": roc999,
            "n_running": int(len(x)),
            "missing_pct": float(df[c].isna().mean()),
            "variance": float(df[c].var(skipna=True)),
        })
    return pd.DataFrame(rows)


sensor_cfg = learn_bounds_roc(df, running, n_min=cfg.n_stable_min)
sensor_cfg["pf_scale"] = pf_scale
sensor_cfg["vll_scale_for_identity"] = vll_scale

print(f"âœ… Sensor config: {len(sensor_cfg)} sensors")
print(f"   With sufficient running data (â‰¥{cfg.n_stable_min}): "
      f"{(sensor_cfg['n_running'] >= cfg.n_stable_min).sum()}")
print(sensor_cfg[["sensor", "p0_1", "p99_9", "n_running"]].head(10).to_string())

# %%
# %% [code]
# =============================================================================
# Cell 9: SQS (Signal Quality Score) Function
# =============================================================================
# Computes per-sensor per-timestamp quality scores based on:
#   - Data presence (start at 1.0 if non-NaN)
#   - Bounds violations (penalty)
#   - Rate-of-change jumps (penalty)
#   - kW identity contradictions (penalty)
#   - Regression contradictions (penalty)
# =============================================================================

def compute_sqs(
    df: pd.DataFrame, sensor_cfg: pd.DataFrame,
    pf_scale: float, vll_scale: int,
    kw_col: str, vll_col: str, iavg_col: str, pf_col: str,
    kw_pred: pd.Series,
):

    cfg_idx = sensor_cfg.set_index("sensor")

    sqs = pd.DataFrame(np.nan, index=df.index, columns=df.columns, dtype=float)

    step_records = []

    # (A) Start score: 1.0 where data is present
    for s in df.columns:
        mask = df[s].notna()
        sqs.loc[mask, s] = 1.0

    # (B) Bounds + ROC penalties
    for s in df.columns:
        if s not in cfg_idx.index:
            continue

        row = cfg_idx.loc[s]
        p0, p9, roc = row["p0_1"], row["p99_9"], row["roc_p99_9"]
        x = df[s]

        bounds_penalty = pd.Series(1.0, index=df.index)
        roc_penalty = pd.Series(1.0, index=df.index)

        if np.isfinite(p0) and np.isfinite(p9):
            margin = (p9 - p0) * 0.01
            lo, hi = p0 - margin, p9 + margin
            oob = (x < lo) | (x > hi)
            bounds_penalty[oob & x.notna()] = 0.6
            sqs.loc[oob & x.notna(), s] *= 0.6

        if np.isfinite(roc):
            dx = x.diff().abs()
            jump = dx > (roc * 1.25)
            roc_penalty[jump & x.notna()] = 0.7
            sqs.loc[jump & x.notna(), s] *= 0.7

        for ts in df.index:
            if pd.notna(df.loc[ts, s]):
                step_records.append({
                    "timestamp": ts,
                    "sensor": s,
                    "start_score": 1.0,
                    "bounds_penalty": bounds_penalty.loc[ts],
                    "roc_penalty": roc_penalty.loc[ts],
                    "identity_penalty": 1.0,
                    "regression_penalty": 1.0,
                })

    # (C) Identity contradiction penalty
    if all(c in df.columns for c in [kw_col, vll_col, iavg_col, pf_col]):
        rkw_id_vals = kw_identity_residual(
            df, kw_col, vll_col, iavg_col, pf_col, pf_scale, vll_scale
        )
        thr_id = float(np.nanquantile(rkw_id_vals.dropna().values, 0.99))
        bad_id = rkw_id_vals > thr_id

        for c in [kw_col, vll_col, iavg_col, pf_col]:
            if c in sqs.columns:
                sqs.loc[bad_id & df[c].notna(), c] *= 0.85
                for rec in step_records:
                    if rec["sensor"] == c and bad_id.loc[rec["timestamp"]]:
                        rec["identity_penalty"] = 0.85

    # (D) Regression contradiction penalty
    if kw_col in df.columns:
        kw = df[kw_col]
        r_reg = (kw - kw_pred).abs() / (kw.abs() + EPS)
        vals = r_reg.dropna().values

        if len(vals) > 100:
            thr_reg = float(np.nanquantile(vals, 0.995))
            bad_reg = r_reg > thr_reg
            sqs.loc[bad_reg & kw.notna(), kw_col] *= 0.80

            for rec in step_records:
                if rec["sensor"] == kw_col and bad_reg.loc[rec["timestamp"]]:
                    rec["regression_penalty"] = 0.80

    sqs = sqs.clip(0, 1)

    sqs_steps = pd.DataFrame(step_records)
    sqs_steps["final_score"] = (
        sqs_steps["start_score"]
        * sqs_steps["bounds_penalty"]
        * sqs_steps["roc_penalty"]
        * sqs_steps["identity_penalty"]
        * sqs_steps["regression_penalty"]
    )

    return sqs, sqs_steps


def summarize_sqs(sqs: pd.DataFrame) -> pd.DataFrame:
    """Compute p10 only over present sensors (NaN columns skipped per row)."""
    arr = sqs.values

    sqs_mean = np.full(arr.shape[0], np.nan)
    sqs_p10 = np.full(arr.shape[0], np.nan)
    sqs_valid_frac = np.full(arr.shape[0], np.nan)

    for i in range(arr.shape[0]):
        row = arr[i, :]
        present = row[np.isfinite(row)]

        if len(present) > 0:
            sqs_mean[i] = np.mean(present)
            sqs_p10[i] = np.quantile(present, 0.10)
            sqs_valid_frac[i] = np.mean(present > 0)
        else:
            sqs_mean[i] = 0.0
            sqs_p10[i] = 0.0
            sqs_valid_frac[i] = 0.0

    return pd.DataFrame({
        "sqs_mean": sqs_mean, "sqs_p10": sqs_p10,
        "sqs_valid_frac": sqs_valid_frac
    }, index=sqs.index)


print("âœ… SQS functions defined.")

# %%
# %% [code]
# =============================================================================
# Cell 10: Regression â†’ Derived Signals â†’ Final SQS
# =============================================================================
# Trains a Huber regression model to predict kW from electrical features.
# Uses running periods with good SQS for training data.
# Produces derived signals (kw_expected, kw_residual) and final SQS.
# =============================================================================

def identity_expected_kw(
    df: pd.DataFrame, vll_col: str, iavg_col: str, pf_col: str,
    pf_scale: float, vll_scale: int
) -> pd.Series:
    if not all(c in df.columns for c in [vll_col, iavg_col, pf_col]):
        return pd.Series(np.nan, index=df.index)
    pf = df[pf_col] * pf_scale
    kw_est = (np.sqrt(3) * (df[vll_col] * vll_scale) * df[iavg_col] * pf) / 1000.0
    need = df[[vll_col, iavg_col, pf_col]].notna().all(axis=1)
    return kw_est.where(need)


def build_kw_features(
    df: pd.DataFrame,
    vll_col: str, iavg_col: str, pf_col: str, frec_col: str,
    pf_scale: float, vll_scale: int
) -> pd.DataFrame:
    vll  = df[vll_col] * vll_scale if vll_col in df.columns else pd.Series(np.nan, index=df.index)
    iavg = df[iavg_col] if iavg_col in df.columns else pd.Series(np.nan, index=df.index)
    pf   = df[pf_col] * pf_scale if pf_col in df.columns else pd.Series(np.nan, index=df.index)
    frec = df[frec_col] if frec_col in df.columns else pd.Series(np.nan, index=df.index)
    return pd.DataFrame({
        "vll": vll, "iavg": iavg, "pf": pf, "frec": frec,
        "vll_i": vll * iavg, "i_pf": iavg * pf, "vll_pf": vll * pf,
    }, index=df.index)


def train_kw_regression(
    df: pd.DataFrame, running_mask: pd.Series,
    sqs_prov: pd.DataFrame, kw_col: str,
    X: pd.DataFrame, sqs_min: float, min_points: int
) -> Tuple[Optional[HuberRegressor], pd.Series]:
    """Train regression on running periods with good quality data."""

    if kw_col not in df.columns:
        return None, pd.Series(np.nan, index=df.index)

    kw = df[kw_col]

    ok = (
        running_mask &
        kw.notna() &
        X.notna().all(axis=1)
    )
    if kw_col in sqs_prov.columns:
        ok = ok & (sqs_prov[kw_col] >= sqs_min)

    print(f"   Regression training points: {ok.sum():,}")

    if ok.sum() < min_points:
        print(f"   âš ï¸ Not enough ({ok.sum()} < {min_points}); identity fallback.")
        return None, pd.Series(np.nan, index=df.index)

    model = HuberRegressor()
    model.fit(X.loc[ok].values, kw.loc[ok].values)

    kw_pred = pd.Series(np.nan, index=df.index)
    okp = X.notna().all(axis=1)
    kw_pred.loc[okp] = model.predict(X.loc[okp].values)

    train_resid = kw.loc[ok] - model.predict(X.loc[ok].values)
    r2 = 1 - (train_resid.var() / kw.loc[ok].var())
    mae = train_resid.abs().mean()
    print(f"   Regression RÂ²: {r2:.4f}, MAE: {mae:.2f}")

    return model, kw_pred


# === EXECUTE ===

# Pass 1: Provisional SQS using identity estimate
print("--- Pass 1: Provisional SQS ---")
dummy_pred = identity_expected_kw(df, COL_VLL, COL_IAVG, COL_PF, pf_scale, vll_scale)
dummy_pred = dummy_pred.ffill()

sqs0, sqs_steps = compute_sqs(
    df, sensor_cfg, pf_scale, vll_scale,
    COL_KW, COL_VLL, COL_IAVG, COL_PF, dummy_pred
)

# Train regression on running periods
print("\n--- Regression Training ---")
X_feat = build_kw_features(df, COL_VLL, COL_IAVG, COL_PF, COL_FREC, pf_scale, vll_scale)
model_kw, kw_pred = train_kw_regression(
    df, running, sqs0, COL_KW, X_feat,
    cfg.reg_sqs_min, cfg.reg_min_train_points
)

if model_kw is None or kw_pred.isna().all():
    kw_pred = identity_expected_kw(df, COL_VLL, COL_IAVG, COL_PF, pf_scale, vll_scale)
    print("   Using identity fallback.")

kw_residual = df[COL_KW] - kw_pred if COL_KW in df.columns else pd.Series(np.nan, index=df.index)

# Add derived columns
df[DER_KW_PRED] = kw_pred
df[DER_KW_RES]  = kw_residual

# Learn bounds for derived signals
add_cfg = learn_bounds_roc(df[[DER_KW_PRED, DER_KW_RES]], running, n_min=cfg.n_stable_min)
add_cfg["pf_scale"] = pf_scale
add_cfg["vll_scale_for_identity"] = vll_scale
sensor_cfg = pd.concat([sensor_cfg, add_cfg], ignore_index=True)

# Pass 2: Final SQS with regression
print("\n--- Pass 2: Final SQS ---")
sqs, sqs_steps2 = compute_sqs(
    df, sensor_cfg, pf_scale, vll_scale,
    COL_KW, COL_VLL, COL_IAVG, COL_PF, df[DER_KW_PRED]
)
sqs_summary = summarize_sqs(sqs)

print(f"   SQS mean: {sqs_summary['sqs_mean'].mean():.3f}")
print(f"   SQS p10:  {sqs_summary['sqs_p10'].mean():.3f}")
print(f"   SQS valid frac: {sqs_summary['sqs_valid_frac'].mean():.3f}")
print(f"\nâœ… Regression + SQS complete.")
if COL_KW in df.columns:
    print(f"   kw_residual: mean={kw_residual.mean():.2f}, std={kw_residual.std():.2f}")

# %%
# %% [code]
# =============================================================================
# Cell 11: Engine A â€“ Drift Detection
# =============================================================================
# Detects slow drift anomalies using rolling baselines and MAD scaling.
# Downtime periods are excluded from scoring.
# =============================================================================

def rolling_median(s: pd.Series, win: int) -> pd.Series:
    return s.rolling(win, min_periods=max(30, win // 5)).median()


def rolling_mad(resid: pd.Series, win: int) -> pd.Series:
    def _mad(a):
        a = a[np.isfinite(a)]
        if len(a) < 50:
            return np.nan
        med = np.median(a)
        return 1.4826 * np.median(np.abs(a - med))
    return resid.rolling(win, min_periods=max(50, win // 5)).apply(_mad, raw=True)


def select_engineA_sensors(sensor_cfg: pd.DataFrame, n_min: int, miss_max: float) -> List[str]:
    return sensor_cfg[
        (sensor_cfg["n_running"] >= n_min) &
        (sensor_cfg["missing_pct"] <= miss_max) &
        (sensor_cfg["variance"] > 0)
    ]["sensor"].tolist()


def compute_engineA(
    df: pd.DataFrame, sqs: pd.DataFrame, running_mask: pd.Series,
    sensors: List[str],
    baseline_win: int, mad_win: int, sqs_min: float, k: float
) -> Tuple[pd.DataFrame, Dict[str, Tuple[pd.Series, pd.Series]]]:
    """Compute drift scores. Downtime periods are excluded."""

    A = pd.DataFrame(index=df.index, columns=sensors, dtype=float)
    cache: Dict[str, Tuple[pd.Series, pd.Series]] = {}

    for s in sensors:
        if s not in df.columns:
            continue
        x = df[s]
        valid = running_mask & (sqs[s] >= sqs_min) & x.notna()
        x_use = x.where(valid)
        base = rolling_median(x_use, baseline_win)
        resid = x_use - base
        mad = rolling_mad(resid, mad_win)
        z = resid.abs() / (mad + EPS)
        score = 1.0 - np.exp(-z / max(k, EPS))
        A[s] = score
        cache[s] = (base, mad)

    return A, cache


print("--- Engine A ---")
A_sensors = select_engineA_sensors(sensor_cfg, cfg.n_stable_min, cfg.engineA_missing_max)
if DER_KW_RES not in A_sensors and DER_KW_RES in df.columns:
    A_sensors.append(DER_KW_RES)
print(f"   Eligible sensors: {len(A_sensors)}")

A, baseline_cache = compute_engineA(
    df, sqs, running, A_sensors,
    cfg.engineA_baseline_win, cfg.engineA_mad_win,
    cfg.engineA_sqs_min, cfg.engineA_score_k
)
print(f"   Engine A shape: {A.shape}, mean score: {A.mean().mean():.4f}")
print(f"âœ… Engine A complete.")

# %%
# %% [code]
# =============================================================================
# Cell 12: Engine B â€“ Periodicity Detection
# =============================================================================
# Detects periodic anomalies using spectral energy ratio analysis.
# Downtime periods are excluded from scoring.
# =============================================================================

def spectral_energy_ratio(u_window: np.ndarray, period_min: int, period_max: int) -> float:
    u = np.asarray(u_window, dtype=float)
    u = u[np.isfinite(u)]
    if len(u) < 60:
        return np.nan
    u = u - np.mean(u)
    w = np.hanning(len(u))
    fft = np.fft.rfft(u * w)
    P = np.abs(fft) ** 2
    freqs = np.fft.rfftfreq(len(u), d=1.0)
    band = (freqs >= 1.0 / period_max) & (freqs <= 1.0 / period_min)
    return float(np.nansum(P[band]) / (np.nansum(P) + EPS))


def learn_engineB_thresholds(
    u_running: pd.Series, win: int, period_min: int, period_max: int
) -> Tuple[float, float]:
    """Learn thresholds from running data."""
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
    e_low  = float(np.nanquantile(ratios, 0.90))
    e_high = float(np.nanquantile(ratios, 0.99))
    if e_high - e_low < 0.05:
        e_low  = max(0.05, e_low - 0.05)
        e_high = min(0.95, e_high + 0.05)
    return e_low, e_high


def select_engineB_sensors(
    df: pd.DataFrame, sensor_cfg: pd.DataFrame,
    running_mask: pd.Series,
    sqs: pd.DataFrame, sqs_min: float
) -> List[str]:
    cfg_idx = sensor_cfg.set_index("sensor")
    eligible = []

    for s in df.columns:
        if s not in cfg_idx.index:
            continue
        if cfg_idx.loc[s, "n_running"] < 200:
            continue
        if cfg_idx.loc[s, "variance"] <= 0:
            continue
        if s not in sqs.columns:
            continue
        ok = running_mask & df[s].notna() & (sqs[s] >= sqs_min)
        if ok.sum() < 200:
            continue
        eligible.append(s)

    return eligible[:30]


def compute_engineB(
    df: pd.DataFrame, sqs: pd.DataFrame, running_mask: pd.Series,
    sensors: List[str],
    baseline_cache: Dict[str, Tuple[pd.Series, pd.Series]],
    win: int, sqs_min: float, valid_frac_min: float,
    period_min: int, period_max: int
) -> pd.DataFrame:
    """Compute periodicity scores. Downtime periods are excluded."""

    B = pd.DataFrame(index=df.index, columns=sensors, dtype=float)

    thresholds: Dict[str, Tuple[float, float]] = {}
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


print("--- Engine B ---")
B_sensors = select_engineB_sensors(df, sensor_cfg, running, sqs, cfg.engineB_sqs_min)
if DER_KW_RES not in B_sensors and DER_KW_RES in df.columns:
    B_sensors.append(DER_KW_RES)
print(f"   Eligible sensors: {len(B_sensors)}")

B = compute_engineB(
    df, sqs, running, B_sensors, baseline_cache,
    cfg.engineB_win, cfg.engineB_sqs_min, cfg.engineB_valid_frac_min,
    cfg.engineB_period_min, cfg.engineB_period_max
)
print(f"   Engine B shape: {B.shape}, non-null: {B.notna().sum().sum():,}")
print(f"âœ… Engine B complete.")

# %%
# %% [code]
# =============================================================================
# Cell 13: Physics Validation
# =============================================================================
# Cross-checks electrical signals using physical equations:
#   kW = âˆš3 Ã— V_LL Ã— I Ã— PF / 1000
#   PF = kW / kVA
#   kVARÂ² = kVAÂ² - kWÂ²
#   Current unbalance across phases
# Downtime periods are forced to score 0.
# =============================================================================

def physics_scores(
    df: pd.DataFrame, pf_scale: float, vll_scale: int,
    cols: Dict[str, str], downtime: pd.Series
) -> pd.DataFrame:

    out = pd.DataFrame(index=df.index)
    running_mask = ~downtime

    need = ["kw_tot", "pf_tot", "vll_avg", "iavg"]
    if not all(k in cols and cols[k] in df.columns for k in need):
        out["physics_score"] = np.nan
        return out

    kw   = df[cols["kw_tot"]]
    pf   = df[cols["pf_tot"]] * pf_scale
    vll  = df[cols["vll_avg"]] * vll_scale
    iavg = df[cols["iavg"]]

    # Only compute during running periods
    kw_r   = kw.where(running_mask)
    pf_r   = pf.where(running_mask)
    vll_r  = vll.where(running_mask)
    iavg_r = iavg.where(running_mask)

    # kW identity mismatch
    kw_est = (np.sqrt(3) * vll_r * iavg_r * pf_r) / 1000.0
    r_kw = (kw_r - kw_est).abs() / (kw_r.abs() + EPS)
    out["r_kw_identity"] = r_kw

    # PF mismatch
    if "kva_tot" in cols and cols["kva_tot"] in df.columns:
        kva = df[cols["kva_tot"]].where(running_mask)
        pf_est = kw_r / (kva + EPS)
        out["r_pf_identity"] = (pf_est - pf_r).abs()
    else:
        out["r_pf_identity"] = np.nan

    # kVAR mismatch
    if all(k in cols and cols[k] in df.columns for k in ["kva_tot", "kvar_tot"]):
        kva  = df[cols["kva_tot"]].where(running_mask)
        kvar = df[cols["kvar_tot"]].where(running_mask)
        kvar_est = np.sqrt(np.maximum(kva ** 2 - kw_r ** 2, 0))
        out["r_kvar_identity"] = (kvar - kvar_est).abs() / (kvar.abs() + EPS)
    else:
        out["r_kvar_identity"] = np.nan

    # Current unbalance
    if all(k in cols and cols[k] in df.columns for k in ["ia", "ib", "ic"]):
        ia = df[cols["ia"]].where(running_mask)
        ib = df[cols["ib"]].where(running_mask)
        ic = df[cols["ic"]].where(running_mask)
        out["unbalance_I"] = (
            np.maximum.reduce([ia, ib, ic]) - np.minimum.reduce([ia, ib, ic])
        ) / (iavg_r.abs() + EPS)
    else:
        out["unbalance_I"] = np.nan

    # Normalize and combine
    parts = []
    for c in ["r_kw_identity", "r_pf_identity", "r_kvar_identity", "unbalance_I"]:
        s = out[c]
        vals = s.dropna().values
        if len(vals) > 100:
            q = float(np.nanquantile(vals, 0.99))
            if np.isfinite(q) and q > EPS:
                parts.append(np.clip(s / (q + EPS), 0, 1))

    if parts:
        out["physics_score"] = np.maximum.reduce(parts)
    else:
        out["physics_score"] = np.nan

    # Force 0 during downtime
    out.loc[downtime, "physics_score"] = 0.0

    return out


cols_phys = {
    "kw_tot": COL_KW, "pf_tot": COL_PF, "vll_avg": COL_VLL, "iavg": COL_IAVG,
    "kva_tot": "DESF_TA__KVA_TOT", "kvar_tot": "DESF_TA__KVAR_TOT",
    "ia": "DESF_TA__J1_IA", "ib": "DESF_TA__J1_IB", "ic": "DESF_TA__J1_IC",
}
cols_phys = {k: v for k, v in cols_phys.items() if v in df.columns}

phys_df = physics_scores(df, pf_scale, vll_scale, cols_phys, downtime)
phys_score = phys_df.get("physics_score", pd.Series(np.nan, index=df.index))

running_phys = phys_score[running]
print(f"   Physics score (running only): mean={running_phys.mean():.4f}")
print(f"   Physics score >0.5 (running): {(running_phys > 0.5).sum():,}")
print(f"   Physics score during downtime: forced to 0")
print(f"âœ… Physics validation complete.")

# %%
# %% [code]
# =============================================================================
# Cell 14: PCA Multivariate Anomaly Detection
# =============================================================================
# Trains PCA models per subsystem (mechanical, electrical, thermal)
# on running data. Scores all timestamps using SPE and TÂ² statistics.
# =============================================================================

def mean_abs_corr(running_df: pd.DataFrame) -> pd.Series:
    c = running_df.corr().abs()
    return c.where(~np.eye(c.shape[0], dtype=bool)).mean()


def select_mv_candidates(
    df: pd.DataFrame, running_mask: pd.Series, sensor_cfg: pd.DataFrame,
    sensors: List[str], mv_missing_max: float, mv_mean_abs_corr_min: float
) -> List[str]:
    cfg_idx = sensor_cfg.set_index("sensor")
    base = [
        s for s in sensors
        if s in df.columns and s in cfg_idx.index
        and cfg_idx.loc[s, "missing_pct"] <= mv_missing_max
        and cfg_idx.loc[s, "n_running"] >= 100
    ]
    if len(base) < 3:
        return []
    running_df = df.loc[running_mask, base].dropna(how="all")
    if running_df.shape[0] < 200:
        return []
    good = [
        s for s in base
        if running_df[s].isna().mean() <= mv_missing_max
        and running_df[s].var(skipna=True) > 0
    ]
    if len(good) < 3:
        return []
    mac = mean_abs_corr(running_df[good].ffill().bfill())
    return [s for s in good if float(mac.get(s, 0.0)) >= mv_mean_abs_corr_min]


def train_pca(running_df, explained_var, score_q):
    X = running_df.ffill().bfill().values
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)
    pca = PCA(n_components=explained_var, svd_solver="full")
    Z = pca.fit_transform(Xs)
    Xhat = pca.inverse_transform(Z)
    resid = Xs - Xhat
    spe = np.sum(resid ** 2, axis=1)
    t2 = np.sum((Z ** 2) / (pca.explained_variance_ + EPS), axis=1)
    return scaler, pca, float(np.nanquantile(spe, score_q)), float(np.nanquantile(t2, score_q))


def score_pca(live_df, scaler, pca, spe_thr, t2_thr):
    X = live_df.ffill().bfill().values
    Xs = scaler.transform(X)
    Z = pca.transform(Xs)
    Xhat = pca.inverse_transform(Z)
    resid = Xs - Xhat
    spe = np.sum(resid ** 2, axis=1)
    t2 = np.sum((Z ** 2) / (pca.explained_variance_ + EPS), axis=1)
    spe_s = np.clip(spe / (spe_thr + EPS), 0, 5) / 5.0
    t2_s = np.clip(t2 / (t2_thr + EPS), 0, 5) / 5.0
    return pd.DataFrame({"mv_score": np.maximum(spe_s, t2_s), "spe": spe, "t2": t2}, index=live_df.index)


print("--- PCA ---")
# AFTER (fixed):
mech_sensors  = [s for s in catalog["MECH_VIB"]  if s in df.columns]
therm_sensors = [s for s in catalog["THERM_TEMP"] if s in df.columns]
elec_sensors  = [s for s in catalog["ELEC"]       if s in df.columns]

# Keep DER_KW_RES for ENGINE scoring (topk_mean) but NOT for decomposition
elec_sensors_scoring = elec_sensors.copy()
if DER_KW_RES in df.columns and DER_KW_RES not in elec_sensors_scoring:
    elec_sensors_scoring.append(DER_KW_RES)

running_df = df.loc[running]
mv_mech = mv_elec = mv_therm = None

for label, sens, container_name in [
    ("MECH", mech_sensors, "mv_mech"),
    ("ELEC", elec_sensors, "mv_elec"),
    ("THERM", therm_sensors, "mv_therm")
]:
    cand = select_mv_candidates(df, running, sensor_cfg, sens, cfg.mv_missing_max, cfg.mv_mean_abs_corr_min)
    if len(cand) >= 3 and running_df[cand].dropna(how="all").shape[0] >= cfg.n_stable_min:
        sc, pca_model, spe_t, t2_t = train_pca(running_df[cand], cfg.mv_explained_var, cfg.mv_score_q)
        result = score_pca(df[cand], sc, pca_model, spe_t, t2_t)
        if container_name == "mv_mech": mv_mech = result
        elif container_name == "mv_elec": mv_elec = result
        else: mv_therm = result
        print(f"   {label} PCA: {len(cand)} sensors, {pca_model.n_components_} components")
    else:
        print(f"   {label} PCA: skipped ({len(cand)} candidates)")

print(f"âœ… PCA complete.")

# %%
# %% [code]
# =============================================================================
# Cell 15: Block Scoring â†’ Fusion â†’ Classification â†’ Risk
# =============================================================================
# Combines Engine A (drift), Engine B (periodicity), PCA multivariate,
# and physics validation into subsystem scores. Classifies fault types
# and computes risk scores. Downtime periods are forced to score 0.
# =============================================================================

def merge_engine_scores(A: pd.DataFrame, B: pd.DataFrame) -> pd.DataFrame:
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


def topk_mean(score_df: pd.DataFrame, sensors: List[str], k: int) -> pd.Series:
    """Top-k mean to prevent one noisy sensor from dominating."""
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
        valid_sorted = np.sort(valid)[::-1]
        topk = valid_sorted[:min(k, len(valid_sorted))]
        result[i] = np.mean(topk)

    return pd.Series(result, index=score_df.index)


def fuse_subsystem_score(mech, elec, therm, phys, cfg):
    w = np.array([cfg.w_mech, cfg.w_elec, cfg.w_therm, cfg.w_physics])
    X = np.vstack([mech.fillna(0).values, elec.fillna(0).values,
                   therm.fillna(0).values, phys.fillna(0).values])
    return pd.Series((w @ X) / (w.sum() + EPS), index=mech.index)


def classify_type(mech, elec, therm, phys, subsystem, cfg):
    m = mech.fillna(0)
    e = elec.fillna(0)
    t = therm.fillna(0)
    p = phys.fillna(0)
    s = subsystem.fillna(0)

    label = pd.Series("NORMAL", index=subsystem.index)

    process_fault = (
        (e >= cfg.medium) & (m >= cfg.medium * 0.6) &
        (s >= cfg.medium) & (p < cfg.high)
    )
    mech_fault = (m >= cfg.high) & (p < cfg.high)
    elec_fault = (e >= cfg.high) & (p >= cfg.medium)
    instrument_fault = (
        (p >= cfg.high) &
        (e < cfg.medium * 0.8) &
        (m < cfg.medium * 0.8) &
        (t < cfg.medium * 0.8)
    )

    label.loc[s < cfg.medium] = "NORMAL"
    label.loc[process_fault] = "PROCESS"
    label.loc[mech_fault] = "MECH"
    label.loc[elec_fault] = "ELEC"
    label.loc[instrument_fault] = "INSTRUMENT"
    return label


def compute_risk_scores(mode, mech, elec, therm, phys, sqs_summary, cfg):
    mech_s  = mech.fillna(0)
    elec_s  = elec.fillna(0)
    therm_s = therm.fillna(0)
    phys_s  = phys.fillna(0)

    # Gate: zero during downtime, full weight during running
    gate = pd.Series(1.0, index=mode.index)
    gate.loc[mode == "DOWNTIME"] = 0.0

    conf = np.clip(sqs_summary["sqs_mean"].fillna(0).values, 0, 1)
    conf = pd.Series(conf, index=mode.index)

    p10 = np.clip(sqs_summary["sqs_p10"].fillna(0).values, 0, 1)
    instr = np.maximum(phys_s.values, (1.0 - p10))
    instr = pd.Series(np.clip(instr, 0, 1), index=mode.index)

    rw = np.array([cfg.rw_mech, cfg.rw_elec, cfg.rw_therm, cfg.rw_instrument])
    X = np.vstack([mech_s.values, elec_s.values, therm_s.values, instr.values])
    weighted = pd.Series((rw @ X) / (rw.sum() + EPS), index=mode.index)

    risk = weighted * conf * gate

    return pd.DataFrame({
        "risk_mech": mech_s * conf * gate,
        "risk_elec": elec_s * conf * gate,
        "risk_therm": therm_s * conf * gate,
        "risk_instrument": instr * conf * gate,
        "risk_score": risk,
    }, index=mode.index)


def build_sensor_risk_decomposition(
    index: pd.Index,
    combined_scores: pd.DataFrame,
    mech_final: pd.Series, elec_final: pd.Series, therm_final: pd.Series,
    mech_sensors: List[str], elec_sensors: List[str], therm_sensors: List[str],
    sqs_summary: pd.DataFrame, mode: pd.Series, phys_score: pd.Series,
    cfg: PipelineConfig
) -> pd.DataFrame:
    """
    Decompose final risk_score into sensor-level contributions.

    mech/elec/therm are decomposed from their final subsystem scores.
    The instrument term (max(physics, 1 - p10)) is kept as a virtual sensor
    because it is not directly attributable to a single physical sensor.
    """
    score_df = combined_scores.copy()
    score_df.columns = score_df.columns.map(str)

    conf = pd.Series(np.clip(sqs_summary["sqs_mean"].fillna(0).values, 0, 1), index=index)
    p10 = pd.Series(np.clip(sqs_summary["sqs_p10"].fillna(0).values, 0, 1), index=index)
    gate = pd.Series(1.0, index=index)
    gate.loc[mode == "DOWNTIME"] = 0.0
    instr = pd.Series(
        np.clip(np.maximum(phys_score.fillna(0).values, (1.0 - p10.values)), 0, 1),
        index=index
    )

    rw_sum = cfg.rw_mech + cfg.rw_elec + cfg.rw_therm + cfg.rw_instrument
    subsystem_specs = [
        ("MECH", [str(s) for s in mech_sensors], mech_final, cfg.rw_mech / (rw_sum + EPS)),
        ("ELEC", [str(s) for s in elec_sensors], elec_final, cfg.rw_elec / (rw_sum + EPS)),
        ("THERM", [str(s) for s in therm_sensors], therm_final, cfg.rw_therm / (rw_sum + EPS)),
    ]

    def allocate(values: List[Tuple[str, float]], final_score: float, topk: int):
        """Allocate final subsystem score into base(top-k) and uplift parts."""
        base_alloc: Dict[str, float] = {}
        uplift_alloc: Dict[str, float] = {}

        if final_score <= 0:
            return base_alloc, uplift_alloc

        values_sorted = sorted(values, key=lambda x: x[1], reverse=True)
        top = values_sorted[:min(topk, len(values_sorted))]
        if top:
            top_vals = np.array([v for _, v in top], dtype=float)
            base_score = float(np.mean(top_vals))
        else:
            top_vals = np.array([], dtype=float)
            base_score = 0.0

        base_part = min(base_score, final_score)
        uplift_part = max(0.0, final_score - base_part)

        if base_part > 0 and len(top) > 0:
            denom = float(np.sum(top_vals))
            if denom > EPS:
                for s, v in top:
                    base_alloc[s] = base_part * (v / denom)
            else:
                eq = base_part / len(top)
                for s, _ in top:
                    base_alloc[s] = eq

        if uplift_part > 0:
            positive = [(s, v) for s, v in values_sorted if v > 0]
            pool = positive if positive else values_sorted
            if pool:
                denom = float(sum(v for _, v in pool))
                if denom > EPS:
                    for s, v in pool:
                        uplift_alloc[s] = uplift_part * (v / denom)
                else:
                    eq = uplift_part / len(pool)
                    for s, _ in pool:
                        uplift_alloc[s] = eq

        return base_alloc, uplift_alloc

    rows = []
    k = cfg.block_score_topk

    for i, ts in enumerate(index):
        conf_i = float(conf.iloc[i])
        gate_i = float(gate.iloc[i])

        for subsystem, sensors, final_series, w_sub in subsystem_specs:
            final_val = float(final_series.iloc[i]) if np.isfinite(final_series.iloc[i]) else 0.0
            final_val = max(0.0, final_val)
            if final_val <= 0:
                continue

            row_scores = score_df.iloc[i]
            present = [s for s in sensors if s in score_df.columns]
            values = []
            for s in present:
                v = row_scores[s]
                if np.isfinite(v):
                    values.append((s, float(v)))

            # If no finite score exists at this timestamp, distribute equally across
            # available subsystem sensors so the decomposition remains sensor-level.
            if not values:
                if present:
                    eq = final_val / len(present)
                    for sensor in present:
                        risk_component = eq * w_sub * conf_i * gate_i
                        rows.append({
                            "timestamp_utc": ts,
                            "sensor_id": sensor,
                            "subsystem": subsystem,
                            "is_virtual_sensor": False,
                            "base_component": 0.0,
                            "uplift_component": eq,
                            "subsystem_score_component": eq,
                            "risk_weight": w_sub,
                            "confidence_factor": conf_i,
                            "gate_factor": gate_i,
                            "risk_score_component": risk_component,
                        })
                else:
                    risk_component = final_val * w_sub * conf_i * gate_i
                    rows.append({
                        "timestamp_utc": ts,
                        "sensor_id": f"__{subsystem}_LATENT__",
                        "subsystem": subsystem,
                        "is_virtual_sensor": True,
                        "base_component": 0.0,
                        "uplift_component": final_val,
                        "subsystem_score_component": final_val,
                        "risk_weight": w_sub,
                        "confidence_factor": conf_i,
                        "gate_factor": gate_i,
                        "risk_score_component": risk_component,
                    })
                continue

            base_alloc, uplift_alloc = allocate(values, final_val, k)
            sensors_union = sorted(set(base_alloc.keys()) | set(uplift_alloc.keys()))

            for sensor in sensors_union:
                base_c = float(base_alloc.get(sensor, 0.0))
                uplift_c = float(uplift_alloc.get(sensor, 0.0))
                sub_c = base_c + uplift_c
                if sub_c <= 0:
                    continue
                risk_component = sub_c * w_sub * conf_i * gate_i
                rows.append({
                    "timestamp_utc": ts,
                    "sensor_id": sensor,
                    "subsystem": subsystem,
                    "is_virtual_sensor": False,
                    "base_component": base_c,
                    "uplift_component": uplift_c,
                    "subsystem_score_component": sub_c,
                    "risk_weight": w_sub,
                    "confidence_factor": conf_i,
                    "gate_factor": gate_i,
                    "risk_score_component": risk_component,
                })

        instr_i = float(instr.iloc[i])
        if instr_i > 0:
            w_instr = cfg.rw_instrument / (rw_sum + EPS)
            instrument_sensors = [
                s for s in [COL_KW, COL_PF, COL_VLL, COL_IAVG, COL_FREC]
                if s in score_df.columns
            ]
            if not instrument_sensors:
                instrument_sensors = [str(c) for c in score_df.columns]

            if instrument_sensors:
                eq = instr_i / len(instrument_sensors)
                for sensor in instrument_sensors:
                    rows.append({
                        "timestamp_utc": ts,
                        "sensor_id": sensor,
                        "subsystem": "INSTRUMENT",
                        "is_virtual_sensor": False,
                        "base_component": eq,
                        "uplift_component": 0.0,
                        "subsystem_score_component": eq,
                        "risk_weight": w_instr,
                        "confidence_factor": conf_i,
                        "gate_factor": gate_i,
                        "risk_score_component": eq * w_instr * conf_i * gate_i,
                    })
            else:
                rows.append({
                    "timestamp_utc": ts,
                    "sensor_id": "__INSTRUMENT__",
                    "subsystem": "INSTRUMENT",
                    "is_virtual_sensor": True,
                    "base_component": instr_i,
                    "uplift_component": 0.0,
                    "subsystem_score_component": instr_i,
                    "risk_weight": w_instr,
                    "confidence_factor": conf_i,
                    "gate_factor": gate_i,
                    "risk_score_component": instr_i * w_instr * conf_i * gate_i,
                })

    return pd.DataFrame(rows)


# =================== EXECUTE ===================
print("--- Block Scoring (top-k mean) ---")

combined = merge_engine_scores(A, B)

k = cfg.block_score_topk
# Use elec_sensors_scoring for topk_mean (internal scoring)
mech_score  = topk_mean(combined, mech_sensors, k)
therm_score = topk_mean(combined, therm_sensors, k)
elec_score  = topk_mean(combined, elec_sensors_scoring, k)

# Fold PCA scores
if mv_mech is not None:
    mech_score = pd.Series(
        np.nanmax(np.vstack([mech_score.fillna(0).values, mv_mech["mv_score"].values]), axis=0),
        index=df.index
    )
if mv_elec is not None:
    elec_score = pd.Series(
        np.nanmax(np.vstack([elec_score.fillna(0).values, mv_elec["mv_score"].values]), axis=0),
        index=df.index
    )
if mv_therm is not None:
    therm_score = pd.Series(
        np.nanmax(np.vstack([therm_score.fillna(0).values, mv_therm["mv_score"].values]), axis=0),
        index=df.index
    )

# Force 0 during downtime
mech_score.loc[downtime]  = 0.0
elec_score.loc[downtime]  = 0.0
therm_score.loc[downtime] = 0.0

print(f"   Mech score (running mean):  {mech_score[running].mean():.4f}")
print(f"   Elec score (running mean):  {elec_score[running].mean():.4f}")
print(f"   Therm score (running mean): {therm_score[running].mean():.4f}")

subsystem_score = fuse_subsystem_score(mech_score, elec_score, therm_score, phys_score, cfg)
cls = classify_type(mech_score, elec_score, therm_score, phys_score, subsystem_score, cfg)
risk_df = compute_risk_scores(mode, mech_score, elec_score, therm_score, phys_score, sqs_summary, cfg)
# Use elec_sensors (catalog-only) for decomposition (user-facing)
risk_sensor_decomposition = build_sensor_risk_decomposition(
    df.index, combined,
    mech_score, elec_score, therm_score,
    mech_sensors, elec_sensors, therm_sensors,  # ← catalog-only, no DERIVED
    sqs_summary, mode, phys_score, cfg
)

if not risk_sensor_decomposition.empty:
    risk_sum = risk_sensor_decomposition.groupby("timestamp_utc")["risk_score_component"].sum()
    risk_sum = risk_sum.reindex(df.index, fill_value=0.0)
    decomp_max_abs_err = float(np.max(np.abs(risk_sum.values - risk_df["risk_score"].values)))
    print(f"   Risk decomposition rows: {len(risk_sensor_decomposition):,}")
    print(f"   Risk decomposition max |sum - risk_score|: {decomp_max_abs_err:.6e}")
else:
    print("   Risk decomposition rows: 0")

print(f"\n   Subsystem score (running mean): {subsystem_score[running].mean():.4f}")
print(f"\n   Classification:")
print(cls.value_counts().to_string())
print(f"\n   Risk score mean: {risk_df['risk_score'].mean():.4f}")
print(f"   Risk > MEDIUM:   {(risk_df['risk_score'] > cfg.medium).sum():,} min")
print(f"   Risk > HIGH:     {(risk_df['risk_score'] > cfg.high).sum():,} min")
print(f"âœ… Scoring complete.")

# %%
# %% [code]
# =============================================================================
# Cell 16: Alert Episodes
# =============================================================================
# Identifies contiguous time intervals where risk score exceeds threshold.
# Merges nearby episodes and filters by minimum duration.
# =============================================================================

def build_alert_episodes(
    ts, score, label, sensor_score_df, sensor_catalog,
    min_duration, merge_gap, threshold
):
    def class_to_sensor_candidates(main_class: str, sensor_catalog: Dict[str, List[str]]) -> List[str]:
        if main_class == "MECH":
            return sensor_catalog["MECH_VIB"]
        if main_class == "ELEC":
            return sensor_catalog["ELEC"]
        if main_class == "THERM":
            return sensor_catalog["THERM_TEMP"]
        if main_class == "PROCESS":
            return sensor_catalog["MECH_VIB"] + sensor_catalog["ELEC"]
        if main_class == "INSTRUMENT":
            return [COL_KW, COL_PF, COL_VLL, COL_IAVG, COL_FREC] + sensor_catalog["ELEC"]
        return sensor_catalog["MECH_VIB"] + sensor_catalog["THERM_TEMP"] + sensor_catalog["ELEC"]

    def summarize_episode_sensors(
        score_df: pd.DataFrame, candidates: List[str], start_idx: int, end_idx: int
    ) -> pd.DataFrame:
        out_cols = ["sensor", "sensor_rank", "sensor_peak_score", "sensor_mean_score"]
        available = [s for s in candidates if s in score_df.columns]
        if not available:
            return pd.DataFrame(columns=out_cols)

        seg = score_df.iloc[start_idx:end_idx + 1][available]
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

        # "Affected" means positive anomaly score within the alert window.
        positive = summary[summary["sensor_peak_score"] > 0]
        if not positive.empty:
            summary = positive

        summary = summary.sort_values(
            ["sensor_peak_score", "sensor_mean_score", "sensor"],
            ascending=[False, False, True],
            kind="stable"
        ).reset_index(drop=True)
        summary["sensor_rank"] = np.arange(1, len(summary) + 1, dtype=int)
        return summary[out_cols]

    sensor_score_df = sensor_score_df.copy()
    sensor_score_df.columns = sensor_score_df.columns.map(str)
    sensor_catalog = {k: [str(s) for s in v] for k, v in sensor_catalog.items()}

    is_on = (score >= threshold).fillna(False).values
    intervals = []
    start = None
    for i, on in enumerate(is_on):
        if on and start is None:
            start = i
        if (not on) and start is not None:
            intervals.append((start, i - 1))
            start = None
    if start is not None:
        intervals.append((start, len(is_on) - 1))

    merged = []
    for a, b in intervals:
        if not merged:
            merged.append([a, b])
        else:
            if a - merged[-1][1] - 1 <= merge_gap:
                merged[-1][1] = b
            else:
                merged.append([a, b])

    episodes = []
    sensor_alert_rows = []
    episode_cols = [
        "start_ts", "end_ts", "duration_minutes", "severity", "class",
        "sensor_id", "sensor_max_score", "sensor_mean_score",
        "affected_sensor_count", "affected_sensors", "max_score", "mean_score"
    ]
    sensor_alert_cols = [
        "start_ts", "end_ts", "duration_minutes", "severity", "class",
        "sensor", "sensor_rank", "sensor_peak_score", "sensor_mean_score",
        "alert_max_score", "alert_mean_score"
    ]
    for a, b in merged:
        dur = b - a + 1
        if dur >= min_duration:
            seg = label.iloc[a:b + 1]
            main = seg.value_counts().index[0] if len(seg) else "UNKNOWN"
            sensor_candidates = class_to_sensor_candidates(main, sensor_catalog)
            sensor_summary = summarize_episode_sensors(sensor_score_df, sensor_candidates, a, b)

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

            alert_max = float(score.iloc[a:b + 1].max())
            alert_mean = float(score.iloc[a:b + 1].mean())
            severity = "HIGH" if alert_max >= 0.85 else "MEDIUM"

            episodes.append({
                "start_ts": ts[a], "end_ts": ts[b],
                "duration_minutes": dur, "severity": severity,
                "class": main,
                "sensor_id": sensor_id,
                "sensor_max_score": sensor_max_score,
                "sensor_mean_score": sensor_mean_score,
                "affected_sensor_count": affected_sensor_count,
                "affected_sensors": affected_sensors,
                "max_score": alert_max,
                "mean_score": alert_mean,
            })

            if not sensor_summary.empty:
                for row in sensor_summary.itertuples(index=False):
                    sensor_alert_rows.append({
                        "start_ts": ts[a], "end_ts": ts[b],
                        "duration_minutes": dur, "severity": severity,
                        "class": main,
                        "sensor": str(row.sensor),
                        "sensor_rank": int(row.sensor_rank),
                        "sensor_peak_score": float(row.sensor_peak_score),
                        "sensor_mean_score": float(row.sensor_mean_score),
                        "alert_max_score": alert_max,
                        "alert_mean_score": alert_mean,
                    })
    return (
        pd.DataFrame(episodes, columns=episode_cols),
        pd.DataFrame(sensor_alert_rows, columns=sensor_alert_cols),
    )


print("--- Alerts ---")
alerts_med, alerts_sensor_med = build_alert_episodes(
    df.index, risk_df["risk_score"], cls, combined, catalog,
    cfg.alert_min_duration_min, cfg.alert_merge_gap_min, cfg.medium
)
alerts_high, alerts_sensor_high = build_alert_episodes(
    df.index, risk_df["risk_score"], cls, combined, catalog,
    cfg.alert_min_duration_min, cfg.alert_merge_gap_min, cfg.high
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
    print(alerts.to_string(index=False))
print(f"âœ… Alerts complete.")

# %%
# %% [code]
# =============================================================================
# Cell 17: Assemble + Save
# =============================================================================
# Combines all scores into a single dataframe and saves outputs
# to parquet, CSV, and text files for downstream analysis.
# =============================================================================

scores = pd.DataFrame(index=df.index)
scores.index.name = "timestamp_utc"
scores["mode"]  = mode
scores["class"] = cls
scores["mech_score"]      = mech_score
scores["elec_score"]      = elec_score
scores["therm_score"]     = therm_score
scores["physics_score"]   = phys_score
scores["subsystem_score"] = subsystem_score
scores["pf_scale"]               = pf_scale
scores["vll_scale_for_identity"] = vll_scale
if DER_KW_PRED in df.columns:
    scores["kw_expected"] = df[DER_KW_PRED]
    scores["kw_residual"] = df[DER_KW_RES]
scores = pd.concat([scores, sqs_summary, risk_df], axis=1)

# Save outputs
output_dir = r"data"
os.makedirs(output_dir, exist_ok=True)

scores.to_parquet(os.path.join(output_dir, "scores.parquet"))
scores.to_csv(os.path.join(output_dir, "scores.csv"))

if not risk_sensor_decomposition.empty:
    risk_sensor_decomposition.to_parquet(
        os.path.join(output_dir, "risk_sensor_decomposition.parquet"),
        index=False
    )
risk_sensor_decomposition.to_csv(
    os.path.join(output_dir, "risk_sensor_decomposition.csv"),
    index=False
)

if not alerts.empty:
    alerts.to_parquet(os.path.join(output_dir, "alerts.parquet"), index=False)
    alerts.to_csv(os.path.join(output_dir, "alerts.csv"), index=False)
    try:
        alerts.to_excel(os.path.join(output_dir, "alerts.xlsx"), index=False)
    except Exception as exc:
        print(f"âš ï¸ Could not write alerts.xlsx ({exc})")

if not alerts_sensor.empty:
    alerts_sensor.to_parquet(os.path.join(output_dir, "alerts_sensor_level.parquet"), index=False)
alerts_sensor.to_csv(os.path.join(output_dir, "alerts_sensor_level.csv"), index=False)

sensor_cfg_out = sensor_cfg.copy()
sensor_cfg_out["config_start_ts"] = df.index.min()
sensor_cfg_out["config_end_ts"]   = df.index.max()
sensor_cfg_out.to_parquet(os.path.join(output_dir, "sensor_config.parquet"), index=False)
sensor_cfg_out.to_csv(os.path.join(output_dir, "sensor_config.csv"), index=False)

print(f"âœ… Scores: {scores.shape}")
print(f"âœ… Alerts: {len(alerts)}")
print(f"âœ… Sensor-level alert rows: {len(alerts_sensor)}")
print(f"âœ… Risk decomposition rows: {len(risk_sensor_decomposition)}")
print(f"âœ… Saved to: {output_dir}")


# Signal completion for the dashboard
_done_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pipeline.done")
with open(_done_path, "w") as _f:
    _f.write("done")
print("Pipeline complete.")

# %%




