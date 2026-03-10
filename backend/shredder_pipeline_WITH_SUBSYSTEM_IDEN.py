# %%
# =============================================================================
# Cell 1: Imports and Constants
# =============================================================================
from __future__ import annotations

import os
import warnings
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import HuberRegressor, LinearRegression
from sklearn.metrics import r2_score
from scipy.cluster.hierarchy import linkage, fcluster, dendrogram
from scipy.spatial.distance import squareform

warnings.filterwarnings("ignore")

EPS = 1e-9

print("✅ Imports complete.")


# =============================================================================
# Cell 2: Pipeline Configuration
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

    # --- Fixed weight for physics / instrument ---
    w_physics: float = 0.15
    rw_instrument: float = 0.15

    # --- Alert thresholds ---
    high: float = 0.80
    medium: float = 0.55

    # --- Episode controls ---
    alert_min_duration_min: int = 5
    alert_merge_gap_min: int = 3

    # --- Physics ---
    physics_exclude_downtime: bool = True

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
    output_dir: str = "NEW"


cfg = PipelineConfig()
print("✅ Configuration loaded.")
print(f"   Parquet path: {cfg.parquet_path}")


# =============================================================================
# Cell 3: Core Electrical Column Constants
# =============================================================================
COL_KW   = "DESF_TA__KW_TOT"
COL_PF   = "DESF_TA__PF"
COL_VLL  = "DESF_TA__J2_V_AVG_LL"
COL_IAVG = "DESF_TA__J1_I_AVG"
COL_FREC = "DESF_TA__FREC"

DER_KW_PRED = "DERIVED.kw_expected"
DER_KW_RES  = "DERIVED.kw_residual"

CORE_ELECTRICAL = [COL_KW, COL_PF, COL_VLL, COL_IAVG, COL_FREC]

print("✅ Core electrical constants defined.")


# =============================================================================
# Cell 4: Read Parquet + Pivot + Resample
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

missing_core = [c for c in CORE_ELECTRICAL if c not in df.columns]
if missing_core:
    print(f"⚠️  Missing core columns (physics may be limited): {missing_core}")
else:
    print("✅ All core electrical columns present.")
print(f"✅ Minute-grid data: {df.shape[0]:,} rows × {df.shape[1]} columns")


# =============================================================================
# Cell 5: Scale Inference
# =============================================================================
def infer_pf_scale(pf_raw: pd.Series) -> float:
    med = np.nanmedian(pf_raw.dropna().values)
    scale = 0.1 if np.isfinite(med) and med > 1.2 else 1.0
    print(f"   PF: median={med:.3f}, scale={scale}")
    return scale


def infer_vll_scale_for_identity(
    kw: pd.Series, vll: pd.Series, iavg: pd.Series, pf_unit: pd.Series
) -> int:
    valid = kw.notna() & vll.notna() & iavg.notna() & pf_unit.notna()
    running_mask = valid & (kw > 10) & (iavg > 1)
    if running_mask.sum() < 100:
        return 1
    kw_r = kw[running_mask]
    vll_r = vll[running_mask]
    iavg_r = iavg[running_mask]
    pf_r = pf_unit[running_mask]
    best_scale, best_med = 1, float("inf")
    for s in [1, 10, 100, 1000, 10000]:
        kw_est = (np.sqrt(3) * (vll_r * s) * iavg_r * pf_r) / 1000.0
        resid = (kw_r - kw_est).abs() / (kw_r.abs() + EPS)
        med = np.nanmedian(resid.values)
        if np.isfinite(med) and med < best_med:
            best_med, best_scale = med, s
    print(f"   Best VLL scale: {best_scale} (residual: {best_med:.4f})")
    return best_scale


pf_scale = infer_pf_scale(df[COL_PF]) if COL_PF in df.columns else 1.0
pf_unit = df[COL_PF] * pf_scale if COL_PF in df.columns else pd.Series(1.0, index=df.index)
vll_scale = (
    infer_vll_scale_for_identity(df[COL_KW], df[COL_VLL], df[COL_IAVG], pf_unit)
    if all(c in df.columns for c in [COL_KW, COL_VLL, COL_IAVG, COL_PF])
    else 1
)
print(f"✅ Scale inference: PF_scale={pf_scale}, VLL_scale={vll_scale}")



# %%

# =============================================================================
# Cell 6: Base Invalid Mask + Bad Cell Nullification
# =============================================================================
def base_invalid_mask(df: pd.DataFrame, pf_scale: float) -> pd.DataFrame:
    bad = df.isna().copy()
    for c in [x for x in df.columns if "J2_V" in x]:
        bad[c] = bad[c] | (df[c] <= 0)
    for c in [x for x in df.columns if "FREC" in x]:
        bad[c] = bad[c] | (df[c] < 40) | (df[c] > 70)
    max_raw = 10.5 if pf_scale == 0.1 else 1.05
    if COL_PF in df.columns:
        bad[COL_PF] = bad[COL_PF] | (df[COL_PF] <= 0) | (df[COL_PF] > max_raw)
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


base_bad = base_invalid_mask(df, pf_scale)
df, base_bad = apply_bad_mask_nullify_cells(df, base_bad)
print(f"✅ Base invalid mask applied. df shape: {df.shape}")


# =============================================================================
# Cell 7: Downtime Detection (Electrical Signals Only)
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
df

# %%

# =============================================================================
# Cell 8: DYNAMIC SYSTEM DISCOVERY via Hierarchical Clustering
# =============================================================================
print("=" * 70)
print("DYNAMIC SYSTEM DISCOVERY")
print("=" * 70)


def clean_for_clustering(
    df: pd.DataFrame, running_mask: pd.Series,
    null_threshold_pct: float, ffill_limit: int,
) -> pd.DataFrame:
    df_running = df.loc[running_mask].copy()

    status_cols = [c for c in df_running.columns if "STATUS" in c.upper()]
    df_clean = df_running.drop(columns=status_cols, errors="ignore")

    null_pct = df_clean.isnull().mean() * 100
    high_null = null_pct[null_pct > null_threshold_pct].index.tolist()
    df_clean = df_clean.drop(columns=high_null, errors="ignore")
    print(f"   Dropped {len(status_cols)} STATUS cols, {len(high_null)} high-null cols")

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


def discover_systems(
    df_analysis: pd.DataFrame,
    corr_method: str, cluster_method: str,
    threshold_auto: bool, threshold_manual: float, threshold_max: float,
    min_system_size: int, output_dir: str,
) -> Tuple[Dict[str, List[str]], List[str], float]:
    """
    Returns:
        systems_dict:   {system_label: [sensor_list]}  (multi-variable)
        isolated_list:  [sensors that didn't cluster]
        threshold_used: float
    """
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
        sorted_dists = np.sort(merge_distances)
        gaps = np.diff(sorted_dists)
        if len(gaps) > 0:
            biggest_gap_idx = np.argmax(gaps)
            suggested = sorted_dists[biggest_gap_idx] + gaps[biggest_gap_idx] / 2
            threshold_used = min(suggested, threshold_max)
        else:
            threshold_used = threshold_manual
        print(f"   Auto threshold (gap heuristic): {threshold_used:.4f}")
    else:
        threshold_used = threshold_manual
        print(f"   Manual threshold: {threshold_used:.4f}")

    # --- Save dendrogram ---
    os.makedirs(output_dir, exist_ok=True)
    fig, ax = plt.subplots(figsize=(16, 8))
    dendrogram(
        Z, labels=df_analysis.columns.tolist(),
        leaf_rotation=90, leaf_font_size=7,
        color_threshold=threshold_used, above_threshold_color="gray", ax=ax,
    )
    ax.axhline(y=threshold_used, color="red", linestyle="--", linewidth=2,
               label=f"Threshold d={threshold_used:.2f}")
    ax.set_title("Hierarchical System Identification — Dendrogram", fontsize=14, fontweight="bold")
    ax.set_ylabel("Distance d(i,j)")
    ax.set_xlabel("Sensor Variables")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    plt.tight_layout()
    dend_path = os.path.join(output_dir, "dendrogram_systems.png")
    plt.savefig(dend_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"   💾 Saved dendrogram: {dend_path}")

    # --- Save correlation heatmap ---
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

    # --- Extract clusters ---
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

        r2_mean = float(np.mean(r2_adj_list))
        r2_min_val = float(np.min(r2_adj_list))
        quality = "HIGH ✅" if r2_mean > 0.8 else "MEDIUM ⚠️" if r2_mean > r2_min_quality else "LOW ❌"
        results.append({
            "System_ID": sys_id, "N_Variables": len(variables),
            "Variables": variables, "R2_Adj_Mean": round(r2_mean, 4),
            "R2_Adj_Min": round(r2_min_val, 4), "Quality": quality,
        })
        if r2_mean >= r2_min_quality:
            usable_systems[sys_id] = variables

    return pd.DataFrame(results), pd.DataFrame(details), usable_systems


# =================== EXECUTE SYSTEM DISCOVERY ===================

df_for_clustering = clean_for_clustering(
    df, running,
    null_threshold_pct=cfg.sys_null_threshold_pct,
    ffill_limit=cfg.sys_ffill_limit,
)

if len(df_for_clustering) < 100:
    print("⚠️  Very few clean rows for clustering. Results may be unreliable.")

systems_raw, isolated_sensors, threshold_used = discover_systems(
    df_for_clustering,
    corr_method=cfg.sys_corr_method,
    cluster_method=cfg.sys_cluster_method,
    threshold_auto=cfg.sys_threshold_auto,
    threshold_manual=cfg.sys_threshold_manual,
    threshold_max=cfg.sys_threshold_max,
    min_system_size=cfg.sys_min_system_size,
    output_dir=cfg.output_dir,
)

print("\n--- System Validation (Cross-Reconstruction R²) ---")
sys_summary, sys_detail, catalog = validate_systems_r2(
    df_for_clustering, systems_raw, cfg.sys_r2_adj_min_quality,
)
print(f"\n{'=' * 70}")
print("SYSTEM SUMMARY")
print(f"{'=' * 70}")
if not sys_summary.empty:
    print(sys_summary[["System_ID", "N_Variables", "R2_Adj_Mean", "R2_Adj_Min", "Quality"]].to_string(index=False))

# Add isolated sensors
if isolated_sensors:
    catalog["ISOLATED"] = isolated_sensors

all_catalog_sensors = []
for sensors in catalog.values():
    all_catalog_sensors.extend(sensors)

print(f"\n✅ Dynamic catalog: {len(catalog)} groups, {len(all_catalog_sensors)} total sensors")
for label, sensors in catalog.items():
    print(f"   {label}: {len(sensors)} sensors")

# --- Dynamic fusion weights (proportional to system size) ---
non_isolated = {k: v for k, v in catalog.items() if k != "ISOLATED"}
total_in_systems = sum(len(v) for v in non_isolated.values())

dynamic_weights: Dict[str, float] = {}
dynamic_risk_weights: Dict[str, float] = {}

remaining_fusion = 1.0 - cfg.w_physics
remaining_risk = 1.0 - cfg.rw_instrument

for label, sensors in non_isolated.items():
    frac = len(sensors) / max(total_in_systems, 1)
    dynamic_weights[label] = frac * remaining_fusion
    dynamic_risk_weights[label] = frac * remaining_risk

dynamic_weights["PHYSICS"] = cfg.w_physics
dynamic_risk_weights["INSTRUMENT"] = cfg.rw_instrument

print(f"\n   Dynamic fusion weights:  {dynamic_weights}")
print(f"   Dynamic risk weights:    {dynamic_risk_weights}")

# Save artifacts
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
print(f"   💾 Saved system artifacts to {cfg.output_dir}")


# =============================================================================
# Cell 9: Learn Bounds + ROC
# =============================================================================
def kw_identity_residual(df, kw_col, vll_col, iavg_col, pf_col, pf_scale, vll_scale):
    kw = df[kw_col]
    vll = df[vll_col]
    iavg = df[iavg_col]
    pf = df[pf_col] * pf_scale
    kw_est = (np.sqrt(3) * (vll * vll_scale) * iavg * pf) / 1000.0
    return (kw - kw_est).abs() / (kw.abs() + EPS)


def learn_bounds_roc(df, running_mask, n_min):
    running_df = df.loc[running_mask]
    rows = []
    for c in df.columns:
        x = running_df[c].dropna()
        if len(x) >= n_min:
            p01 = float(np.nanquantile(x.values, 0.001))
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
print(f"✅ Sensor config: {len(sensor_cfg)} sensors")


# %%

# =============================================================================
# Cell 10: SQS (Signal Quality Score)
# =============================================================================
def compute_sqs(
    df, sensor_cfg, pf_scale, vll_scale,
    kw_col, vll_col, iavg_col, pf_col, kw_pred,
):
    cfg_idx = sensor_cfg.set_index("sensor")
    sqs = pd.DataFrame(np.nan, index=df.index, columns=df.columns, dtype=float)

    for s in df.columns:
        sqs.loc[df[s].notna(), s] = 1.0

    for s in df.columns:
        if s not in cfg_idx.index:
            continue
        row = cfg_idx.loc[s]
        p0, p9, roc = row["p0_1"], row["p99_9"], row["roc_p99_9"]
        x = df[s]
        if np.isfinite(p0) and np.isfinite(p9):
            margin = (p9 - p0) * 0.01
            oob = (x < p0 - margin) | (x > p9 + margin)
            sqs.loc[oob & x.notna(), s] *= 0.6
        if np.isfinite(roc):
            jump = x.diff().abs() > (roc * 1.25)
            sqs.loc[jump & x.notna(), s] *= 0.7

    if all(c in df.columns for c in [kw_col, vll_col, iavg_col, pf_col]):
        rkw = kw_identity_residual(df, kw_col, vll_col, iavg_col, pf_col, pf_scale, vll_scale)
        thr_id = float(np.nanquantile(rkw.dropna().values, 0.99))
        bad_id = rkw > thr_id
        for c in [kw_col, vll_col, iavg_col, pf_col]:
            if c in sqs.columns:
                sqs.loc[bad_id & df[c].notna(), c] *= 0.85

    if kw_col in df.columns:
        kw = df[kw_col]
        r_reg = (kw - kw_pred).abs() / (kw.abs() + EPS)
        vals = r_reg.dropna().values
        if len(vals) > 100:
            thr_reg = float(np.nanquantile(vals, 0.995))
            sqs.loc[(r_reg > thr_reg) & kw.notna(), kw_col] *= 0.80

    return sqs.clip(0, 1)


def summarize_sqs(sqs):
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
    return pd.DataFrame({"sqs_mean": sqs_mean, "sqs_p10": sqs_p10,
                         "sqs_valid_frac": sqs_valid_frac}, index=sqs.index)


print("✅ SQS functions defined.")



# %%


# =============================================================================
# Cell 11: Regression → Derived Signals → Final SQS
# =============================================================================
def identity_expected_kw(df, vll_col, iavg_col, pf_col, pf_scale, vll_scale):
    if not all(c in df.columns for c in [vll_col, iavg_col, pf_col]):
        return pd.Series(np.nan, index=df.index)
    pf = df[pf_col] * pf_scale
    kw_est = (np.sqrt(3) * (df[vll_col] * vll_scale) * df[iavg_col] * pf) / 1000.0
    need = df[[vll_col, iavg_col, pf_col]].notna().all(axis=1)
    return kw_est.where(need)


def build_kw_features(df, vll_col, iavg_col, pf_col, frec_col, pf_scale, vll_scale):
    vll = df[vll_col] * vll_scale if vll_col in df.columns else pd.Series(np.nan, index=df.index)
    iavg = df[iavg_col] if iavg_col in df.columns else pd.Series(np.nan, index=df.index)
    pf = df[pf_col] * pf_scale if pf_col in df.columns else pd.Series(np.nan, index=df.index)
    frec = df[frec_col] if frec_col in df.columns else pd.Series(np.nan, index=df.index)
    return pd.DataFrame({
        "vll": vll, "iavg": iavg, "pf": pf, "frec": frec,
        "vll_i": vll * iavg, "i_pf": iavg * pf, "vll_pf": vll * pf,
    }, index=df.index)


def train_kw_regression(df, running_mask, sqs_prov, kw_col, X, sqs_min, min_points):
    if kw_col not in df.columns:
        return None, pd.Series(np.nan, index=df.index)
    kw = df[kw_col]
    ok = running_mask & kw.notna() & X.notna().all(axis=1)
    if kw_col in sqs_prov.columns:
        ok = ok & (sqs_prov[kw_col] >= sqs_min)
    print(f"   Regression training points: {ok.sum():,}")
    if ok.sum() < min_points:
        return None, pd.Series(np.nan, index=df.index)
    model = HuberRegressor()
    model.fit(X.loc[ok].values, kw.loc[ok].values)
    kw_pred = pd.Series(np.nan, index=df.index)
    okp = X.notna().all(axis=1)
    kw_pred.loc[okp] = model.predict(X.loc[okp].values)
    train_resid = kw.loc[ok] - model.predict(X.loc[ok].values)
    r2 = 1 - (train_resid.var() / kw.loc[ok].var())
    print(f"   Regression R²: {r2:.4f}, MAE: {train_resid.abs().mean():.2f}")
    return model, kw_pred


print("--- Pass 1: Provisional SQS ---")
dummy_pred = identity_expected_kw(df, COL_VLL, COL_IAVG, COL_PF, pf_scale, vll_scale)
dummy_pred = dummy_pred.ffill()
sqs0 = compute_sqs(df, sensor_cfg, pf_scale, vll_scale, COL_KW, COL_VLL, COL_IAVG, COL_PF, dummy_pred)

print("\n--- Regression Training ---")
X_feat = build_kw_features(df, COL_VLL, COL_IAVG, COL_PF, COL_FREC, pf_scale, vll_scale)
model_kw, kw_pred = train_kw_regression(
    df, running, sqs0, COL_KW, X_feat, cfg.reg_sqs_min, cfg.reg_min_train_points
)
if model_kw is None or kw_pred.isna().all():
    kw_pred = identity_expected_kw(df, COL_VLL, COL_IAVG, COL_PF, pf_scale, vll_scale)

kw_residual = df[COL_KW] - kw_pred if COL_KW in df.columns else pd.Series(np.nan, index=df.index)
df[DER_KW_PRED] = kw_pred
df[DER_KW_RES] = kw_residual

add_cfg = learn_bounds_roc(df[[DER_KW_PRED, DER_KW_RES]], running, n_min=cfg.n_stable_min)
add_cfg["pf_scale"] = pf_scale
add_cfg["vll_scale_for_identity"] = vll_scale
sensor_cfg = pd.concat([sensor_cfg, add_cfg], ignore_index=True)

print("\n--- Pass 2: Final SQS ---")
sqs = compute_sqs(df, sensor_cfg, pf_scale, vll_scale, COL_KW, COL_VLL, COL_IAVG, COL_PF, df[DER_KW_PRED])
sqs_summary = summarize_sqs(sqs)
print(f"   SQS mean: {sqs_summary['sqs_mean'].mean():.3f}")
print("✅ Regression + SQS complete.")

# %%

# =============================================================================
# Cell 12: Engine A — Drift Detection
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


def select_engineA_sensors(sensor_cfg, n_min, miss_max):
    return sensor_cfg[
        (sensor_cfg["n_running"] >= n_min) &
        (sensor_cfg["missing_pct"] <= miss_max) &
        (sensor_cfg["variance"] > 0)
    ]["sensor"].tolist()


def compute_engineA(df, sqs, running_mask, sensors, baseline_win, mad_win, sqs_min, k):
    A = pd.DataFrame(index=df.index, columns=sensors, dtype=float)
    cache = {}
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
    cfg.engineA_baseline_win, cfg.engineA_mad_win, cfg.engineA_sqs_min, cfg.engineA_score_k,
)
print(f"   Engine A shape: {A.shape}, mean score: {A.mean().mean():.4f}")
print("✅ Engine A complete.")

# %%
# =============================================================================
# Cell 13: Engine B — Periodicity Detection
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


def select_engineB_sensors(df, sensor_cfg, running_mask, sqs, sqs_min):
    cfg_idx = sensor_cfg.set_index("sensor")
    eligible = []
    for s in df.columns:
        if s not in cfg_idx.index:
            continue
        if cfg_idx.loc[s, "n_running"] < 200 or cfg_idx.loc[s, "variance"] <= 0:
            continue
        if s not in sqs.columns:
            continue
        ok = running_mask & df[s].notna() & (sqs[s] >= sqs_min)
        if ok.sum() < 200:
            continue
        eligible.append(s)
    return eligible[:30]


def compute_engineB(
    df, sqs, running_mask, sensors, baseline_cache,
    win, sqs_min, valid_frac_min, period_min, period_max,
):
    B = pd.DataFrame(index=df.index, columns=sensors, dtype=float)
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


print("--- Engine B ---")
B_sensors = select_engineB_sensors(df, sensor_cfg, running, sqs, cfg.engineB_sqs_min)
if DER_KW_RES not in B_sensors and DER_KW_RES in df.columns:
    B_sensors.append(DER_KW_RES)
print(f"   Eligible sensors: {len(B_sensors)}")

B = compute_engineB(
    df, sqs, running, B_sensors, baseline_cache,
    cfg.engineB_win, cfg.engineB_sqs_min, cfg.engineB_valid_frac_min,
    cfg.engineB_period_min, cfg.engineB_period_max,
)
print(f"   Engine B shape: {B.shape}, non-null: {B.notna().sum().sum():,}")
print("✅ Engine B complete.")


# =============================================================================
# Cell 14: Physics Validation
# =============================================================================
def physics_scores(df, pf_scale, vll_scale, cols, downtime):
    out = pd.DataFrame(index=df.index)
    running_mask = ~downtime
    need = ["kw_tot", "pf_tot", "vll_avg", "iavg"]
    if not all(k in cols and cols[k] in df.columns for k in need):
        out["physics_score"] = np.nan
        return out

    kw = df[cols["kw_tot"]].where(running_mask)
    pf = (df[cols["pf_tot"]] * pf_scale).where(running_mask)
    vll = (df[cols["vll_avg"]] * vll_scale).where(running_mask)
    iavg = df[cols["iavg"]].where(running_mask)

    kw_est = (np.sqrt(3) * vll * iavg * pf) / 1000.0
    out["r_kw_identity"] = (kw - kw_est).abs() / (kw.abs() + EPS)

    if "kva_tot" in cols and cols["kva_tot"] in df.columns:
        kva = df[cols["kva_tot"]].where(running_mask)
        out["r_pf_identity"] = ((kw / (kva + EPS)) - pf).abs()
    else:
        out["r_pf_identity"] = np.nan

    if all(k in cols and cols[k] in df.columns for k in ["kva_tot", "kvar_tot"]):
        kva = df[cols["kva_tot"]].where(running_mask)
        kvar = df[cols["kvar_tot"]].where(running_mask)
        out["r_kvar_identity"] = (kvar - np.sqrt(np.maximum(kva**2 - kw**2, 0))).abs() / (kvar.abs() + EPS)
    else:
        out["r_kvar_identity"] = np.nan

    if all(k in cols and cols[k] in df.columns for k in ["ia", "ib", "ic"]):
        ia = df[cols["ia"]].where(running_mask)
        ib = df[cols["ib"]].where(running_mask)
        ic = df[cols["ic"]].where(running_mask)
        out["unbalance_I"] = (
            np.maximum.reduce([ia, ib, ic]) - np.minimum.reduce([ia, ib, ic])
        ) / (iavg.abs() + EPS)
    else:
        out["unbalance_I"] = np.nan

    parts = []
    for c in ["r_kw_identity", "r_pf_identity", "r_kvar_identity", "unbalance_I"]:
        s = out[c]
        vals = s.dropna().values
        if len(vals) > 100:
            q = float(np.nanquantile(vals, 0.99))
            if np.isfinite(q) and q > EPS:
                parts.append(np.clip(s / (q + EPS), 0, 1))
    out["physics_score"] = np.maximum.reduce(parts) if parts else np.nan
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
print(f"   Physics score (running mean): {phys_score[running].mean():.4f}")
print("✅ Physics validation complete.")


# =============================================================================
# Cell 15: PCA Multivariate Anomaly Detection (per discovered system)
# =============================================================================
def mean_abs_corr(running_df):
    c = running_df.corr().abs()
    return c.where(~np.eye(c.shape[0], dtype=bool)).mean()


def select_mv_candidates(df, running_mask, sensor_cfg, sensors, mv_missing_max, mv_mean_abs_corr_min):
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
    spe = np.sum(resid**2, axis=1)
    t2 = np.sum((Z**2) / (pca.explained_variance_ + EPS), axis=1)
    return scaler, pca, float(np.nanquantile(spe, score_q)), float(np.nanquantile(t2, score_q))


def score_pca(live_df, scaler, pca, spe_thr, t2_thr):
    X = live_df.ffill().bfill().values
    Xs = scaler.transform(X)
    Z = pca.transform(Xs)
    resid = Xs - pca.inverse_transform(Z)
    spe = np.sum(resid**2, axis=1)
    t2 = np.sum((Z**2) / (pca.explained_variance_ + EPS), axis=1)
    spe_s = np.clip(spe / (spe_thr + EPS), 0, 5) / 5.0
    t2_s = np.clip(t2 / (t2_thr + EPS), 0, 5) / 5.0
    return pd.DataFrame({"mv_score": np.maximum(spe_s, t2_s), "spe": spe, "t2": t2}, index=live_df.index)


print("--- PCA (Dynamic Systems) ---")
running_df_full = df.loc[running]

mv_results: Dict[str, pd.DataFrame] = {}
for sys_label, sensors in catalog.items():
    if sys_label == "ISOLATED":
        continue
    present = [s for s in sensors if s in df.columns]
    cand = select_mv_candidates(
        df, running, sensor_cfg, present, cfg.mv_missing_max, cfg.mv_mean_abs_corr_min
    )
    if len(cand) >= 3 and running_df_full[cand].dropna(how="all").shape[0] >= cfg.n_stable_min:
        sc, pca_model, spe_t, t2_t = train_pca(
            running_df_full[cand], cfg.mv_explained_var, cfg.mv_score_q
        )
        mv_results[sys_label] = score_pca(df[cand], sc, pca_model, spe_t, t2_t)
        print(f"   {sys_label} PCA: {len(cand)} sensors, {pca_model.n_components_} components")
    else:
        print(f"   {sys_label} PCA: skipped ({len(cand)} candidates)")

print(f"✅ PCA complete for {len(mv_results)} systems.")


# %%
# =============================================================================
# Cell 16: Block Scoring → Fusion → Classification → Risk (Dynamic)
# =============================================================================
def merge_engine_scores(A, B):
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

system_scores: Dict[str, pd.Series] = {}
for sys_label, sensors in catalog.items():
    if sys_label == "ISOLATED":
        continue
    present = [s for s in sensors if s in df.columns]

    # Add DER_KW_RES for internal scoring if system has electrical-like sensors
    scoring_sensors = present.copy()
    if DER_KW_RES in df.columns and DER_KW_RES not in scoring_sensors:
        if any(any(k in s.upper() for k in ["KW", "KVA", "KVAR", "PF", "_I", "_V"]) for s in present):
            scoring_sensors.append(DER_KW_RES)

    score = topk_mean(combined, scoring_sensors, cfg.block_score_topk)

    # Fold PCA
    if sys_label in mv_results:
        score = pd.Series(
            np.nanmax(
                np.vstack([score.fillna(0).values, mv_results[sys_label]["mv_score"].values]),
                axis=0,
            ),
            index=df.index,
        )

    score.loc[downtime] = 0.0
    system_scores[sys_label] = score
    print(f"   {sys_label} score (running mean): {score[running].mean():.4f}")


# --- Dynamic Fusion ---
def dynamic_fuse(system_scores, phys_score, dynamic_weights, index):
    parts, weights = [], []
    for label, score_s in system_scores.items():
        w = dynamic_weights.get(label, 0.0)
        parts.append(score_s.fillna(0).values)
        weights.append(w)
    parts.append(phys_score.fillna(0).values)
    weights.append(dynamic_weights.get("PHYSICS", 0.15))
    W = np.array(weights)
    X = np.vstack(parts)
    return pd.Series((W @ X) / (W.sum() + EPS), index=index)


subsystem_score = dynamic_fuse(system_scores, phys_score, dynamic_weights, df.index)


# --- Dynamic Classification ---
def dynamic_classify(system_scores, phys_score, subsystem_score, cfg):
    label = pd.Series("NORMAL", index=subsystem_score.index)
    s = subsystem_score.fillna(0)
    p = phys_score.fillna(0)

    if system_scores:
        score_matrix = pd.DataFrame(
            {k: v.fillna(0) for k, v in system_scores.items()},
            index=subsystem_score.index,
        )
        dominant = score_matrix.idxmax(axis=1)
        max_score = score_matrix.max(axis=1)
    else:
        dominant = pd.Series("UNKNOWN", index=subsystem_score.index)
        max_score = pd.Series(0.0, index=subsystem_score.index)

    # Instrument: physics high, all systems low
    all_low = max_score < cfg.medium * 0.8
    instrument = (p >= cfg.high) & all_low
    label.loc[instrument] = "INSTRUMENT"

    # System-level faults: label = dominant system name
    active = s >= cfg.medium
    for sys_label in system_scores:
        sys_mask = active & (dominant == sys_label) & ~instrument
        label.loc[sys_mask] = sys_label

    # PROCESS: multiple systems elevated simultaneously
    if len(system_scores) >= 2:
        elevated_count = pd.Series(0, index=subsystem_score.index)
        for v in system_scores.values():
            elevated_count += (v.fillna(0) >= cfg.medium * 0.6).astype(int)
        process_mask = active & (elevated_count >= 2) & (p < cfg.high) & ~instrument
        label.loc[process_mask] = "PROCESS"

    return label


cls = dynamic_classify(system_scores, phys_score, subsystem_score, cfg)


# --- Dynamic Risk Scores ---
def dynamic_risk_scores(mode, system_scores, phys_score, sqs_summary, dynamic_risk_weights, cfg):
    gate = pd.Series(1.0, index=mode.index)
    gate.loc[mode == "DOWNTIME"] = 0.0
    conf = pd.Series(
        np.clip(sqs_summary["sqs_mean"].fillna(0).values, 0, 1), index=mode.index
    )
    p10 = np.clip(sqs_summary["sqs_p10"].fillna(0).values, 0, 1)
    instr = pd.Series(
        np.clip(np.maximum(phys_score.fillna(0).values, 1.0 - p10), 0, 1),
        index=mode.index,
    )

    risk_parts = {}
    weights, parts_arr = [], []
    for label, score_s in system_scores.items():
        w = dynamic_risk_weights.get(label, 0.0)
        risk_parts[f"risk_{label}"] = score_s.fillna(0) * conf * gate
        weights.append(w)
        parts_arr.append(score_s.fillna(0).values)

    w_instr = dynamic_risk_weights.get("INSTRUMENT", cfg.rw_instrument)
    risk_parts["risk_INSTRUMENT"] = instr * conf * gate
    weights.append(w_instr)
    parts_arr.append(instr.values)

    W = np.array(weights)
    X = np.vstack(parts_arr)
    risk_parts["risk_score"] = pd.Series((W @ X) / (W.sum() + EPS), index=mode.index)

    return pd.DataFrame(risk_parts, index=mode.index)


risk_df = dynamic_risk_scores(
    mode, system_scores, phys_score, sqs_summary, dynamic_risk_weights, cfg
)


# --- Sensor-level risk decomposition ---
def build_dynamic_sensor_risk_decomposition(
    index, combined_scores, system_scores_dict, catalog,
    sqs_summary, mode, phys_score, dynamic_risk_weights, cfg,
):
    score_df = combined_scores.copy()
    score_df.columns = score_df.columns.map(str)

    conf = pd.Series(np.clip(sqs_summary["sqs_mean"].fillna(0).values, 0, 1), index=index)
    p10 = pd.Series(np.clip(sqs_summary["sqs_p10"].fillna(0).values, 0, 1), index=index)
    gate = pd.Series(1.0, index=index)
    gate.loc[mode == "DOWNTIME"] = 0.0
    instr = pd.Series(
        np.clip(np.maximum(phys_score.fillna(0).values, 1.0 - p10.values), 0, 1),
        index=index,
    )

    rw_sum = sum(dynamic_risk_weights.values())

    sys_specs = []
    for label, sensors_list in catalog.items():
        if label == "ISOLATED":
            continue
        if label not in system_scores_dict:
            continue
        w = dynamic_risk_weights.get(label, 0.0) / (rw_sum + EPS)
        present = [str(s) for s in sensors_list if str(s) in score_df.columns]
        sys_specs.append((label, present, system_scores_dict[label], w))

    rows = []
    k = cfg.block_score_topk

    for i, ts in enumerate(index):
        conf_i = float(conf.iloc[i])
        gate_i = float(gate.iloc[i])

        for subsystem, sensors, final_series, w_sub in sys_specs:
            final_val = float(final_series.iloc[i]) if np.isfinite(final_series.iloc[i]) else 0.0
            final_val = max(0.0, final_val)
            if final_val <= 0:
                continue

            row_scores = score_df.iloc[i]
            values = []
            for s in sensors:
                v = row_scores.get(s, np.nan)
                if np.isfinite(v):
                    values.append((s, float(v)))

            if not values:
                if sensors:
                    eq = final_val / len(sensors)
                    for sensor in sensors:
                        rows.append({
                            "timestamp_utc": ts, "sensor_id": sensor,
                            "subsystem": subsystem, "is_virtual_sensor": False,
                            "subsystem_score_component": eq,
                            "risk_weight": w_sub, "confidence_factor": conf_i,
                            "gate_factor": gate_i,
                            "risk_score_component": eq * w_sub * conf_i * gate_i,
                        })
                continue

            values_sorted = sorted(values, key=lambda x: x[1], reverse=True)
            top = values_sorted[:min(k, len(values_sorted))]
            top_vals = np.array([v for _, v in top])
            base_score = float(np.mean(top_vals))
            base_part = min(base_score, final_val)
            uplift_part = max(0.0, final_val - base_part)

            denom = float(np.sum(top_vals))
            sensor_allocs: Dict[str, float] = {}
            for s, v in top:
                sensor_allocs[s] = base_part * (v / (denom + EPS))

            if uplift_part > 0:
                positive = [(s, v) for s, v in values_sorted if v > 0]
                pool = positive if positive else values_sorted
                denom_u = float(sum(v for _, v in pool))
                for s, v in pool:
                    alloc = uplift_part * (v / (denom_u + EPS))
                    sensor_allocs[s] = sensor_allocs.get(s, 0.0) + alloc

            for sensor, sub_c in sensor_allocs.items():
                if sub_c <= 0:
                    continue
                rows.append({
                    "timestamp_utc": ts, "sensor_id": sensor,
                    "subsystem": subsystem, "is_virtual_sensor": False,
                    "subsystem_score_component": sub_c,
                    "risk_weight": w_sub, "confidence_factor": conf_i,
                    "gate_factor": gate_i,
                    "risk_score_component": sub_c * w_sub * conf_i * gate_i,
                })

        # Instrument
        instr_i = float(instr.iloc[i])
        if instr_i > 0:
            w_instr = dynamic_risk_weights.get("INSTRUMENT", cfg.rw_instrument) / (rw_sum + EPS)
            instr_sensors = [str(s) for s in CORE_ELECTRICAL if str(s) in score_df.columns]
            if instr_sensors:
                eq = instr_i / len(instr_sensors)
                for sensor in instr_sensors:
                    rows.append({
                        "timestamp_utc": ts, "sensor_id": sensor,
                        "subsystem": "INSTRUMENT", "is_virtual_sensor": False,
                        "subsystem_score_component": eq,
                        "risk_weight": w_instr, "confidence_factor": conf_i,
                        "gate_factor": gate_i,
                        "risk_score_component": eq * w_instr * conf_i * gate_i,
                    })

    return pd.DataFrame(rows)


risk_sensor_decomposition = build_dynamic_sensor_risk_decomposition(
    df.index, combined, system_scores, catalog,
    sqs_summary, mode, phys_score, dynamic_risk_weights, cfg,
)

print(f"\n   Subsystem score (running mean): {subsystem_score[running].mean():.4f}")
print(f"   Classification:\n{cls.value_counts().to_string()}")
print(f"   Risk score mean: {risk_df['risk_score'].mean():.4f}")
print(f"   Risk > MEDIUM: {(risk_df['risk_score'] > cfg.medium).sum():,} min")
print(f"   Risk > HIGH:   {(risk_df['risk_score'] > cfg.high).sum():,} min")
print(f"   Decomposition rows: {len(risk_sensor_decomposition):,}")
print("✅ Dynamic scoring complete.")



# %%

# =============================================================================
# Cell 17: Alert Episodes
# =============================================================================
def build_alert_episodes(
    ts, score, label, sensor_score_df, sensor_catalog,
    min_duration, merge_gap, threshold,
):
    def class_to_sensor_candidates(main_class, sensor_catalog):
        # Exact match on system name
        for sys_label, sensors in sensor_catalog.items():
            if main_class == sys_label:
                return sensors
        # PROCESS → all non-isolated
        if main_class == "PROCESS":
            return [s for k, v in sensor_catalog.items() if k != "ISOLATED" for s in v]
        # INSTRUMENT → core electrical + everything
        if main_class == "INSTRUMENT":
            return CORE_ELECTRICAL + [s for v in sensor_catalog.values() for s in v]
        # Default: everything
        return [s for v in sensor_catalog.values() for s in v]

    def summarize_episode_sensors(score_df, candidates, start_idx, end_idx):
        available = [s for s in candidates if s in score_df.columns]
        if not available:
            return pd.DataFrame(columns=["sensor", "sensor_rank", "sensor_peak_score", "sensor_mean_score"])
        seg = score_df.iloc[start_idx:end_idx + 1][available]
        if seg.empty:
            return pd.DataFrame(columns=["sensor", "sensor_rank", "sensor_peak_score", "sensor_mean_score"])
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

        episodes.append({
            "start_ts": ts[a], "end_ts": ts[b], "duration_minutes": dur,
            "severity": severity, "class": main,
            "sensor_id": sid, "sensor_max_score": smax, "sensor_mean_score": smean,
            "affected_sensor_count": cnt, "affected_sensors": aff,
            "max_score": alert_max, "mean_score": alert_mean,
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
# Cell 18: Assemble + Save
# =============================================================================
scores = pd.DataFrame(index=df.index)
scores.index.name = "timestamp_utc"
scores["mode"] = mode
scores["class"] = cls

for sys_label, score_s in system_scores.items():
    scores[f"score_{sys_label}"] = score_s

scores["physics_score"] = phys_score
scores["subsystem_score"] = subsystem_score
scores["pf_scale"] = pf_scale
scores["vll_scale_for_identity"] = vll_scale

if DER_KW_PRED in df.columns:
    scores["kw_expected"] = df[DER_KW_PRED]
    scores["kw_residual"] = df[DER_KW_RES]

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

# --- Save per-system sensor value CSVs for charting ---
for sys_label, sensors in catalog.items():
    present = [s for s in sensors if s in df.columns]
    if not present:
        continue
    sys_df = df[present].copy()
    sys_df.index.name = "timestamp_utc"
    fname = f"sensor_values_{sys_label}.csv"
    sys_df.to_csv(os.path.join(output_dir, fname))
    print(f"   💾 Saved {fname}: {sys_df.shape}")

print(f"\n✅ Scores: {scores.shape}")
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
    print(f"  {label:20s}: {len(sensors):3d} sensors | fusion_w={w_f:.3f} | risk_w={w_r:.3f}")
print(f"  {'PHYSICS':20s}:   — sensors | fusion_w={dynamic_weights.get('PHYSICS', 0):.3f}")
print(f"  {'INSTRUMENT':20s}:   — sensors | risk_w={dynamic_risk_weights.get('INSTRUMENT', 0):.3f}")
print(f"\n  Clustering threshold: {threshold_used:.4f}")
print(f"  Running minutes: {running.sum():,}")
print(f"  Downtime minutes: {downtime.sum():,}")
print(f"  Risk alerts (MEDIUM+): {len(alerts)}")
print("✅ Pipeline complete.")


