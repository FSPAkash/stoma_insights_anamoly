# Beta Dashboard Migration: Pipeline Comparison & Plan

## 1. Pipeline Comparison: `shredder_pipeline_WITH_SUBSYSTEM_IDEN.py` vs `1403_SQS_AUTOENCODE_A_B 1.ipynb`

### 1.1 Architecture Changes

| Aspect | Current Pipeline | New Notebook |
|--------|-----------------|--------------|
| Multivariate Engine | PCA (sklearn) | Deep AutoEncoder (PyTorch) |
| Physics Engine | Full (kW identity, PF identity, kVAR, unbalance) | **Removed entirely** |
| Derived Signals | `DER_KW_PRED`, `DER_KW_RES` via HuberRegressor | **Removed** |
| Scale Inference | PF scale, VLL scale auto-detect | **Removed** |
| SQS Passes | 2-pass (provisional -> regression -> final) | Single-pass (bounds + ROC only) |
| Sensor Baseline | Always computed from data | Tries external Excel baseline first, fallback to data |
| Clustering Threshold | Gap heuristic (biggest gap in merge distances) | Stability Plateau method (longest constant K run) |
| System Validation | R2 only | R2 + SVD Cohesion + Condition Number |
| Directional Anomalies | None | Per-sensor config (`pos`/`neg`/`both`) for ~50 sensors |
| Classification Labels | NORMAL, SYS_X, PROCESS, INSTRUMENT | NORMAL, SYS_X, PROCESS (no INSTRUMENT) |

### 1.2 AutoEncoder Details

- **Architecture:** Symmetric encoder-decoder. Input -> h1 (Tanh) -> h2 (ReLU) -> latent (bottleneck, default dim=3) -> h2 (ReLU) -> h1 (Tanh) -> Output.
- **Training:** Per subsystem, on RUNNING data only. 300 epochs, Adam, MSE loss, batch 256. MinMaxScaler preprocessing.
- **Scoring:** Directional residual operator -- vibration sensors flag only increases (`pos`), temperature only increases (`pos`), electrical flags both directions (`both`). Per-sensor sigma-scaled errors summed then MinMax-normalized to [0,1].
- **Adaptive Threshold:** Inertial with gated adaptation (freezes when risk is anomalously high), slew-rate limiting (max change/step = 0.0005).
- **On/Off Delay Alarm:** State-machine: `on_delay=10` consecutive above-threshold minutes to activate, `off_delay=5` consecutive below-threshold minutes to deactivate.
- **Per-subsystem outputs:** `system_score` [0,1], `per_sensor_sigma` DataFrame, `threshold` array, `alarm` boolean series, `risk_raw`.

### 1.3 Engine A Changes

- **Current:** Selects sensors by `n_running`, `missing_pct`, `variance` filters. Includes `DER_KW_RES`.
- **New:** Uses ALL real sensors from catalog (including ISOLATED). No filtering. No derived signals. Downtime rows explicitly set to NaN.

### 1.4 Engine B Changes

- **Current:** Max 30 sensors, filtered from sensor_cfg.
- **New:** Max 50 sensors, selected from catalog. No `DER_KW_RES`.

### 1.5 Block Scoring / Fusion Changes

- **Current:** `system_score = max(topk_mean(A+B), PCA_mv_score)`. Fusion includes physics with fixed weight 0.15.
- **New:** `system_score = 0.5 * AE_system_score + 0.5 * topk_mean(A+B)`. ISOLATED sensors scored separately. No physics. Weights purely proportional to system size.

### 1.6 Risk Scoring Changes

- **Current:** Includes `risk_INSTRUMENT` using `max(physics, 1 - sqs_p10)`.
- **New:** No instrument risk component. Risk = weighted sum of system scores * confidence * gate.

---

## 2. Output File Changes

### 2.1 New Output Files

| File | Schema |
|------|--------|
| `detailed_system_sensor_scores.csv/.parquet` | Index: `timestamp_utc`. Cols: `downtime_flag`, `{SYS}__System_Score`, `{SYS}__{sensor}__Invalid_Flag`, `{SYS}__{sensor}__SQS`, `{SYS}__{sensor}__Engine_A`, `{SYS}__{sensor}__Engine_B` |
| `detailed_engine_a.csv/.parquet` | Index: `timestamp_utc`. Cols: `downtime_flag`, `{SYS}__{sensor}` (Engine A score) |
| `detailed_engine_b.csv/.parquet` | Index: `timestamp_utc`. Cols: `downtime_flag`, `{SYS}__{sensor}` (Engine B score) |
| `detailed_subsystem_scores.csv/.parquet` | Index: `timestamp_utc`. Cols: `downtime_flag`, `{SYS}__System_Score` |
| `detailed_sqs.csv/.parquet` | Index: `timestamp_utc`. Cols: `downtime_flag`, `{SYS}__{sensor}` (SQS per sensor) |
| `ae_model_metadata.csv` | Cols: `subsystem`, `n_sensors`, `input_dim`, `latent_dim`, `final_loss`, `risk_train_sigma`, `sensors_used` |

### 2.2 Modified Files

| File | Changes |
|------|---------|
| `scores.csv` | Added: `ae_score_{SYS_X}`, `ae_alarm_{SYS_X}`. Removed: `physics_score`, `pf_scale`, `vll_scale_for_identity`, `kw_expected`, `kw_residual` |
| `risk_sensor_decomposition.csv` | Added: `ae_system_score`, `sensor_score_component`. Removed: `is_virtual_sensor` |
| `alerts.csv` | Added: `ae_alarm_minutes` |
| `system_summary.csv` | Added: `Cohesion_C`, `Cond_Number_k`, `SVD_Status` |

### 2.3 Removed Files

- `df_chart_data.csv` (raw data export)
- `sensor_values_{SYS_X}.csv` (per-system raw value CSVs)

### 2.4 New Charts (PNG)

- `topological_stability.png` -- valid system count K vs. threshold with plateau
- `spectral_energy.png` -- singular values per system (cohesion/condition)
- `ae_training_loss.png` -- MSE loss curves per subsystem AE

---

## 3. New Dashboard Flow (from `new_plan.xlsx` + screenshots)

### 3.1 Flow Sequence (Top to Bottom)

1. **System Selected** -- dropdown showing "Shredder" (for now just one; later multiple). Shows data loaded confirmation.
2. **Sensors Found** -- total active sensor count (e.g., 50).
3. **Downtime Detected** -- stats card: e.g., "167 / 10,080 (1.7%) downtime, 9,913 (98.3%) running."
4. **Invalid Flagged** -- sensors with >60% NaN. Show as a report: S1=10min, S2=..., etc. (from bad mask sensors)
5. **Subsystem Detected** -- list discovered subsystems: SYS_1, SYS_2, ..., SYS_N, ISOLATED.
6. **Subsystem Sensor Quality Plots** -- selection buttons for each subsystem + ISOLATED. Per-sensor grid showing:
   - SQS trace (blue)
   - Engine A trace (orange)
   - Engine B trace (green)
   - Red shading = downtime
   - Values 0-1, stacked per sensor in a 3-column grid
7. **Subsystem Scores** -- stacked time series, one subplot per system (SYS_1, SYS_2, SYS_4 etc.), with red downtime shading.
8. **Rest KPI and Alerts as-is** -- existing alert cards, risk timeline, etc. unchanged.

### 3.2 Data Sources for New Charts

- Sensor quality grid: `detailed_sqs.csv`, `detailed_engine_a.csv`, `detailed_engine_b.csv` + `scores.csv` (for downtime)
- Subsystem scores stacked: `detailed_subsystem_scores.csv`
- Invalid sensors: `sensor_config.csv` or `detailed_system_sensor_scores.csv` (Invalid_Flag columns)
- Downtime: `scores.csv` -> `mode` column

---

## 4. Implementation Plan: Beta Dashboard

### 4.1 Approach: New Login Route "beta"

- Add a "Beta" button on the LoginPage (same design language)
- On beta login, render `DashboardBeta` instead of `Dashboard`
- All existing dashboard components remain untouched
- Beta dashboard uses new API endpoints prefixed `/api/beta/`

### 4.2 Backend Changes

New API endpoints (all under `/api/beta/`):

| Endpoint | Source File | Purpose |
|----------|------------|---------|
| `GET /api/beta/overview` | `scores.csv`, `sensor_config.csv`, `dynamic_catalog.csv` | System name, sensor count, downtime stats |
| `GET /api/beta/invalid_sensors` | `detailed_system_sensor_scores.csv` | Sensors with high invalid flag % |
| `GET /api/beta/subsystems` | `dynamic_catalog.csv`, `system_summary.csv` | Discovered subsystems with SVD stats |
| `GET /api/beta/sensor_quality/<system_id>` | `detailed_sqs.csv`, `detailed_engine_a.csv`, `detailed_engine_b.csv` | Per-sensor SQS/A/B time series for a subsystem |
| `GET /api/beta/subsystem_scores` | `detailed_subsystem_scores.csv` | System score time series for stacked chart |
| `GET /api/beta/ae_metadata` | `ae_model_metadata.csv` | AE model info per subsystem |

Plus reuse existing endpoints for alerts, risk timeline, etc.

### 4.3 Frontend New Components

| Component | Description |
|-----------|-------------|
| `DashboardBeta.js` | Main beta layout orchestrator |
| `BetaOverviewCards.js` | System selected, sensors found, downtime stats, invalid sensor report |
| `SubsystemDetected.js` | Visual display of discovered subsystems |
| `SensorQualityGrid.js` | Per-sensor SQS/A/B grid with subsystem selector buttons |
| `SubsystemScoresStacked.js` | Stacked time series chart of system scores |

### 4.4 Design Language Preservation

- Same glassmorphism cards (`GlassCard` pattern)
- Same aurora background
- Same green theme (#1B5E20 primary)
- Same Inter font
- Same section divider + left-border section wrap pattern
- Same Recharts for charts
- Same Framer Motion animations

### 4.5 Data Directory

The notebook outputs to `pipeline_outputs_PLAY/`. Backend beta endpoints should read from a configurable `BETA_DATA_DIR` (defaulting to `backend/data_beta/` or `backend/pipeline_outputs_PLAY/`).

---

## 5. Key Risks & Notes

- The notebook requires **PyTorch** -- new dependency for backend if we want to run the pipeline from the app.
- Physics engine removal means no `physics_score` in beta -- gauges and risk timeline need adjustment.
- `sensor_values_{SYS_X}.csv` files are removed in new pipeline -- the existing `/api/systems/<id>/sensors` endpoint will break for beta data. Use `detailed_*` files instead.
- INSTRUMENT classification removed -- any UI code referencing it needs a fallback.
- `is_virtual_sensor` column removed from risk decomposition -- check if any frontend code depends on it.
- The new `detailed_*` files use double-underscore naming (`SYS_1__SENSOR_NAME__Engine_A`) which needs parsing on the frontend or backend.
