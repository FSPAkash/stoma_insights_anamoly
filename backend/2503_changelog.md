# 2503.py Changelog -- Beta Dashboard Parity with 1803.py

## Date: 2025-03-25

## Problem
2503.py (new pipeline version) was missing several output files required by the beta dashboard (`app.py`), and had no mechanism to publish artifacts to `data_beta/`.

## Changes Made

### 1. Added `import shutil`
Required by the `publish_beta_artifacts()` function for copying files.

### 2. Added `beta_publish_dir` config field
```python
beta_publish_dir: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_beta")
```
Was present in 1803.py but missing from 2503.py's `PipelineConfig` dataclass.

### 3. Added `build_sensor_config_export()` function
Augments the sensor baseline config with runtime missingness stats (`n_running`, `n_present_running`, `missing_pct`). Produces `sensor_config.csv`.

### 4. Added `export_beta_chart_inputs()` function
Exports cleaned minute-grid sensor data for the beta subsystem behavior charts:
- `df_chart_data.csv` -- full wide table with timestamp_utc
- `sensor_values_{system}.csv` -- one file per subsystem

### 5. Added `publish_beta_artifacts()` function
Mirrors the entire `output_dir` into `data_beta/` via `shutil.copytree`. Previous approach used a hardcoded whitelist of filenames which missed outputs like `initial_sensor_validation_report.csv`, `sensor_trust_summary.csv`, parquet files, and all new 2503 outputs. Now every file the pipeline produces gets published automatically -- no maintenance needed when new outputs are added.

### 6. Added `dynamic_weights.csv` export
Writes fusion and risk weights per subsystem -- required by beta dashboard endpoints.

### 7. Added `sensor_config.csv` export
Calls `build_sensor_config_export()` and writes the result.

### 8. Added alert episode tables
Ported `summarize_beta_episode_sensors()` and `build_beta_alert_tables()` from 1803.py. These produce:
- `alerts.csv` / `alerts.parquet` -- alert episodes with severity, duration, top sensor
- `alerts_sensor_level.csv` / `alerts_sensor_level.parquet` -- per-sensor contributions within each episode

### 9. Moved beta publish call to end of pipeline (Cell 19)
The publish call now runs as the final cell, after standalone scoring (Cell 16) and process risk fusion (Cell 18) have completed. This ensures `standalone_outputs/` and `process_risk_outputs/` subdirectories exist and get copied to `data_beta/`.

## Files Previously Missing (now added)
| File | Purpose |
|------|---------|
| `sensor_config.csv` | Per-sensor config with missingness stats |
| `dynamic_weights.csv` | Fusion/risk weights per subsystem |
| `df_chart_data.csv` | Full sensor data for behavior charts |
| `sensor_values_{system}.csv` | Per-subsystem sensor subsets for charts |
| `alerts.csv` | Alert episode table |
| `alerts_sensor_level.csv` | Per-sensor alert contributions |
| Beta publish to `data_beta/` | All above + existing files mirrored |
| `standalone_outputs/` (entire dir) | Standalone sensor scoring outputs mirrored to `data_beta/` |
| `process_risk_outputs/` (entire dir) | Process risk fusion outputs mirrored to `data_beta/` |

## Files Kept As-Is (new in 2503, not in 1803) -- now also published to data_beta
- `standalone_outputs/standalone_evidence.csv/parquet`
- `standalone_outputs/standalone_scores.csv/parquet`
- `standalone_outputs/standalone_alarms.csv/parquet`
- `standalone_outputs/standalone_summary.csv`
- `process_risk_outputs/process_risk.csv/parquet`
- `process_risk_outputs/process_risk_dominant.csv/parquet`
- `process_risk_outputs/process_risk_summary.csv`
- `ae_decision_boundary.png`
- `ae_radar_fingerprints.png`
