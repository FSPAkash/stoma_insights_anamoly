# `1803.py` Beta Alerts Change Log

## Purpose

This document records the exact changes made to support beta alert artifacts from `backend/1803.py`, so the update can be reverted cleanly if needed.

This change was intentionally limited to backend artifact generation and beta API preload behavior.
No frontend files were changed in this update.

## Files Changed

### 1. `backend/1803.py`

Added beta alert generation derived from `1803.py` outputs.

### 2. `backend/app.py`

Updated beta preload lists so the beta API caches the new alert files from `backend/data_beta`.

## What Was Added In `backend/1803.py`

### New published beta files

`1803.py` now generates and publishes:

- `alerts.csv`
- `alerts.parquet`
- `alerts_sensor_level.csv`
- `alerts_sensor_level.parquet`

These are generated from `1803.py` subsystem alarms and sensor contribution outputs.

### Existing beta publish list expanded

The beta artifact publish step now mirrors:

- `alerts.csv`
- `alerts_sensor_level.csv`

into `backend/data_beta` alongside the rest of the beta artifact set.

### New helper function: `summarize_beta_episode_sensors(...)`

Purpose:

- summarize per-sensor contribution within an alert episode
- compute:
  - `sensor_peak_score`
  - `sensor_mean_score`
  - `sensor_rank`

Source data:

- `ae_results[sys_label]["sensor_contributions"]`

### New helper function: `build_beta_alert_tables(...)`

Purpose:

- derive episode-level alerts and sensor-level alert rows from `1803.py` outputs

Inputs:

- `df.index`
- `ae_results`
- `cfg.alert_min_duration_min`
- `cfg.alert_merge_gap_min`

Outputs:

- alert episode table
- sensor-level alert table

### New save block in `backend/1803.py`

The script now saves:

- `alerts.csv`
- `alerts.parquet`
- `alerts_sensor_level.csv`
- `alerts_sensor_level.parquet`

This happens after:

- subsystem alarms are built
- sensor rankings are built

and before the final verification / summary / beta publish step.

## Alert Logic Implemented

### Alert episodes

Alert episodes are derived per subsystem from:

- `{system} -> ae_results[sys_label]["alarm"]`

Logic:

1. Find contiguous `True` alarm regions
2. Merge gaps less than or equal to `cfg.alert_merge_gap_min`
3. Drop intervals shorter than `cfg.alert_min_duration_min`
4. Set alert severity from the subsystem `Score_Level_At_Alarm` values for that merged interval

### Alert class

For this implementation:

- `class = sys_label`

So the current alert classes are subsystem-native labels such as:

- `SYS_1`
- `SYS_2`
- etc.

This was chosen to keep the implementation aligned to `1803.py` outputs without introducing a second class-mapping layer.

### Threshold label

For this implementation:

- `threshold = "ADAPTIVE"`

Reason:

- the alert trigger comes from the subsystem AE alarm logic, which is based on adaptive thresholds
- this is not the same as the old fixed `MEDIUM` / `HIGH` episode-threshold generation path used in the standard pipeline

## Output Schemas Added

### `alerts.csv`

Columns:

- `start_ts`
- `end_ts`
- `duration_minutes`
- `severity`
- `class`
- `sensor_id`
- `sensor_max_score`
- `sensor_mean_score`
- `affected_sensor_count`
- `affected_sensors`
- `max_score`
- `mean_score`
- `threshold`

### `alerts_sensor_level.csv`

Columns:

- `start_ts`
- `end_ts`
- `duration_minutes`
- `severity`
- `class`
- `sensor`
- `sensor_rank`
- `sensor_peak_score`
- `sensor_mean_score`
- `alert_max_score`
- `alert_mean_score`
- `threshold`

## What Was Changed In `backend/app.py`

The beta preload lists were expanded to include:

- `alerts.csv`
- `alerts_sensor_level.csv`

This change was made in two places:

- import-time beta preload
- startup beta preload under `if __name__ == "__main__":`

No beta route behavior was changed in this step.

## What Was Not Changed

These were intentionally left untouched:

- no frontend beta components
- no beta alert cards UI
- no beta alert detail modal
- no risk decomposition generation
- no changes to the standard dashboard

This update only prepares the beta backend data layer for alert cards.

## Runtime Effect

After running `backend/1803.py`, the beta artifact folder should now contain:

- `alerts.csv`
- `alerts_sensor_level.csv`

assuming the pipeline produces at least the base outputs and the beta publish step runs successfully.

These files allow existing beta endpoints to return data:

- `/api/beta/alerts`
- `/api/beta/alerts_sensor_level`

They also allow beta summary counts in `/api/beta/dashboard/summary` to become meaningful.

## Known Limitations

### 1. No decomposition file yet

This change does not generate:

- `risk_sensor_decomposition.csv`

So a full standard-style beta alert detail modal is still not complete.

### 2. Alert class is subsystem-native

This change does not map alerts to:

- `PROCESS`
- `INSTRUMENT`
- `MECH`
- `ELEC`

Instead it keeps:

- `SYS_*`

which is the most direct fit for current `1803.py` outputs.

### 3. Threshold label is adaptive

This change uses:

- `threshold = "ADAPTIVE"`

instead of reproducing the standard fixed-threshold episode labels.

## How To Revert This Change

If you want to fully remove this update, do the following.

### Revert `backend/1803.py`

Remove:

- `alerts.csv` and `alerts_sensor_level.csv` from the `publish_beta_artifacts(...)` selected file list
- `summarize_beta_episode_sensors(...)`
- `build_beta_alert_tables(...)`
- the save block that writes:
  - `alerts.csv`
  - `alerts.parquet`
  - `alerts_sensor_level.csv`
  - `alerts_sensor_level.parquet`

### Revert `backend/app.py`

Remove from beta preload lists:

- `alerts.csv`
- `alerts_sensor_level.csv`

### Delete generated beta artifacts

Delete from:

- `backend/data_beta`

these files if they were created:

- `alerts.csv`
- `alerts_sensor_level.csv`

and optionally from timestamped output folders:

- `alerts.csv`
- `alerts.parquet`
- `alerts_sensor_level.csv`
- `alerts_sensor_level.parquet`

## Safe Rollback Order

Recommended rollback order:

1. revert `backend/app.py`
2. revert `backend/1803.py`
3. delete the generated alert files from `backend/data_beta`
4. rerun the pipeline if you want a fresh artifact set without these files

## Summary

This change adds beta-native alert artifact generation on top of `1803.py`.

It does:

- produce beta `alerts.csv`
- produce beta `alerts_sensor_level.csv`
- publish both into `backend/data_beta`
- let beta alert endpoints serve real data

It does not:

- add beta alert UI
- add beta decomposition
- change the standard dashboard
