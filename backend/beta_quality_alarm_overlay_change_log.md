# Beta Quality Alarm Overlay Change Log

## Purpose

Document the alarm-overlay work added to the signal quality charts so the change can be reviewed or reverted cleanly.

## Scope

This change adds timestamp-level system alarm overlays to the beta signal quality charts without changing `backend/1803.py`.

## Files Changed

- `backend/app.py`
- `frontend/src/components/SensorQualityGrid.js`
- `frontend/src/components/SubsystemBehaviorChartBeta.js`
- `frontend/src/components/BetaAlertEpisodeCards.js`
- `frontend/src/components/BetaAlertDetailModal.js`

## What Changed

### 1. Backend: sensor quality route now returns alarm bands

File: `backend/app.py`

Route updated:

- `/api/beta/sensor_quality/<system_id>`

Added behavior:

- Empty responses now include:
  - `subsystem_timeseries`
  - `timestamp_col`
  - `downtime_bands`
  - `alarm_bands`
- Non-empty responses now include `alarm_bands`
- `alarm_bands` are derived from `_beta_build_timestamp_alert_tables()`
- Bands are filtered to the requested subsystem via `class == system_id`
- Each band row contains:
  - `start`
  - `end`
  - `severity`

No changes were made to:

- `backend/1803.py`
- Source beta CSV generation

### 2. Frontend: signal quality chart now supports alarm overlays

File: `frontend/src/components/SensorQualityGrid.js`

Added behavior:

- Added `Show Alarms` toggle to the chart controls
- Added alarm overlay rendering with `ReferenceArea`
- Added alarm badges in the tooltip
- Added alarm entries to both subsystem-view and sensor-view legends
- Added frontend mapping from API `alarm_bands` to chart indices
- Single-timestamp alarms are widened slightly for visibility

### 3. Visual consistency: low alarms now render in yellow

Files:

- `frontend/src/components/SensorQualityGrid.js`
- `frontend/src/components/SubsystemBehaviorChartBeta.js`
- `frontend/src/components/BetaAlertEpisodeCards.js`
- `frontend/src/components/BetaAlertDetailModal.js`

Updated behavior:

- `HIGH` stays red
- `MEDIUM` stays amber/yellow
- `LOW` now uses a yellow family instead of green

This was done so low alarms display consistently across:

- system behavior chart
- signal quality chart
- alarm cards
- alarm detail modal

## Verification Run

Commands used after the change:

- `python -m py_compile backend/app.py`
- `cmd /c npm run build`

## Revert Plan

If you need to revert only the signal quality alarm overlay work:

1. Revert the `beta_sensor_quality` route changes in `backend/app.py`
2. Remove `showAlarms`, `alarmBands`, tooltip alarm labels, legend alarm items, and `ReferenceArea` alarm overlays from `frontend/src/components/SensorQualityGrid.js`

If you also want to undo the low-is-yellow display change:

3. Revert the low-severity color mappings in:
   - `frontend/src/components/SubsystemBehaviorChartBeta.js`
   - `frontend/src/components/BetaAlertEpisodeCards.js`
   - `frontend/src/components/BetaAlertDetailModal.js`

## Notes

- This change is runtime-only on the API/UI side
- It does not alter beta source artifacts on disk
- It does not modify `1803.py`
