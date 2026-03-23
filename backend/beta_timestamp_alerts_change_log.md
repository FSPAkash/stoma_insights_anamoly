# Beta Timestamp Alerts Change Log

## Purpose

This document records the change that moved the beta alert experience to the native source-of-truth alarm output:

- timestamp-level subsystem alarm rows
- derived at API/runtime from `backend/data_beta/detailed_subsystem_alarms.csv`

This change was made without modifying `backend/1803.py`.

## Source Of Truth

The source of truth for beta alarms in this change is:

- `backend/data_beta/detailed_subsystem_alarms.csv`

Supporting timestamp-level context is read from:

- `backend/data_beta/detailed_system_sensor_scores.csv`
- `backend/data_beta/detailed_sensor_rankings.csv`

## Why This Change Was Made

The previous beta UI was built around merged alert rows from:

- `alerts.csv`
- `alerts_sensor_level.csv`

That caused the UI to hide low-level alarm timestamps whenever a merged interval also contained a high-level timestamp.

This change removes that mismatch by driving beta alerts directly from timestamp-level subsystem alarm rows.

## Files Changed

### Backend

- `backend/app.py`

### Frontend

- `frontend/src/components/DashboardBeta.js`
- `frontend/src/components/BetaAlertEpisodeCards.js`
- `frontend/src/components/BetaAlertDetailModal.js`
- `frontend/src/components/SubsystemBehaviorChartBeta.js`

## Backend Changes

## 1. Added timestamp-level beta alert builder in `backend/app.py`

Added helper functions that:

- read `detailed_subsystem_alarms.csv`
- read matching timestamp rows from `detailed_system_sensor_scores.csv`
- read matching timestamp rows from `detailed_sensor_rankings.csv`
- emit timestamp-level alert rows and timestamp-level sensor rows at request time

Key behavior:

- one alert row per active alarm timestamp
- `start_ts == end_ts == timestamp`
- `duration_minutes = 1`
- `severity` comes from `Score_Level_At_Alarm`
- `class` comes from the subsystem label such as `SYS_1`

## 2. Updated `/api/beta/alerts`

The beta alerts endpoint no longer uses `alerts.csv`.

It now returns timestamp-level rows derived from:

- `detailed_subsystem_alarms.csv`

## 3. Updated `/api/beta/alerts_sensor_level`

The beta sensor-level endpoint no longer uses `alerts_sensor_level.csv`.

It now returns timestamp-level sensor rows derived from:

- `detailed_sensor_rankings.csv`

for the selected alarm timestamp and subsystem.

## 4. Updated `/api/beta/dashboard/summary`

The beta dashboard summary alert counts now come from timestamp-level alert rows instead of merged alert episodes.

This means:

- `total_alerts`
- `high_alerts`
- `medium_alerts`
- `low_alerts`

now reflect timestamp-level alarm rows.

## 5. Updated beta subsystem behavior overlays

The subsystem behavior route now returns timestamp-level alarm markers instead of merged alert episode spans.

This keeps the chart aligned with the same source-of-truth rows used by the cards.

## Frontend Changes

## 1. Cards now represent timestamp-level alarms

`BetaAlertEpisodeCards.js` now renders timestamp-level system alarm rows.

Behavior changes:

- cards no longer represent merged episodes
- card titles and tooltip copy were updated to say "System Alarms"
- selected-day filtering now filters timestamp-level rows

## 2. Modal now represents one alarm row

`BetaAlertDetailModal.js` now presents a single system alarm row instead of a merged episode.

Behavior changes:

- title updated to "System Alarm Detail"
- single-timestamp subtitle when `start_ts == end_ts`
- sensor-level table now reflects one timestamp-level alarm row

## 3. Chart overlays now reflect timestamp-level alarms

`SubsystemBehaviorChartBeta.js` now renders timestamp-level alarm markers.

Behavior changes:

- single-timestamp alarms are widened slightly on the x-axis for visibility
- chart colors still reflect source severity
- chart click-through now targets timestamp-level alarm rows

## What Was Not Changed

- `backend/1803.py` was not modified for this source-of-truth shift
- `alerts.csv` generation inside `1803.py` was not changed
- `alerts_sensor_level.csv` generation inside `1803.py` was not changed

Those files may still exist on disk, but beta UI/runtime alert behavior no longer depends on them.

## Runtime Effect

After this change, the beta UI alert flow should reflect:

- low alarm timestamps
- medium alarm timestamps
- high alarm timestamps

directly from the source alarm file, rather than from merged episode rows.

## How To Revert This Change

## Revert backend

In `backend/app.py`:

- remove the timestamp-level beta alert helper functions
- revert `/api/beta/alerts` to read `alerts.csv`
- revert `/api/beta/alerts_sensor_level` to read `alerts_sensor_level.csv`
- revert `/api/beta/dashboard/summary` to count alerts from `alerts.csv`
- revert `beta_subsystem_behavior` alarm bands to use merged alert rows

## Revert frontend

In the frontend files:

- rename card/detail copy back from "System Alarms" to the previous wording if desired
- remove the single-timestamp display behavior
- remove the chart single-point widening for alarm markers

## Safe Rollback Order

Recommended order:

1. revert `backend/app.py`
2. revert `frontend/src/components/SubsystemBehaviorChartBeta.js`
3. revert `frontend/src/components/BetaAlertEpisodeCards.js`
4. revert `frontend/src/components/BetaAlertDetailModal.js`
5. revert `frontend/src/components/DashboardBeta.js`

## Summary

This change moves beta alarms to the native source-of-truth output:

- timestamp-level subsystem alarm rows

It removes the previous dependency on merged beta alert rows for the runtime UI path.
