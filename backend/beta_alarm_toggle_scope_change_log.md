# Beta Alarm Toggle Scope Change Log

## Purpose

Document the follow-up change that moved the alarm-view toggle off the dashboard shell and into the alert cards and chart components.

## Why This Follow-Up Was Needed

The first implementation placed `alarmView` state in `DashboardBeta.js`.

That caused:

- top-level dashboard data reloads when the toggle changed
- full-page loading behavior
- extra refetches beyond the widgets that actually needed the selected alarm view

## What Changed

### 1. Removed dashboard-level alarm-view state

File:

- `frontend/src/components/DashboardBeta.js`

Removed:

- shared page-level `alarmView` state
- dashboard-level `Minute Windows / Alarm Spans` toggle UI
- alert-view-dependent top-level reload behavior

Dashboard data loading now stays focused on:

- summary
- score timeseries

### 2. Moved alarm-view toggle into the alert cards

File:

- `frontend/src/components/BetaAlertEpisodeCards.js`

Added:

- local `alarmView` state
- local fetches to `/api/beta/alerts?alarm_view=...`
- widget-scoped toggle UI

Effect:

- switching alarm view only reloads the cards

### 3. Moved alarm-view toggle into subsystem behavior

File:

- `frontend/src/components/SubsystemBehaviorChartBeta.js`

Added:

- local `alarmView` state
- local fetches for:
  - subsystem behavior data
  - matching alert rows for click-through
- widget-scoped toggle UI

Effect:

- switching alarm view only reloads the subsystem behavior chart

### 4. Moved alarm-view toggle into sensor quality

File:

- `frontend/src/components/SensorQualityGrid.js`

Added:

- local `alarmView` state
- local fetches to the sensor-quality route with `alarm_view`
- widget-scoped toggle UI

Effect:

- switching alarm view only reloads the signal quality chart

## Files Changed

- `frontend/src/components/DashboardBeta.js`
- `frontend/src/components/BetaAlertEpisodeCards.js`
- `frontend/src/components/SubsystemBehaviorChartBeta.js`
- `frontend/src/components/SensorQualityGrid.js`

## What Did Not Change

- `backend/app.py`
- `backend/1803.py`
- span-building logic
- alert detail modal behavior

## Verification

Commands run after this follow-up:

- `python -m py_compile backend/app.py`
- `cmd /c npm run build`

## Revert Plan

If you need to undo this follow-up only:

1. Restore page-level alarm-view state in `frontend/src/components/DashboardBeta.js`
2. Remove local alarm-view state from:
   - `frontend/src/components/BetaAlertEpisodeCards.js`
   - `frontend/src/components/SubsystemBehaviorChartBeta.js`
   - `frontend/src/components/SensorQualityGrid.js`

## Summary

The alarm-view toggle now lives at the widget level instead of the dashboard level, so switching between minute windows and alarm spans no longer reloads the full dashboard.
