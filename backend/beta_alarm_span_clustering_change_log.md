# Beta Alarm Span Clustering Change Log

## Purpose

Document the addition of user-selectable alarm spans on top of the existing minute-level alarm view.

This change keeps minute-level alarms intact and adds a second runtime span view without modifying `backend/1803.py`.

## Scope

This update adds:

- dual alarm views:
  - `minute`
  - `span`
- a shared dashboard toggle to switch between them
- dynamic contiguous span clustering on the API side
- span-aware alarm cards, detail modal, subsystem behavior overlays, and signal quality overlays

## Files Changed

- `backend/app.py`
- `frontend/src/utils/api.js`
- `frontend/src/components/DashboardBeta.js`
- `frontend/src/components/BetaAlertEpisodeCards.js`
- `frontend/src/components/BetaAlertDetailModal.js`
- `frontend/src/components/SubsystemBehaviorChartBeta.js`
- `frontend/src/components/SensorQualityGrid.js`

## Backend Changes

File:

- `backend/app.py`

### 1. Minute-level alert rows were preserved

The existing runtime minute-level alert builder remains the source-of-truth base.

Minute rows still represent:

- one active alarm timestamp
- `start_ts == end_ts`
- `duration_minutes = 1`

### 2. Added dynamic span builder

New runtime span behavior was added on top of the minute rows.

Span logic:

- group minute rows by subsystem
- sort by timestamp
- collapse contiguous alarm minutes into one span
- do not create fixed-size spans

This means:

- five consecutive alarm minutes become one five-minute span
- twelve consecutive alarm minutes become one twelve-minute span

### 3. Span rows do not roll up to one source severity

Instead of inventing a single source severity, span rows now carry:

- `minute_count`
- `high_count`
- `medium_count`
- `low_count`
- `severity_mix`

For rendering convenience, a display-level `severity` field is also emitted:

- `HIGH`
- `MEDIUM`
- `LOW`
- `MIXED`

`MIXED` is only a UI/display label for mixed spans. The real severity detail remains in the count fields above.

### 4. Added view selection on beta endpoints

These routes now accept:

- `alarm_view=minute|span`

Updated routes:

- `/api/beta/alerts`
- `/api/beta/alerts_sensor_level`
- `/api/beta/dashboard/summary`
- `/api/beta/subsystem_behavior/<system_id>`
- `/api/beta/sensor_quality/<system_id>`

Behavior:

- `minute` returns source minute alarm rows
- `span` returns dynamic contiguous span rows

### 5. Alarm bands now carry richer metadata

Chart alarm-band payloads now include:

- `view_type`
- `minute_count`
- `severity_mix`
- `high_count`
- `medium_count`
- `low_count`

## Frontend Changes

### 1. Added shared dashboard alarm-view toggle

File:

- `frontend/src/components/DashboardBeta.js`

New UI control:

- `Minute Windows`
- `Alarm Spans`

This toggle drives:

- the alarm cards
- the subsystem behavior chart
- the signal quality chart
- the detail modal context

### 2. Cards now support both minute rows and spans

File:

- `frontend/src/components/BetaAlertEpisodeCards.js`

Added behavior:

- minute mode still shows one alarm row per active minute
- span mode shows one card per contiguous alarm span
- span cards show:
  - alert count in the span
  - severity mix such as `7 high, 13 low`
- severity filters in span mode now match contained severities
- `MIXED` appears as an extra filter only in span mode

### 3. Detail modal now supports both views

File:

- `frontend/src/components/BetaAlertDetailModal.js`

Added behavior:

- minute mode still describes one alarm row
- span mode shows:
  - span duration
  - alert count in span
  - severity mix
  - aggregated sensor contribution rows across the span

### 4. Both charts now support both views

Files:

- `frontend/src/components/SubsystemBehaviorChartBeta.js`
- `frontend/src/components/SensorQualityGrid.js`

Added behavior:

- minute mode keeps widened one-minute alarm overlays
- span mode renders full contiguous span overlays
- tooltips now show span alert counts and severity mix when applicable
- mixed spans render with their own visual style

### 5. API helpers now pass alarm-view params

File:

- `frontend/src/utils/api.js`

Updated helpers:

- `getBetaAlerts`
- `getBetaDashboardSummary`
- `getBetaSubsystemBehavior`
- `getBetaSensorQuality`

## Verification Performed

### Build checks

- `python -m py_compile backend/app.py`
- `cmd /c npm run build`

### Runtime checks

Confirmed with Flask test client:

- `/api/beta/alerts?alarm_view=minute`
- `/api/beta/alerts?alarm_view=span`
- `/api/beta/alerts_sensor_level?alarm_view=span`
- `/api/beta/subsystem_behavior/SYS_1?alarm_view=span`
- `/api/beta/sensor_quality/SYS_1?alarm_view=span`

Observed result:

- minute mode returned `468` minute rows
- span mode returned `40` contiguous spans
- sample span rows correctly included severity breakdowns like `7 high, 13 low`

## What Was Not Changed

- `backend/1803.py`
- beta source artifact generation on disk
- source minute-level alarm truth

## Revert Plan

If this whole feature needs to be reverted:

1. Revert span-building helpers and `alarm_view` routing logic in `backend/app.py`
2. Revert beta API helper param support in `frontend/src/utils/api.js`
3. Remove the dashboard alarm-view toggle in `frontend/src/components/DashboardBeta.js`
4. Revert span-specific rendering in:
   - `frontend/src/components/BetaAlertEpisodeCards.js`
   - `frontend/src/components/BetaAlertDetailModal.js`
   - `frontend/src/components/SubsystemBehaviorChartBeta.js`
   - `frontend/src/components/SensorQualityGrid.js`

## Safe Rollback Order

Recommended order:

1. `backend/app.py`
2. `frontend/src/utils/api.js`
3. `frontend/src/components/DashboardBeta.js`
4. `frontend/src/components/SubsystemBehaviorChartBeta.js`
5. `frontend/src/components/SensorQualityGrid.js`
6. `frontend/src/components/BetaAlertEpisodeCards.js`
7. `frontend/src/components/BetaAlertDetailModal.js`

## Summary

This change adds a second UI/runtime alert mode:

- source minute-level windows remain available
- dynamic contiguous spans are now also available

The user can switch between them without changing the underlying source-of-truth alarm data.
