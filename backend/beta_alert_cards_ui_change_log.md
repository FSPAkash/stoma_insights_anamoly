# Beta Alert Cards UI Change Log

## Purpose

This document records the exact frontend changes made to add beta alert cards and beta alert detail behavior to the beta dashboard.

It is written so the change can be reverted cleanly if needed.

## Scope

This update adds beta alert UI on top of the existing beta backend artifacts generated from `backend/1803.py`.

This update does not change:

- `backend/1803.py`
- beta alert CSV generation
- standard dashboard alert behavior

## Files Changed

### New files

- `frontend/src/components/BetaAlertEpisodeCards.js`
- `frontend/src/components/BetaAlertDetailModal.js`

### Updated files

- `frontend/src/components/DashboardBeta.js`
- `frontend/src/components/SubsystemBehaviorChartBeta.js`
- `frontend/src/components/SensorFlowDecomposition.js`

## What Was Added

## 1. Beta alert cards section

File:

- `frontend/src/components/BetaAlertEpisodeCards.js`

Added:

- a beta-specific alert card list modeled after the standard `AlertEpisodeCards.js`
- severity filter
- class filter
- recent vs severity sorting
- expandable card list
- empty state handling
- click-to-open behavior through `onSelectAlert`

Data used:

- beta `alerts.csv` served via `getBetaAlerts()`

## 2. Beta alert detail modal

File:

- `frontend/src/components/BetaAlertDetailModal.js`

Added:

- episode header and timestamp range
- severity, class, and threshold badges
- peak risk, mean risk, and top-sensor gauges
- episode metadata cards
- sensor-level alert table from `getBetaAlertsSensorLevel()`
- optional decomposition panel from `getBetaRiskDecompositionForEpisode()`

Important design choice:

- the modal does not open per-sensor detail views
- beta does not currently have a beta-specific sensor-detail endpoint

## 3. DashboardBeta integration

File:

- `frontend/src/components/DashboardBeta.js`

Added:

- `alerts` state
- `selectedAlert` state
- bootstrap load of `getBetaAlerts()`
- new "Alert Episodes" section under the beta dashboard
- beta alert modal render when an alert is selected

## 4. Chart-to-alert click-through

File:

- `frontend/src/components/SubsystemBehaviorChartBeta.js`

Added:

- `alerts` prop
- `onSelectAlert` prop
- click handling on alert bands
- matching logic from chart alarm bands back to the full alert row

Behavior:

- clicking inside a beta alarm band now opens the matching beta alert episode when a match is found

## 5. Shared decomposition component extension

File:

- `frontend/src/components/SensorFlowDecomposition.js`

Added:

- new optional prop: `interactive = true`

Behavior:

- standard dashboard behavior stays unchanged because the prop defaults to `true`
- beta passes `interactive={false}`
- this removes click affordance from the beta decomposition view without changing the standard dashboard

## Runtime Effect

After this update, the beta dashboard now supports:

- viewing beta alert episode cards
- filtering beta alerts by severity and class
- opening a beta alert detail modal from a card
- opening a beta alert detail modal from a subsystem alarm band
- viewing beta sensor-level alarm rows inside the modal
- optionally viewing beta decomposition if the backend returns flow data

## Known Limitations

## 1. No beta sensor detail modal

The beta modal does not open a deeper sensor-detail modal because there is no beta-specific sensor-detail API route today.

## 2. Decomposition is optional

If beta does not have `risk_sensor_decomposition.csv`, the modal still works, but the decomposition panel is omitted.

## 3. Card filtering follows selected day

The beta card list currently filters by selected day, matching the simpler behavior used in the standard alert cards.

## How To Revert This Change

## Revert step 1: remove beta alert components

Delete:

- `frontend/src/components/BetaAlertEpisodeCards.js`
- `frontend/src/components/BetaAlertDetailModal.js`

## Revert step 2: revert `DashboardBeta.js`

Remove:

- imports for the beta alert components
- `getBetaAlerts` import
- `alerts` state
- `selectedAlert` state
- beta alert loading inside `loadData()`
- the "Alert Episodes" section
- the modal render block
- `alerts` and `onSelectAlert` props passed into `SubsystemBehaviorChartBeta`

## Revert step 3: revert `SubsystemBehaviorChartBeta.js`

Remove:

- `alerts` prop
- `onSelectAlert` prop
- `handleChartClick(...)`
- `rawStart` and `rawEnd` fields inside `alarmBands`
- the `onClick={handleChartClick}` binding on `LineChart`

After this step, beta charts return to passive overlays only.

## Revert step 4: revert `SensorFlowDecomposition.js`

Remove:

- the `interactive` prop
- the conditional click text
- the conditional cursor override
- the guarded `onClick` behavior

This returns the shared component to its original always-clickable behavior.

## Safe Rollback Order

Recommended rollback order:

1. revert `frontend/src/components/DashboardBeta.js`
2. revert `frontend/src/components/SubsystemBehaviorChartBeta.js`
3. revert `frontend/src/components/SensorFlowDecomposition.js`
4. delete `frontend/src/components/BetaAlertEpisodeCards.js`
5. delete `frontend/src/components/BetaAlertDetailModal.js`

## Summary

This change adds the beta dashboard alert UI layer needed to expose the beta alert artifacts already being produced from the updated `1803.py` flow.

It does:

- add beta alert episode cards
- add a beta alert detail modal
- connect beta chart alarm bands to alert selection
- keep standard dashboard alert behavior intact

It does not:

- add a beta sensor detail endpoint
- change the standard dashboard
- change beta backend artifact generation
