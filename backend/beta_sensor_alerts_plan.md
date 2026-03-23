# Beta Alert Cards Implementation Plan

## Goal

Add alert cards to the beta dashboard using the standard dashboard as the reference interaction model, while staying aligned with the updated `backend/1803.py` artifact set.

This plan is intentionally split into:

- Phase 1: beta alert cards and selection flow
- Phase 2: beta alert detail modal
- Phase 3: optional decomposition parity

## Source Of Truth

For this beta path, the source of truth is now:

- `backend/1803.py`
- `backend/data_beta`

The beta dashboard should not depend on files from the standard `backend/data` folder for alert cards.

## What The Standard Dashboard Does Today

The standard dashboard alert experience is driven by:

- `frontend/src/components/Dashboard.js`
- `frontend/src/components/AlertEpisodeCards.js`
- `frontend/src/components/AlertDetailModal.js`
- `frontend/src/components/SubsystemBehaviorChart.js`

Behavior pattern:

1. `Dashboard.js` loads `getAlerts()`
2. It stores `alerts` and `selectedAlert`
3. `AlertEpisodeCards` renders filterable alert cards
4. Clicking a card opens `AlertDetailModal`
5. `AlertDetailModal` loads:
   - `/api/alerts_sensor_level`
   - `/api/risk_decomposition/episode`
6. `SubsystemBehaviorChart` can also open an alert by clicking an alert band

This is the exact interaction model beta should mirror first.

## Current Beta State

Beta already has:

- `getBetaAlerts()`
- `getBetaAlertsSensorLevel()`
- `getBetaRiskDecompositionForEpisode()`
- backend alert endpoints under `/api/beta/...`
- beta raw trace overlays in the same `backend/data_beta` folder

Beta dashboard currently has no alert UI:

- no `alerts` state in `DashboardBeta.js`
- no `selectedAlert` state
- no alert cards section
- no beta alert modal
- no alert-band click-through in `SubsystemBehaviorChartBeta.js`

## Fit With The Updated `1803.py`

This plan is suited to the updated `1803.py`.

Backend support already added:

- beta-native `alerts.csv`
- beta-native `alerts_sensor_level.csv`
- both published into `backend/data_beta`

Important limitation:

- `risk_sensor_decomposition.csv` is still not part of the guaranteed `1803.py` beta artifact set

So the plan should treat decomposition as optional for the first frontend pass.

## Recommended Build Strategy

## Phase 1: Add Beta Alert Cards To `DashboardBeta.js`

Primary files:

- `frontend/src/components/DashboardBeta.js`
- `frontend/src/utils/api.js`

Changes:

- load `getBetaAlerts()` during beta dashboard bootstrap
- add `alerts` state
- add `selectedAlert` state
- render an alert section below `Subsystem Behavior`
- pass `alerts` and `onSelectAlert` down where needed

Why this order:

- it gives immediate value with the least UI risk
- it matches the standard dashboard page flow closely
- it uses data that now exists in `backend/data_beta`

## Phase 2: Add A Beta Alert Card Component

Recommended file:

- `frontend/src/components/BetaAlertEpisodeCards.js`

Suggested approach:

- start from `AlertEpisodeCards.js`
- keep the same filter model:
  - severity
  - class
  - sort
- keep the same empty-state behavior
- keep the same card fields:
  - `start_ts`
  - `end_ts`
  - `duration_minutes`
  - `severity`
  - `class`
  - `max_score`
  - `mean_score`
  - `sensor_id`
  - `affected_sensors`

Why a beta-specific component first:

- beta terminology and tooltips are slightly different
- `1803.py` uses subsystem-native classes like `SYS_1`
- we can tune copy for beta without risking regressions in standard

## Phase 3: Add A Beta Alert Detail Modal

Recommended file:

- `frontend/src/components/BetaAlertDetailModal.js`

Suggested approach:

- start from `AlertDetailModal.js`
- keep the same shell and card layout
- keep the same metadata blocks and sensor-level table
- call beta APIs only:
  - `getBetaAlertsSensorLevel({ start_ts, end_ts, class })`
  - optionally `getBetaRiskDecompositionForEpisode(start_ts, end_ts)`

Recommended MVP for the modal:

- episode header
- severity and class badges
- duration / peak / mean summary
- top sensor summary
- sensor-level alert table

Recommended second pass:

- only show decomposition if beta decomposition data exists
- otherwise render the modal without `SensorFlowDecomposition`

This avoids blocking the alert cards feature on a backend artifact that is not yet guaranteed.

## Phase 4: Connect Subsystem Alert Bands To The Modal

Primary file:

- `frontend/src/components/SubsystemBehaviorChartBeta.js`

Changes:

- accept `alerts`
- accept `onSelectAlert`
- when clicking an alert band, match to the full alert object by:
  - `start_ts`
  - `end_ts`
  - `class` when available

This should mirror the standard chart behavior in `SubsystemBehaviorChart.js`.

This is not the first dependency, but it is strongly recommended for parity.

## Reuse Strategy

Recommended reuse model:

- use the standard dashboard components as implementation references
- do not directly swap beta onto standard components yet
- build beta-specific wrappers or copies first

Components to use as direct inspiration:

- `AlertEpisodeCards.js`
- `AlertDetailModal.js`
- `SubsystemBehaviorChart.js`

Reason:

- beta still has different backend guarantees
- the modal has a decomposition dependency that standard assumes
- beta class semantics are subsystem-first (`SYS_*`)

## Data Contract For The Beta UI

## `alerts.csv`

Expected fields used by the beta cards:

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

## `alerts_sensor_level.csv`

Expected fields used by the beta modal:

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

This is the current output of the sensor alarms for beta.

## UX Decisions To Keep

To stay close to the standard dashboard, beta should keep:

- filter pills for severity and class
- sort by recent vs severity
- compact alert cards with a risk bar
- click-to-open modal behavior
- empty states instead of blank sections
- top-sensor emphasis in both card and modal

## UX Decisions To Change Slightly For Beta

Recommended beta-specific adjustments:

- tooltips should describe subsystem-native alert classes rather than legacy class families
- card copy should say "alert episode" or "system alert" consistently with beta charts
- the modal should avoid promising decomposition if it is not present
- the chart legend should use the same alert naming as the cards

## Implementation Checklist

## Frontend

- add `getBetaAlerts()` load to `DashboardBeta.js`
- add `alerts` state to `DashboardBeta.js`
- add `selectedAlert` state to `DashboardBeta.js`
- create `BetaAlertEpisodeCards.js`
- render the beta alert cards section
- create `BetaAlertDetailModal.js`
- wire card click to modal open
- pass `alerts` and `onSelectAlert` to `SubsystemBehaviorChartBeta.js`
- wire alert-band click-through in `SubsystemBehaviorChartBeta.js`
- gate decomposition rendering behind available beta data

## QA

- verify cards render when beta alerts exist
- verify empty state when no alerts exist
- verify severity and class filters work
- verify sort behavior matches the standard dashboard pattern
- verify clicking a card opens the correct beta alert
- verify sensor-level rows match the selected alert window
- verify the chart can open the same alert from an overlay band
- verify the beta page still behaves correctly on mobile widths

## Suggested Build Order For The Next Agent

1. Update `DashboardBeta.js` to load beta alerts and hold selection state
2. Build `BetaAlertEpisodeCards.js` from `AlertEpisodeCards.js`
3. Build `BetaAlertDetailModal.js` from `AlertDetailModal.js`
4. Integrate the new section into the beta page
5. Add chart click-through in `SubsystemBehaviorChartBeta.js`
6. Add decomposition only if the beta artifact is available

## Non-Goals For The First Pass

Do not block the release on:

- shared refactors between standard and beta alert components
- risk decomposition parity
- reclassification from `SYS_*` into higher-level legacy categories

The first pass should focus on making the updated `1803.py` outputs visible and useful in the beta dashboard.
