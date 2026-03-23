# Beta Dashboard Performance Optimization - Change Log

**Date:** 2026-03-23
**Purpose:** Reduce initial load time and subsystem tab-switching latency on the beta dashboard.
**Safety:** All timeout guards, data sources, and Render keep-alive logic are untouched. No data points were removed -- all minute-by-minute values are preserved.

---

## How to Revert

All changes are confined to these files. To revert, restore these files from the commit prior to these changes:

```
git checkout HEAD~1 -- \
  backend/app.py \
  frontend/src/components/DashboardBeta.js \
  frontend/src/components/SensorQualityGrid.js \
  frontend/src/components/SubsystemBehaviorChartBeta.js
```

---

## Changes by File

### 1. `frontend/src/components/DashboardBeta.js`

**Added `useRef` import** for LazySection component.

**Added `getBetaSubsystems` to parent-level data load:**
- Both chart components previously called `getBetaSubsystems()` independently on mount (2 duplicate calls).
- Now fetched once in `DashboardBeta.loadData()` via `Promise.allSettled` alongside the existing summary and timeseries calls.
- Passed down as `subsystems` prop to both `SensorQualityGrid` and `SubsystemBehaviorChartBeta`.

**Added `LazySection` component:**
- Simple `IntersectionObserver`-based wrapper (200px rootMargin).
- Wraps `SubsystemBehaviorChartBeta` so it only mounts and fires API calls when scrolled into view.
- Shows a "Scroll to load..." placeholder until visible.

---

### 2. `frontend/src/components/SensorQualityGrid.js`

**Data source: NO CHANGE.** Still fetches from `/api/beta/sensor_quality/{systemId}` which reads `detailed_sqs.csv`, `detailed_engine_a.csv`, `detailed_engine_b.csv`, `detailed_subsystem_scores.csv`, and `detailed_subsystem_alarms.csv`.

**Removed `getBetaSubsystems` import and self-fetch:**
- Now receives `subsystems` as a prop from `DashboardBeta`.
- Local `useEffect` just sets initial `selectedSystem` from props.

**Decoupled alarm view from data fetch:**
- `loadQuality` callback no longer has `alarmView` in its dependency array.
- Previously, toggling between "Minute Windows" and "Alarm Spans" re-fetched the entire timeseries (~10K rows). Now it only re-fetches the lightweight `getBetaAlerts` call.
- `alarmBands` useMemo now derives overlays from the `alerts` state (fetched via `getBetaAlerts`) filtered by `selectedSystem`, instead of from `qualityData.alarm_bands`.

**Added client-side cache (`qualityCacheRef`):**
- `useRef({})` map keyed by `systemId`.
- On first fetch for a system, data is stored in the ref.
- Switching back to a previously loaded system is instant (no API call).

---

### 3. `frontend/src/components/SubsystemBehaviorChartBeta.js`

**Data source: NO CHANGE.** Still fetches from `/api/beta/subsystem_behavior/{systemId}` which reads `df_chart_data.csv` (or `sensor_values_{systemId}.csv` fallback) and `detailed_subsystem_alarms.csv`.

**Removed `getBetaSubsystems` import and self-fetch:**
- Now receives `subsystems` as a prop from `DashboardBeta`.
- ISOLATED subsystem filtering (`s.system_id !== 'ISOLATED'`) preserved via `useMemo` on the prop.

**Decoupled alarm view from data fetch:**
- Same pattern as SensorQualityGrid: `loadData` no longer depends on `alarmView`.
- `alarmBands` useMemo now derives from `alerts` state filtered by `selectedSystem`.

**Added client-side cache (`behaviorCacheRef`):**
- Same pattern as SensorQualityGrid: `useRef({})` map keyed by `systemId`.

---

### 4. `backend/app.py`

**`beta_sensor_quality` endpoint -- vectorized:**
- Replaced the row-by-row Python `for` loop (iterating 10K indices with `.iloc[i]` per sensor per metric) with vectorized pandas column selection.
- Builds a single `combined` DataFrame with renamed columns, then calls `sanitize_df()` + `to_dict("records")` once.
- Same output shape: `{sensor}__sqs`, `{sensor}__a`, `{sensor}__b`, `ts`, `downtime` fields per row.
- Same data sources: `detailed_sqs.csv`, `detailed_engine_a.csv`, `detailed_engine_b.csv`, `detailed_subsystem_alarms.csv`, `detailed_subsystem_scores.csv`.
- `alarm_bands` now returned as empty array `[]` (frontend derives these from `/api/beta/alerts`).

**`beta_subsystem_behavior` endpoint:**
- Removed `alarm_view` variable and `_beta_build_alert_tables_for_view` call.
- `alarm_bands` now returned as empty array `[]` (frontend derives these from `/api/beta/alerts`).
- Timeseries data construction is untouched -- still uses `df_chart_data.csv` / `sensor_values_{systemId}.csv`.

---

## Performance Impact Summary

| Bottleneck | Before | After |
|---|---|---|
| Initial API calls | 6+ parallel (2x subsystems, 2x alerts, quality, behavior) | 4 (1x subsystems lifted to parent, behavior deferred) |
| Subsystem tab switch | Full re-fetch of ~10K row timeseries | Instant from cache |
| Alarm view toggle | Full re-fetch of ~10K row timeseries | Only re-fetches lightweight alerts endpoint |
| `beta_sensor_quality` serialization | Row-by-row Python loop with `.iloc` | Vectorized pandas `to_dict("records")` |
| SubsystemBehavior on load | Fetches immediately on mount | Deferred until scrolled into view |

---

## What Was NOT Changed

- Axios timeout (120s) in `frontend/src/utils/api.js`
- Render keep-alive ping in `backend/app.py`
- CSV pre-loading at import time in `backend/app.py`
- All data sources and CSV file paths
- All minute-by-minute data point granularity (downsample=1 preserved)
- `getBetaAlerts` endpoint (unchanged)
- `BetaOverviewCards`, `BetaAlertEpisodeCards`, `BetaAlertDetailModal` (unchanged)
- Time filter logic and zoom behavior (unchanged)
