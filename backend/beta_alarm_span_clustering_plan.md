# Beta Alarm Span Clustering Plan

## Goal

Keep the current source-of-truth minute-level alarm rows, and add an optional clustered span view that:

- groups nearby minute-level alarm windows within the same subsystem
- shows those spans on the subsystem behavior chart and signal quality chart
- shows matching span cards in the system alarm card section
- lets the user toggle between:
  - minute windows
  - clustered spans

This plan does not require changes to `backend/1803.py`.

## Current State

The runtime beta alert flow is currently built from timestamp-level subsystem alarm rows in:

- `backend/data_beta/detailed_subsystem_alarms.csv`

The beta API derives alert rows at runtime in:

- `backend/app.py`

The current UI then treats each active alarm minute as a 1-minute alert row:

- `start_ts == end_ts`
- `duration_minutes = 1`

These minute rows are already shown in:

- system alarm cards
- subsystem behavior chart
- signal quality chart

## Constraint

The minute-level rows remain the source of truth.

That means:

- we should not replace them
- we should not rewrite `1803.py`
- we should build clustered spans as a second runtime view on top of the minute rows

## What The Data Suggests

Using the current `backend/data_beta/detailed_subsystem_alarms.csv`:

- `SYS_1` has `386` active alarm minutes
- `SYS_1` already collapses into `30` contiguous runs before any extra clustering
- `SYS_3` has `82` active alarm minutes
- `SYS_3` already collapses into `10` contiguous runs before any extra clustering

Gap observations between contiguous runs:

- `SYS_1` gaps are usually large
- `SYS_1` only has `2` inter-run gaps at `<= 10` minutes
- `SYS_3` has `1` inter-run gap at `2` minutes

The important takeaway is:

- the source alarm signal already gives us dynamic contiguous runs
- those contiguous runs are the right first definition of a span
- we should not think of this as creating fixed `5 minute` or `10 minute` spans

## Recommendation

Use dynamic spans, not fixed-duration spans.

That means:

- if `5` alarm minutes occur back-to-back, that becomes one span
- if `12` alarm minutes occur back-to-back, that becomes one span
- a span lasts from the first alarm minute in the cluster to the last alarm minute in the cluster

For the first pass, the cleanest rule is:

- contiguous alarm minutes within the same subsystem form one span

Optional second step, only if needed after validation:

- allow a very small bridge gap between runs that are nearly continuous

But that bridge rule should be treated as a later tuning decision, not the core definition of a span.

## Data Model

Support two alert display modes:

- `minute`
- `span`

### Minute Mode

This is the existing source-of-truth behavior:

- one row per active alarm timestamp
- `start_ts == end_ts`
- `duration_minutes = 1`

### Span Mode

This is the new derived view:

- group minute rows by `class` first
- collapse contiguous minute rows into native runs
- output one span row per merged result

Span rows should include:

- `class`
- `start_ts`
- `end_ts`
- `duration_minutes`
- `minute_count`
- `severity_mix`
- `high_count`
- `medium_count`
- `low_count`
- `max_score`
- `mean_score`
- `affected_sensor_count`
- `affected_sensors`

## Severity Rule For Spans

Minute-level severity remains untouched.

Do not roll span severity up to one label.

Instead, the span should carry a severity breakdown of the member minute alerts.

Recommended fields:

- `high_count`
- `medium_count`
- `low_count`
- `severity_mix`

Examples:

- `5 alerts: 5 high`
- `8 alerts: 6 high, 2 low`
- `4 alerts: 4 low`

This keeps the span honest to the source minute rows and avoids inventing a single severity classification for a mixed span.

## Sensor Detail Rule For Spans

For span cards and span modal detail:

- aggregate the minute-level sensor rows that belong to the span
- rank sensors by span-level contribution

Recommended aggregation:

- `sensor_peak_score = max(sensor_peak_score within span)`
- `sensor_mean_score = mean(sensor_mean_score within span)`
- `sensor_rank = rank by sensor_peak_score desc, then sensor_mean_score desc`

This keeps the minute view intact while making the span view usable.

## Backend Plan

File:

- `backend/app.py`

### 1. Keep the existing minute-level builder

Keep `_beta_build_timestamp_alert_tables()` as the base source-of-truth builder.

### 2. Add a span builder on top of it

Add a new helper, for example:

- `_beta_build_span_alert_tables()`

It should:

- call `_beta_build_timestamp_alert_tables()`
- group alert rows by subsystem
- collapse minute rows into contiguous runs
- build:
  - span-level alert rows
  - span-level sensor rows

Optional later helper behavior:

- support a small bridge gap only if we explicitly decide to cluster non-contiguous runs

### 3. Add alert-view query parameters

Support optional query params on beta alert endpoints:

- `alarm_view=minute|span`

Affected routes:

- `/api/beta/alerts`
- `/api/beta/alerts_sensor_level`
- `/api/beta/dashboard/summary`
- `/api/beta/subsystem_behavior/<system_id>`
- `/api/beta/sensor_quality/<system_id>`

Behavior:

- `alarm_view=minute` returns the current minute rows
- `alarm_view=span` returns clustered rows/bands

### 4. Keep chart responses aligned with card mode

The chart alarm bands must come from the same selected alert mode as the cards.

That prevents:

- cards showing spans while charts show minutes
- or the reverse

## Frontend Plan

### 1. Add one shared alert-view toggle at dashboard level

File:

- `frontend/src/components/DashboardBeta.js`

Add dashboard-level state:

- `alarmView = 'minute' | 'span'`

Pass these into:

- `BetaAlertEpisodeCards.js`
- `SubsystemBehaviorChartBeta.js`
- `SensorQualityGrid.js`
- `BetaAlertDetailModal.js`

Recommended labels:

- `Minute Windows`
- `Alarm Spans`

### 2. Cards should switch dataset, not reinterpret locally

File:

- `frontend/src/components/BetaAlertEpisodeCards.js`

Do not cluster inside the card component.

Instead:

- fetch the selected mode from the API
- render whichever rows come back

Card copy should adapt:

- minute mode: one alarm minute
- span mode: grouped subsystem alarm span

For span cards, add a callout that says how many minute alerts are inside the span and how they break down by severity.

Example:

- `12 alerts`
- `7 high, 5 low`

### 3. Charts should switch overlay mode

Files:

- `frontend/src/components/SubsystemBehaviorChartBeta.js`
- `frontend/src/components/SensorQualityGrid.js`

In minute mode:

- keep the current widened 1-minute markers

In span mode:

- render full span overlays from `start_ts` to `end_ts`
- if all member alerts share one severity, use that severity color
- if the span contains mixed severities, use a distinct mixed-span style and show the exact breakdown in the tooltip

### 4. Modal detail should match selected row type

File:

- `frontend/src/components/BetaAlertDetailModal.js`

In minute mode:

- show one timestamp row

In span mode:

- show span start and end
- show minute count
- show severity breakdown counts
- show aggregated sensor table for the span

## API Shape Recommendation

To keep the frontend simple, the API should return the same top-level shape for both modes.

Example alert row:

- `view_type`: `minute` or `span`
- `class`
- `start_ts`
- `end_ts`
- `duration_minutes`
- `minute_count`
- `severity_mix`
- `high_count`
- `medium_count`
- `low_count`
- `max_score`
- `mean_score`
- `sensor_id`
- `affected_sensor_count`
- `affected_sensors`

The same principle should apply to sensor-level detail rows:

- minute mode returns per-minute sensor context
- span mode returns aggregated sensor context

## UX Recommendation

Use one global toggle for the dashboard alarm mode.

Reason:

- cards and charts stay synchronized
- avoids confusion where one widget is in minute mode and another is in span mode

Optional future enhancement:

- expose an advanced clustering control only if we later decide we need non-contiguous bridge merging

For the first pass, do not expose clustering-gap controls.

## Implementation Order

1. Add span-building helper in `backend/app.py`
2. Add `alarm_view` support to beta alert and chart endpoints
3. Add dashboard-level toggle in `DashboardBeta.js`
4. Wire cards to request/render the selected mode
5. Wire both charts to request/render the selected mode
6. Update modal to support span detail
7. Validate that card counts and chart overlays match in both modes

## Validation Checklist

### Minute Mode

- counts match the current timestamp-level alert rows
- charts show one-minute windows
- cards show one-minute windows

### Span Mode

- spans only merge within the same subsystem
- spans reflect dynamic contiguous alarm runs rather than fixed-length windows
- card count drops relative to minute mode
- chart overlays match the span cards exactly
- clicking a chart span opens the matching span card/modal record
- span cards show correct minute-count and severity breakdown callouts

## What Not To Do

- do not rewrite `1803.py`
- do not replace the minute-level source-of-truth rows
- do not let cards and charts use different alert modes at the same time
- do not invent a single rolled-up severity label for mixed spans
- do not treat spans as fixed `5 minute` or `10 minute` windows

## Summary

The safest design is:

- minute rows remain the source of truth
- clustered spans are a runtime-derived second view
- spans are dynamic and based on actual alarm continuity
- one dashboard toggle controls whether the UI shows minute windows or grouped alarm spans
- span cards should explicitly show how many minute alerts they contain and the severity mix inside the span
