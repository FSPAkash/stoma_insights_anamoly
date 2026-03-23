# `T_0317_newest.py` vs `1803.py`

## Scope

This comparison treats:

- `backend/T_0317_newest.py`
- `backend/1803.py`

as the source of truth.

The key high-level result is:

- The files are identical through line `1334`.
- The divergence starts at line `1335`, inside the AutoEncoder subsystem scoring section.
- Everything before that point is effectively the same pipeline.

## Executive Summary

`1803.py` is not a brand-new pipeline. It is a targeted evolution of `T_0317_newest.py` focused on the AutoEncoder output layer and how downtime is represented.

The biggest behavioral changes in `1803.py` are:

1. AutoEncoder scoring is computed on `running` timestamps only.
2. Downtime is excluded from AE-derived calculations instead of being computed and then forced to zero.
3. Per-timestamp sensor contribution rankings are added.
4. Detailed output schemas expand to include ranking and contribution columns.
5. Several shared output files now use `NaN` during downtime where the older version used `0` or `"Low"`.
6. `1803.py` now exports raw chart inputs for the beta dashboard: `df_chart_data.csv` and `sensor_values_{system}.csv`.
7. The beta subsystem-behavior path is now intended to read one beta artifact set from one folder, instead of splitting raw traces and overlays across two folders.

## What Stayed The Same

The entire pipeline up to AutoEncoder scoring is unchanged in logic. That includes:

- Data loading from the same parquet source
- Pivoting and 1-minute resampling
- Base invalid mask generation
- Downtime detection
- Sensor quality scoring (`SQS`)
- Engine A drift scoring
- Engine B periodicity scoring
- Sensor trust calculation
- Dynamic system discovery and validation
- Core model/config definitions like `PipelineConfig` and `DeepAutoEncoder`

The following top-level definitions are unchanged in logic across both files:

- `PipelineConfig`
- `DeepAutoEncoder`
- `read_parquet_long_to_wide`
- `detect_frequency`
- `clean_resample_mean`
- `resample_to_minute_grid`
- `base_invalid_mask`
- `apply_bad_mask_nullify_cells`
- `initial_sensor_validation`
- `build_downtime_mask`
- `global_downtime`
- `load_sensor_cfg_with_logging`
- `compute_sqs`
- `summarize_sqs`
- `rolling_median`
- `rolling_mad`
- `get_all_sensors_from_df`
- `compute_engineA`
- `spectral_energy_ratio`
- `learn_engineB_thresholds`
- `select_engineB_sensors`
- `compute_engineB`
- `compute_sensor_trust`
- `summarize_trust`
- `clean_for_clustering`
- `_find_plateau_threshold`
- `discover_systems`
- `validate_systems_r2`

Also important: a few shared helper functions look different textually, but only because of docstring/comment cleanup. Their logic is unchanged:

- `_get_direction`
- `directional_residual`
- `_compute_ae_risk_array`
- `robust_adaptive_threshold`
- `_on_off_delay_alarm`
- `select_ae_candidates`
- `train_ae_subsystem`

## What Changed

### 1. AE section version and intent

`T_0317_newest.py` labels the section as:

- `AUTOENCODER SUBSYSTEM SCORING (v2 - with Confidence & Baseline)`

`1803.py` labels it as:

- `AUTOENCODER SUBSYSTEM SCORING (v3 - Sensor Rankings + Downtime Exclusion)`

That label is accurate: the main upgrade is ranking contributing sensors and treating downtime as excluded rather than zero-valued.

### 2. `compute_reference_baseline(...)`

In `T_0317_newest.py`:

- It expects a full-timeline risk array.
- It also takes `running_mask`.
- Healthy baseline selection explicitly filters to running timestamps.
- If too few healthy points exist, it falls back to the first `n` running points.

In `1803.py`:

- It assumes the input risk array is already running-only.
- `running_mask` is removed from the function signature.
- Healthy baseline selection works on that already-filtered running subset.
- If too few healthy points exist, it falls back to the first `n` rows of the running-only array.

Impact:

- The baseline logic is conceptually the same, but `1803.py` moves downtime exclusion upstream instead of handling it inside the baseline function.

### 3. `compute_subsystem_confidence(...)`

In `T_0317_newest.py`:

- Confidence is computed across the full timeline.
- It receives full `system_score`, full `sensor_trust`, and `running_mask`.
- Downtime confidence is later forced to `0.0`.

In `1803.py`:

- Confidence is computed only for running rows.
- It receives a running-only score array and running-only trust frame.
- It returns a running-only NumPy array, which is mapped back to the full index later.
- Downtime becomes `NaN`, not `0.0`.

Impact:

- `1803.py` treats downtime as "not evaluated" rather than "evaluated and normal."

### 4. `score_ae_subsystem(...)`

This is the main behavioral change.

In `T_0317_newest.py`:

- The AE scores the full timeline after `ffill/bfill`.
- Risk is computed for all timestamps.
- Downtime rows are then forced to:
  - `system_score = 0.0`
  - `confidence = 0.0`
  - alarm off
- The return payload contains:
  - `system_score`
  - `per_sensor_sigma`
  - `threshold`
  - `alarm`
  - `confidence`
  - `baseline_sigma`
  - `risk_raw`

In `1803.py`:

- The AE scores only `running` timestamps.
- Downtime rows are excluded before reconstruction, risk, threshold, alarm, and confidence are computed.
- The function adds two new products:
  - `sensor_contributions`
  - `sensor_rankings`
- It also adds a stored `score_level`.
- Results are mapped back to the full index afterward.
- Downtime rows become:
  - `system_score = NaN`
  - `threshold = NaN`
  - `confidence = NaN`
  - `score_level = NaN`
  - `alarm = False`

Impact:

- `T_0317_newest.py` makes downtime look like a low-risk normal state.
- `1803.py` makes downtime look like missing / not-computed state.

### 5. New functions added in `1803.py`

`1803.py` introduces:

- `compute_sensor_contributions(...)`
- `build_sensor_rankings(...)`
- `build_detailed_system_output_v3(...)`

These do not exist in `T_0317_newest.py`.

Purpose:

- compute a sigma-scaled contribution per sensor per timestamp
- rank the top contributing sensors at each timestamp
- publish those rankings into the combined output and a dedicated rankings file
- export cleaned raw sensor chart inputs for the beta dashboard

### 6. Functions removed from the final flow

`T_0317_newest.py` has these helper functions that are not present in `1803.py`:

- `classify_score_level(...)`
- `enforce_downtime_zero(...)`
- `build_detailed_system_output(...)`

Why they disappear:

- `1803.py` no longer recomputes score level in the output layer
- it no longer forces downtime values to zero
- it replaces the old output builder with a v3 builder that preserves `NaN` downtime and includes rankings/contributions

## Output Artifacts

### Outputs That Remain The Same

Both scripts write the same shared non-ranking artifacts:

- `initial_sensor_validation_report.csv`
- `sensor_trust_summary.csv`
- `topological_stability.png`
- `dendrogram_systems.png`
- `correlation_distance_heatmaps.png`
- `spectral_energy.png`
- `system_summary.csv`
- `system_detail.csv`
- `dynamic_catalog.csv`
- `ae_training_loss.png`
- `detailed_system_sensor_scores.csv`
- `detailed_system_sensor_scores.parquet`
- `detailed_engine_a.csv`
- `detailed_engine_a.parquet`
- `detailed_engine_b.csv`
- `detailed_engine_b.parquet`
- `detailed_subsystem_scores.csv`
- `detailed_subsystem_scores.parquet`
- `detailed_sqs.csv`
- `detailed_sqs.parquet`
- `detailed_sensor_trust.csv`
- `detailed_sensor_trust.parquet`
- `detailed_subsystem_confidence.csv`
- `detailed_subsystem_confidence.parquet`
- `detailed_subsystem_alarms.csv`
- `detailed_subsystem_alarms.parquet`
- `subsystem_summary.csv`

## New Output Artifacts In `1803.py`

Only `1803.py` adds:

- `detailed_sensor_rankings.csv`
- `detailed_sensor_rankings.parquet`
- `sensor_config.csv`
- `dynamic_weights.csv`
- `df_chart_data.csv`
- `sensor_values_{system}.csv`

So the detailed export count changes from:

- `T_0317_newest.py`: 7 separate detailed files
- `1803.py`: 8 separate detailed files

Both still also write:

- 1 combined detailed file pair: `detailed_system_sensor_scores.(csv|parquet)`
- 1 summary file: `subsystem_summary.csv`

The extra beta-support files in `1803.py` are important:

- `sensor_config.csv` supports the beta invalid-sensor endpoint
- `dynamic_weights.csv` supports beta subsystem metadata
- `df_chart_data.csv` and `sensor_values_{system}.csv` support beta subsystem behavior charts

## Beta Dashboard Integration

Before this update, the beta dashboard effectively depended on two different folders:

- `data_beta/` for beta subsystem scores, alarms, SQS, and related overlays
- `data/` for raw sensor chart inputs such as `df_chart_data.csv` or `sensor_values_{system}.csv`

After this update, the intended beta artifact model is:

- `1803.py` exports the raw chart inputs itself
- the beta-facing artifact set is mirrored into the beta folder
- the beta subsystem-behavior API reads both raw traces and overlays from that same beta folder

Practical effect:

- subsystem behavior charts no longer require one folder for traces and another for alarm/downtime overlays
- the beta pipeline is closer to being self-contained

This does not mean every historical beta dependency disappeared, but it does resolve the raw-chart-data split.

## Schema / Value Differences In Shared Outputs

### 1. `detailed_system_sensor_scores.(csv|parquet)`

In `T_0317_newest.py`, per subsystem this combined file contains:

- `System_Score`
- `Adaptive_Threshold`
- `Score_Level`
- `System_Confidence`
- `System_Alarm`
- `Baseline_Sigma`
- per-sensor:
  - `Invalid_Flag`
  - `SQS`
  - `Engine_A`
  - `Engine_B`
  - `Trust`

In `1803.py`, it contains all of the above plus:

- subsystem ranking columns like:
  - `Rank_1_Sensor`
  - `Rank_1_Score`
  - `Rank_1_Pct`
  - and so on up to `ae_top_k_sensors`
- per-sensor:
  - `AE_Contribution`

Downtime semantics also change:

- `T_0317_newest.py`: downtime rows are forced to `0`, `"Low"`, or alarm `0`
- `1803.py`: downtime rows are mostly `NaN`, with alarm still `0`

### 2. `detailed_subsystem_scores.(csv|parquet)`

Both versions output:

- `System_Score`
- `Adaptive_Threshold`
- `Score_Level`
- `Baseline_Sigma`

But the values differ during downtime:

- `T_0317_newest.py`
  - `System_Score = 0.0`
  - `Score_Level = "Low"`
- `1803.py`
  - `System_Score = NaN`
  - `Score_Level = NaN`

### 3. `detailed_subsystem_confidence.(csv|parquet)`

- `T_0317_newest.py`: downtime confidence is `0.0`
- `1803.py`: downtime confidence is `NaN`

### 4. `detailed_subsystem_alarms.(csv|parquet)`

Alarm behavior is similar in both for downtime:

- downtime alarm remains off / `0`

But the contextual score-level field differs:

- `T_0317_newest.py`: `Score_Level_At_Alarm = "Low"` during downtime
- `1803.py`: `Score_Level_At_Alarm = NaN` during downtime

### 5. `subsystem_summary.csv`

`T_0317_newest.py` summary columns include:

- `Subsystem`
- `Sensors`
- `Score_Mean`
- `Thresh_Mean`
- `High%`
- `High_Count`
- `Low_Count`
- `Confidence`
- `Baseline_sigma`
- `Alarms`

`1803.py` summary columns include:

- `Subsystem`
- `Sensors`
- `Score_Mean`
- `Thresh_Mean`
- `High%`
- `High_Count`
- `Confidence`
- `Baseline_sigma`
- `Alarms`
- `Top_Alarm_Contributor`

So:

- `Low_Count` is removed
- `Top_Alarm_Contributor` is added

## Practical Meaning Of The Output Difference

If you consume these outputs downstream:

- `T_0317_newest.py` is better if you want a fully filled time series where downtime is coerced into a benign state.
- `1803.py` is better if you want analytical honesty about non-running periods and want explainability about which sensors drove an AE event.

In plain terms:

- `T_0317_newest.py` says: "during downtime, treat the subsystem as low-risk/no-alert"
- `1803.py` says: "during downtime, do not score it at all"

That is the single most important interpretation change.

## Bottom Line

`1803.py` keeps the same upstream pipeline and the same core AE model/training setup, but changes the interpretation and publishing of AE results.

It mainly adds:

- downtime exclusion instead of downtime zeroing
- per-timestamp sensor contribution ranking
- richer combined exports
- one new dedicated rankings output
- a more explainable summary output
- raw chart-data exports for the beta dashboard
- beta-support metadata exports such as `sensor_config.csv` and `dynamic_weights.csv`

It does not change:

- ingestion
- preprocessing
- SQS
- Engine A / Engine B
- trust logic
- dynamic subsystem discovery
- the underlying AE architecture

## Future Agent Note

If you are wiring or debugging the beta dashboard in the future, assume the target model is now:

- the beta subsystem behavior data lives with the rest of the beta artifacts
- raw chart inputs and beta overlays should come from the same beta artifact folder
- do not assume raw traces still need to come from `backend/data` while alarms and scores come from `backend/data_beta`

Specifically, the important files that are now meant to coexist in the same beta artifact set are:

- `df_chart_data.csv`
- `sensor_values_{system}.csv`
- `dynamic_catalog.csv`
- `detailed_subsystem_alarms.csv`
- `detailed_subsystem_scores.csv`
- `detailed_sqs.csv`
- `detailed_engine_a.csv`
- `detailed_engine_b.csv`

If the beta dashboard breaks, first verify that this single-folder beta artifact set is complete before debugging the frontend.
