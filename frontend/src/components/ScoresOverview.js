import React from 'react';
import GlassCard from './GlassCard';
import InfoTooltip from './InfoTooltip';

const metricTooltips = {
  risk_score: 'Fused risk score: (0.35*mech + 0.35*elec + 0.15*therm + 0.15*instrument) * clip(sqs_mean,0,1) * gate. Gate is 0 during downtime, 1 otherwise.',
  mech_score: 'Mechanical subsystem block score: top-3 mean of Engine A/B vibration sensor scores, folded with PCA multivariate (SPE/T2). Forced to 0 during downtime.',
  elec_score: 'Electrical subsystem block score: top-3 mean from 30 electrical tags + kW residual, folded with PCA. Forced to 0 during downtime.',
  therm_score: 'Thermal subsystem block score: top-3 mean from 11 temperature sensors, folded with PCA. Forced to 0 during downtime.',
  physics_score: 'Physics validation: max of normalized identity residuals (kW, PF, kVAR) and current unbalance. Each normalized by its 99th percentile, clipped to [0,1].',
  subsystem_score: 'Weighted fusion: 0.35*mech + 0.35*elec + 0.15*therm + 0.15*physics. Missing values filled with 0 before fusion.',
  sqs_mean: 'Row-wise mean of per-sensor SQS across all present sensors. SQS starts at 1.0 and is penalized for bounds violations, rate-of-change spikes, identity contradictions, and regression contradictions.',
};

const styles = {
  heading: {
    fontSize: '12px',
    fontWeight: 500,
    color: '#6B736B',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    marginBottom: '16px',
    display: 'flex',
    alignItems: 'center',
  },
  table: {
    width: '100%',
    borderCollapse: 'separate',
    borderSpacing: '0 4px',
    fontSize: '12px',
  },
  th: {
    textAlign: 'left',
    padding: '8px 12px',
    color: '#6B736B',
    fontWeight: 500,
    fontSize: '11px',
    textTransform: 'uppercase',
    letterSpacing: '0.04em',
    borderBottom: '1px solid rgba(203,230,200,0.4)',
  },
  td: {
    padding: '10px 12px',
    color: '#2D332D',
    fontVariantNumeric: 'tabular-nums',
  },
  row: {
    background: 'rgba(255,255,255,0.45)',
    borderRadius: '8px',
    transition: 'background 0.15s',
  },
};

function ScoresOverview({ stats }) {
  if (!stats) return null;

  const metrics = [
    { key: 'risk_score', label: 'Risk Score' },
    { key: 'mech_score', label: 'Mechanical' },
    { key: 'elec_score', label: 'Electrical' },
    { key: 'therm_score', label: 'Thermal' },
    { key: 'physics_score', label: 'Physics' },
    { key: 'subsystem_score', label: 'Fused Subsystem' },
    { key: 'sqs_mean', label: 'SQS Mean' },
  ];

  return (
    <GlassCard delay={0.5} style={{ marginTop: '16px' }} intensity="strong">
      <div style={styles.heading}>
        Score Statistics
        <InfoTooltip text="Aggregate statistics for all scores across the full time range. Engine A uses rolling baseline (120-min window) and MAD (240-min window) for drift detection. Engine B uses FFT spectral ratio within [5,60]-minute periods. PCA trains on running data with 95% explained variance threshold." />
      </div>
      <table style={styles.table}>
        <thead>
          <tr>
            <th style={styles.th}>Metric</th>
            <th style={styles.th}>Mean</th>
            <th style={styles.th}>Max</th>
            <th style={styles.th}>Min</th>
            <th style={styles.th}>Std Dev</th>
            <th style={{ ...styles.th, width: '30px' }}></th>
          </tr>
        </thead>
        <tbody>
          {metrics.map((m) => {
            const s = stats[m.key];
            return (
              <tr key={m.key} style={styles.row}>
                <td style={{ ...styles.td, fontWeight: 500 }}>{m.label}</td>
                <td style={styles.td}>{s?.mean ?? '--'}</td>
                <td style={styles.td}>{s?.max ?? '--'}</td>
                <td style={styles.td}>{s?.min ?? '--'}</td>
                <td style={styles.td}>{s?.std ?? '--'}</td>
                <td style={styles.td}>
                  <InfoTooltip text={metricTooltips[m.key]} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </GlassCard>
  );
}

export default ScoresOverview;