import React from 'react';
import GlassCard from './GlassCard';
import GaugeWidget from './GaugeWidget';
import InfoTooltip from './InfoTooltip';

const subsystemTooltips = {
  mech_score: 'Mechanical subsystem score from 8 vibration sensors (VT tags). Computed via top-3 mean of per-sensor Engine A/B anomaly scores, folded with PCA multivariate score. Forced to 0 during downtime.',
  elec_score: 'Electrical subsystem score from 30 electrical tags (current, voltage, power, PF, frequency) plus kW residual. Top-3 mean of sensor scores folded with PCA. Forced to 0 during downtime.',
  therm_score: 'Thermal subsystem score from 11 temperature sensors (TT and RTD tags). Top-3 mean folded with PCA multivariate anomaly. Forced to 0 during downtime.',
  physics_score: 'Physics validation score from identity residual checks: kW identity mismatch, PF identity, kVAR identity, and current unbalance. Each normalized by its 99th percentile. Final is the max across all components. Forced to 0 during downtime.',
  subsystem_score: 'Fused subsystem score: weighted average of mech (0.35), elec (0.35), therm (0.15), and physics (0.15) scores.',
};

function SubsystemGauges({ stats }) {
  if (!stats) return null;

  const subsystems = [
    { key: 'mech_score', label: 'Mechanical' },
    { key: 'elec_score', label: 'Electrical' },
    { key: 'therm_score', label: 'Thermal' },
    { key: 'physics_score', label: 'Physics' },
    { key: 'subsystem_score', label: 'Fused Score' },
  ];

  const gaugeColor = (val) => {
    if (val >= 0.8) return '#EF5350';
    if (val >= 0.55) return '#FFA726';
    return '#4CAF50';
  };

  return (
    <GlassCard delay={0.35} style={{ marginTop: '16px' }} intensity="strong">
      <div style={{ fontSize: '12px', fontWeight: 500, color: '#6B736B', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '16px', display: 'flex', alignItems: 'center' }}>
        Subsystem Scores
        <InfoTooltip text="Each subsystem score is the top-3 mean of per-sensor anomaly scores from Engine A (drift detection using rolling baseline + MAD) and Engine B (periodicity via FFT spectral ratio), folded with PCA reconstruction error. Fusion weights: mech 0.35, elec 0.35, therm 0.15, physics 0.15." />
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-around', flexWrap: 'wrap', gap: '20px' }}>
        {subsystems.map((sub) => {
          const stat = stats[sub.key];
          // Find the highest subsystem (excluding self for fused score)
          let topSub = null;
          if (sub.key === 'subsystem_score') {
            const others = subsystems.filter((s) => s.key !== 'subsystem_score');
            for (const o of others) {
              const oMean = stats[o.key]?.mean;
              if (oMean != null && (topSub === null || oMean > topSub.val)) {
                topSub = { label: o.label, val: oMean };
              }
            }
          }
          const hoverDetail = sub.key === 'subsystem_score' && topSub
            ? `Highest: ${topSub.label} (${topSub.val.toFixed(3)})`
            : stat ? `mean: ${Number(stat.mean).toFixed(3)} | std: ${Number(stat.std).toFixed(3)} | min: ${Number(stat.min).toFixed(3)}` : null;
          return (
            <GaugeWidget
              key={sub.key}
              value={stat?.mean ?? null}
              label={sub.label}
              sublabel={stat ? `max: ${Number(stat.max).toFixed(3)}` : ''}
              color={gaugeColor(stat?.mean ?? 0)}
              size={110}
              tooltip={subsystemTooltips[sub.key]}
              hoverDetail={hoverDetail}
            />
          );
        })}
      </div>
      <div style={{ display: 'flex', gap: '12px', justifyContent: 'center', marginTop: '14px', flexWrap: 'wrap' }}>
        {[
          { color: '#4CAF50', label: 'Normal (< 0.55)' },
          { color: '#FFA726', label: 'Medium (0.55 - 0.79)' },
          { color: '#EF5350', label: 'High (>= 0.80)' },
        ].map((item) => (
          <div key={item.label} style={{ display: 'flex', alignItems: 'center', gap: '5px', fontSize: '11px', color: '#6B736B' }}>
            <div style={{ width: '18px', height: '4px', borderRadius: '2px', background: item.color, opacity: 0.8 }} />
            {item.label}
          </div>
        ))}
      </div>
    </GlassCard>
  );
}

export default SubsystemGauges;