import { useMemo } from 'react';
import GlassCard from './GlassCard';
import GaugeWidget from './GaugeWidget';
import InfoTooltip from './InfoTooltip';

const filterBadgeStyle = {
  fontSize: '10px',
  fontWeight: 500,
  color: '#1B5E20',
  background: 'rgba(129,199,132,0.15)',
  border: '1px solid rgba(129,199,132,0.3)',
  borderRadius: '6px',
  padding: '3px 10px',
  marginLeft: 'auto',
  whiteSpace: 'nowrap',
  letterSpacing: '0.02em',
  textTransform: 'none',
};

function SubsystemGauges({ stats, filterLabel, systems, onFilterClick }) {
  if (!stats) return null;

  const gaugeColor = (val) => {
    if (val >= 0.8) return '#EF5350';
    if (val >= 0.55) return '#FFA726';
    return '#4CAF50';
  };

  // Build dynamic subsystem list from discovered systems + fixed scores
  const subsystems = useMemo(() => {
    const list = [];

    // Dynamic SYS_* scores from stats keys
    const sysKeys = Object.keys(stats)
      .filter((k) => k.startsWith('score_SYS_'))
      .sort();

    if (sysKeys.length > 0) {
      // New dynamic format
      sysKeys.forEach((key) => {
        const sysId = key.replace('score_', '');
        const sysInfo = systems?.find((s) => s.system_id === sysId);
        list.push({
          key,
          label: sysId.replace('_', ' '),
          tooltip: sysInfo
            ? `${sysId} subsystem score from ${sysInfo.sensor_count} sensors. Top-3 mean of per-sensor Engine A/B anomaly scores, folded with PCA multivariate score. Forced to 0 during downtime.`
            : `${sysId} subsystem anomaly score. Forced to 0 during downtime.`,
        });
      });
    } else {
      // Legacy format fallback
      if (stats.mech_score) list.push({ key: 'mech_score', label: 'Mechanical', tooltip: 'Mechanical subsystem score.' });
      if (stats.elec_score) list.push({ key: 'elec_score', label: 'Electrical', tooltip: 'Electrical subsystem score.' });
      if (stats.therm_score) list.push({ key: 'therm_score', label: 'Thermal', tooltip: 'Thermal subsystem score.' });
    }

    list.push({
      key: 'subsystem_score',
      label: 'Fused Score',
      tooltip: 'Fused subsystem score: dynamically weighted average of all discovered subsystem scores, multiplied by SQS confidence and downtime gate.',
    });

    return list;
  }, [stats, systems]);

  return (
    <GlassCard delay={0.35} style={{ marginTop: '16px' }} intensity="strong">
      <div style={{ fontSize: '12px', fontWeight: 500, color: '#6B736B', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '16px', display: 'flex', alignItems: 'center' }}>
        Subsystem Scores
        <InfoTooltip text="Each subsystem score is the top-3 mean of per-sensor anomaly scores from Engine A (drift detection) and Engine B (periodicity), folded with PCA reconstruction error. Weights are dynamically assigned based on system size." />
        {filterLabel && <span style={{ ...filterBadgeStyle, cursor: 'pointer' }} onClick={onFilterClick}>{filterLabel}</span>}
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-around', flexWrap: 'wrap', gap: '20px' }}>
        {subsystems.map((sub) => {
          const stat = stats[sub.key];
          let topSub = null;
          if (sub.key === 'subsystem_score') {
            const others = subsystems.filter((s) => s.key !== 'subsystem_score' && s.key !== 'physics_score');
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
              tooltip={sub.tooltip}
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
