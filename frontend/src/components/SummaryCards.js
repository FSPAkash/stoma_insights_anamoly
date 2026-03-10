import React from 'react';
import { motion } from 'framer-motion';
import GlassCard from './GlassCard';
import GaugeWidget from './GaugeWidget';
import InfoTooltip from './InfoTooltip';

const styles = {
  grid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
    gap: '16px',
  },
  statLabel: {
    fontSize: '12px',
    fontWeight: 500,
    color: '#6B736B',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    marginBottom: '8px',
    display: 'flex',
    alignItems: 'center',
  },
  statValue: {
    fontSize: '28px',
    fontWeight: 600,
    color: '#1B5E20',
    letterSpacing: '-0.02em',
  },
  statSub: {
    fontSize: '12px',
    color: '#8A928A',
    marginTop: '4px',
  },
  gaugeRow: {
    display: 'flex',
    justifyContent: 'space-around',
    flexWrap: 'wrap',
    gap: '16px',
    marginTop: '16px',
  },
  badge: {
    display: 'inline-block',
    padding: '3px 10px',
    borderRadius: '20px',
    fontSize: '12px',
    fontWeight: 600,
  },
};

function SummaryCards({ summary, stats }) {
  const gaugeColor = (val) => {
    if (val >= 0.8) return '#EF5350';
    if (val >= 0.55) return '#FFA726';
    return '#4CAF50';
  };

  const getTopContributor = () => {
    if (!stats) return null;
    const subs = [
      { key: 'mech_score', label: 'Mechanical', weight: 0.35 },
      { key: 'elec_score', label: 'Electrical', weight: 0.35 },
      { key: 'therm_score', label: 'Thermal', weight: 0.15 },
      { key: 'physics_score', label: 'Physics', weight: 0.15 },
    ];
    let top = null;
    for (const s of subs) {
      const mean = stats[s.key]?.mean;
      if (mean != null && (top === null || mean > top.mean)) {
        top = { ...s, mean };
      }
    }
    return top;
  };

  const topContrib = getTopContributor();

  return (
    <div>
      <div style={styles.grid}>
        <GlassCard delay={0.05} intensity="strong">
          <div style={styles.statLabel}>
            Total Alerts
            <InfoTooltip text="Alert episodes are built from contiguous periods where the fused risk_score exceeds the medium (0.55) or high (0.80) threshold. Episodes shorter than 5 minutes are discarded, and gaps of 3 minutes or less are merged. Only non-NORMAL classifications generate alerts." />
          </div>
          <div style={styles.statValue}>{summary?.total_alerts ?? '--'}</div>
          <div style={{ display: 'flex', gap: '8px', marginTop: '8px' }}>
            <span style={{ ...styles.badge, background: '#FFCDD2', color: '#C62828' }}>
              HIGH {summary?.high_alerts ?? 0}
            </span>
            <span style={{ ...styles.badge, background: '#FFE0B2', color: '#E65100' }}>
              MED {summary?.medium_alerts ?? 0}
            </span>
          </div>
        </GlassCard>

        <GlassCard delay={0.1} intensity="strong">
          <div style={styles.statLabel}>
            Total Sensors
            <InfoTooltip text="Total sensor channels monitored: 8 mechanical vibration (VT), 11 thermal/temperature (TT, RTD), and 30 electrical (current, voltage, power, PF, frequency). Sensors are never dropped; only individual invalid cells are nullified." />
          </div>
          <div style={styles.statValue}>{summary?.total_sensors ?? '--'}</div>
          <div style={styles.statSub}>Monitored channels</div>
        </GlassCard>

        <GlassCard delay={0.15} intensity="strong">
          <div style={styles.statLabel}>
            Avg Signal Quality
            <InfoTooltip text="Signal Quality Score (SQS) per sensor per timestamp. Starts at 1.0 and is penalized multiplicatively: bounds violation (x0.6), rate-of-change spike (x0.7), identity contradiction (x0.85), regression contradiction (x0.80). Final SQS is clipped to [0,1]. This shows the mean across all sensors and timestamps." />
          </div>
          <div style={styles.statValue}>
            {summary?.avg_sqs !== null && summary?.avg_sqs !== undefined
              ? Number(summary.avg_sqs).toFixed(3)
              : '--'}
          </div>
          <div style={styles.statSub}>SQS Mean</div>
        </GlassCard>

        <GlassCard delay={0.2} intensity="strong">
          <div style={styles.statLabel}>
            Running Uptime
            <InfoTooltip text="Downtime is detected from electrical signals only (kW, current, voltage, frequency). The machine is marked DOWNTIME when all four signals are below their 2nd-percentile thresholds, or when both kW and current are off. All other timestamps are RUNNING." right />
          </div>
          <div style={styles.statValue}>
            {summary?.running_pct !== null && summary?.running_pct !== undefined
              ? `${summary.running_pct}%`
              : '--'}
          </div>
          <div style={styles.statSub}>
            {stats?.running_count ?? '--'} running / {stats?.downtime_count ?? '--'} downtime
          </div>
        </GlassCard>

        <GlassCard delay={0.25} intensity="strong">
          <div style={styles.statLabel}>
            Classification
            <InfoTooltip text="Fault classification rules: MECH requires mech_score >= high and physics < high. ELEC requires elec_score >= high and physics >= medium. PROCESS requires elec >= medium, mech >= 0.6*medium, and subsystem >= medium. INSTRUMENT requires physics >= high with all others below 0.8*medium. NORMAL is assigned when fused subsystem score is below medium threshold." right />
          </div>
          <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap', marginTop: '4px' }}>
            {summary?.class_distribution &&
              Object.entries(summary.class_distribution).map(([cls, count]) => (
                <motion.span
                  key={cls}
                  style={{
                    ...styles.badge,
                    background: cls === 'NORMAL' ? '#E6F4EA' : cls === 'MECH' ? '#E8F5E9' : cls === 'ELEC' ? '#E0F2F1' : cls === 'THERM' ? '#FFF3E0' : cls === 'INSTRUMENT' ? '#E3F2FD' : '#F3E5F5',
                    color: cls === 'NORMAL' ? '#1B5E20' : cls === 'MECH' ? '#1B5E20' : cls === 'ELEC' ? '#004D40' : cls === 'THERM' ? '#E65100' : cls === 'INSTRUMENT' ? '#0D47A1' : '#4A148C',
                  }}
                  whileHover={{ scale: 1.05 }}
                >
                  {cls} {count}
                </motion.span>
              ))}
          </div>
        </GlassCard>
      </div>

      <GlassCard delay={0.3} style={{ marginTop: '16px' }} intensity="strong">
        <div style={{ ...styles.statLabel, marginBottom: '12px' }}>
          Risk Gauges
          <InfoTooltip text="Risk score = weighted fusion of subsystem scores (mech 0.35, elec 0.35, therm 0.15, instrument 0.15) multiplied by SQS confidence and downtime gate. During downtime, risk is forced to 0. Instrument risk uses max(physics_score, 1 - sqs_p10)." />
        </div>
        <div style={styles.gaugeRow}>
          <GaugeWidget
            value={summary?.current_risk_score} label="Current Risk"
            color={gaugeColor(summary?.current_risk_score ?? 0)} size={130}
            tooltip="Latest timestamp risk_score value. Computed as weighted subsystem fusion * confidence * gate."
            hoverDetail={topContrib ? `Top contributor: ${topContrib.label} (mean ${topContrib.mean.toFixed(3)}, weight ${topContrib.weight})` : null}
          />
          <GaugeWidget
            value={summary?.avg_risk_score} label="Average Risk"
            color={gaugeColor(summary?.avg_risk_score ?? 0)} size={130}
            tooltip="Mean risk_score across all timestamps in the dataset."
            hoverDetail={topContrib ? `Top contributor: ${topContrib.label} (mean ${topContrib.mean.toFixed(3)}, weight ${topContrib.weight})` : null}
          />
          <GaugeWidget
            value={summary?.max_risk_score} label="Peak Risk"
            color={gaugeColor(summary?.max_risk_score ?? 0)} size={130}
            tooltip="Maximum risk_score observed. HIGH severity is assigned when max_score >= 0.85 within an episode."
            hoverDetail={topContrib ? `Top contributor: ${topContrib.label} (max ${(stats[topContrib.key]?.max ?? 0).toFixed(3)})` : null}
          />
          <GaugeWidget
            value={summary?.avg_sqs} label="Avg SQS"
            color="#4CAF50" size={130}
            tooltip="Average Signal Quality Score. Multiplicative penalties from bounds, rate-of-change, identity, and regression checks. Higher is better."
          />
        </div>
        <div style={{ display: 'flex', gap: '12px', justifyContent: 'center', marginTop: '12px', flexWrap: 'wrap' }}>
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
    </div>
  );
}

export default SummaryCards;