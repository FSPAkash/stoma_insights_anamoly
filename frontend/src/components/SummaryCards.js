import React, { useState } from 'react';
import ReactDOM from 'react-dom';
import { motion, AnimatePresence } from 'framer-motion';
import GlassCard from './GlassCard';
import GaugeWidget from './GaugeWidget';
import InfoTooltip from './InfoTooltip';
import { classColor, systemBgColor, systemColor, formatSensorName } from '../utils/formatters';

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

function DetectedSystemCard({ sys, idx }) {
  const color = systemColor(sys.system_id, idx);
  const bg = systemBgColor(sys.system_id);

  return (
    <motion.div
      style={{
        position: 'relative',
        background: 'rgba(255,255,255,0.55)',
        backdropFilter: 'blur(20px)',
        WebkitBackdropFilter: 'blur(20px)',
        borderRadius: '18px',
        border: '1.5px solid rgba(203,230,200,0.45)',
        padding: '20px 18px 18px',
        boxShadow: '0 2px 16px rgba(27,94,32,0.06), 0 1px 3px rgba(0,0,0,0.04)',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'flex-start',
        minHeight: '120px',
      }}
      whileHover={{
        boxShadow: `0 8px 32px rgba(27,94,32,0.10), 0 2px 8px ${color}15`,
        borderColor: `${color}44`,
        y: -3,
      }}
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: idx * 0.06, duration: 0.4 }}
    >
      <div style={{ position: 'absolute', top: 16, right: 16, width: 10, height: 10, borderRadius: '50%', background: color, boxShadow: `0 0 8px ${color}55` }} />
      <div style={{ fontSize: '11px', fontWeight: 600, color: '#8A928A', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: '8px' }}>System Detected</div>
      <div style={{ fontSize: '22px', fontWeight: 700, color, lineHeight: 1.1, marginBottom: '6px', textTransform: 'capitalize' }}>
        {sys.system_id.replace(/_/g, ' ')}
      </div>
      <div style={{ fontSize: '12px', color: '#6B736B', lineHeight: 1.4 }}>
        {sys.sensor_count} sensor{sys.sensor_count !== 1 ? 's' : ''} active
      </div>
      {sys.fusion_weight != null && (
        <div style={{ fontSize: '11px', color: '#8A928A', marginTop: '2px' }}>
          Weight {(sys.fusion_weight * 100).toFixed(1)}%
          {sys.quality && ` -- ${sys.quality.replace(/[^\w\s]/g, '').trim()}`}
        </div>
      )}
      {sys.sensors && sys.sensors.length > 0 && (
        <div style={{ marginTop: '12px', paddingTop: '10px', borderTop: '1px solid rgba(203,230,200,0.3)', width: '100%' }}>
          <div style={{ fontSize: '10px', fontWeight: 600, color: '#8A928A', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: '8px' }}>Sensors</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '5px' }}>
            {sys.sensors.map((s) => (
              <span key={s} style={{ display: 'inline-block', padding: '3px 9px', borderRadius: '7px', fontSize: '10.5px', fontWeight: 500, background: bg, color: '#2D332D', border: '1px solid rgba(0,0,0,0.05)' }}>
                {formatSensorName(s)}
              </span>
            ))}
          </div>
        </div>
      )}
    </motion.div>
  );
}

function DetectedSubsystemsCard({ systems }) {
  const [modalOpen, setModalOpen] = useState(false);
  const count = systems?.length ?? 0;

  return (
    <>
      <GlassCard delay={0.1} intensity="strong">
        <div style={styles.statLabel}>
          Detected Subsystems
          <InfoTooltip text="Subsystems discovered via correlation-based clustering of sensor channels. Each subsystem (SYS_1, SYS_2, etc.) groups sensors that behave similarly. Click to view details about each subsystem and its sensors." />
        </div>
        <div style={styles.statValue}>{count || '--'}</div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginTop: '8px' }}>
          <motion.button
            onClick={() => setModalOpen(true)}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: '6px',
              padding: '5px 14px',
              background: 'rgba(27,94,32,0.08)',
              border: '1.5px solid rgba(27,94,32,0.2)',
              borderRadius: '16px',
              fontSize: '11px',
              fontWeight: 600,
              color: '#1B5E20',
              cursor: 'pointer',
              transition: 'all 0.2s',
            }}
            whileHover={{
              background: 'rgba(230,244,234,0.9)',
              borderColor: 'rgba(27,94,32,0.45)',
              boxShadow: '0 2px 8px rgba(27,94,32,0.1)',
            }}
            whileTap={{ scale: 0.97 }}
          >
            Check subsystems
          </motion.button>
        </div>
      </GlassCard>

      {ReactDOM.createPortal(
        <AnimatePresence>
          {modalOpen && (
            <motion.div
              style={{
                position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
                background: 'rgba(0,0,0,0.35)', backdropFilter: 'blur(6px)', WebkitBackdropFilter: 'blur(6px)',
                zIndex: 9999, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '24px',
              }}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.2 }}
              onClick={(e) => { if (e.target === e.currentTarget) setModalOpen(false); }}
            >
              <motion.div
                style={{
                  position: 'relative', width: '100%', maxWidth: '820px', maxHeight: '85vh', overflowY: 'auto',
                  background: 'rgba(255,255,255,0.92)', backdropFilter: 'blur(28px)', WebkitBackdropFilter: 'blur(28px)',
                  border: '1.5px solid rgba(203,230,200,0.5)', borderRadius: '20px', padding: '32px 36px 28px',
                  boxShadow: '0 24px 80px rgba(0,0,0,0.15), 0 4px 16px rgba(27,94,32,0.08)',
                }}
                initial={{ opacity: 0, scale: 0.92, y: 30 }}
                animate={{ opacity: 1, scale: 1, y: 0 }}
                exit={{ opacity: 0, scale: 0.92, y: 30 }}
                transition={{ duration: 0.3, ease: [0.23, 1, 0.32, 1] }}
                onClick={(e) => e.stopPropagation()}
              >
                <div style={{
                  display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                  marginBottom: '24px', paddingBottom: '16px', borderBottom: '1px solid rgba(203,230,200,0.4)',
                }}>
                  <div>
                    <div style={{ fontSize: '16px', fontWeight: 700, color: '#1B5E20', letterSpacing: '0.02em' }}>
                      Detected Shredder Systems
                    </div>
                    <div style={{ fontSize: '12px', fontWeight: 400, color: '#6B736B', marginTop: '2px' }}>
                      {count} system{count !== 1 ? 's' : ''} identified across all sensor groups
                    </div>
                  </div>
                  <motion.button
                    style={{
                      width: '32px', height: '32px', borderRadius: '10px',
                      background: 'rgba(27,94,32,0.08)', border: 'none', cursor: 'pointer',
                      display: 'flex', alignItems: 'center', justifyContent: 'center',
                      fontSize: '16px', color: '#6B736B', fontWeight: 600,
                    }}
                    onClick={() => setModalOpen(false)}
                    whileHover={{ background: 'rgba(27,94,32,0.15)' }}
                    whileTap={{ scale: 0.9 }}
                  >
                    X
                  </motion.button>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '14px' }}>
                  {systems && systems.map((sys, idx) => (
                    <DetectedSystemCard key={sys.system_id} sys={sys} idx={idx} />
                  ))}
                </div>
              </motion.div>
            </motion.div>
          )}
        </AnimatePresence>,
        document.body
      )}
    </>
  );
}

function SummaryCards({ summary, stats, systems }) {
  const gaugeColor = (val) => {
    if (val >= 0.8) return '#EF5350';
    if (val >= 0.55) return '#FFA726';
    return '#4CAF50';
  };

  const getTopContributor = () => {
    if (!stats) return null;
    // Only consider discovered subsystems (SYS_*), exclude physics
    const sysKeys = Object.keys(stats).filter((k) => k.startsWith('score_SYS_'));
    const subs = sysKeys.map((k) => ({ key: k, label: k.replace('score_', '').replace('_', ' ') }));
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

  // Filter out NORMAL from classification display -- NORMAL is never an alert classification
  const filteredClassDist = summary?.class_distribution
    ? Object.fromEntries(Object.entries(summary.class_distribution).filter(([cls]) => cls !== 'NORMAL'))
    : {};

  return (
    <div>
      <div style={styles.grid}>
        <GlassCard delay={0.05} intensity="strong">
          <div style={styles.statLabel}>
            Total Alerts
            <InfoTooltip text="Alert episodes are built from contiguous periods where the fused risk_score exceeds the medium (0.55) or high (0.80) threshold. Episodes shorter than 5 minutes are discarded, and gaps of 3 minutes or less are merged. Classified by dominant subsystem (SYS_1, SYS_2, etc.) or PROCESS when multiple subsystems are involved." />
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

        <DetectedSubsystemsCard systems={systems} />

        <GlassCard delay={0.15} intensity="strong">
          <div style={styles.statLabel}>
            Avg Signal Quality
            <InfoTooltip text="Signal Quality Score (SQS) per sensor per timestamp. Starts at 1.0 and is penalized multiplicatively for bounds violations, rate-of-change spikes, identity contradictions, and regression contradictions. Final SQS is clipped to [0,1]. This shows the mean across all sensors and timestamps." />
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
            <InfoTooltip text="Downtime is detected from electrical signals (kW, current, voltage, frequency). The machine is marked DOWNTIME when all four signals fall below their 2nd-percentile thresholds, or when both kW and current are off. During downtime all risk scores are gated to 0." right />
          </div>
          <div style={styles.statValue}>
            {summary?.running_pct !== null && summary?.running_pct !== undefined
              ? `${summary.running_pct}%`
              : '--'}
          </div>
          <div style={styles.statSub}>
            {stats?.running_count ?? '--'} running / {stats?.downtime_count ?? 0} downtime
          </div>
        </GlassCard>

        <GlassCard delay={0.25} intensity="strong">
          <div style={styles.statLabel}>
            Alert Classification
            <InfoTooltip text="Alert classification based on dominant subsystem. SYS_* is assigned when that subsystem's score dominates the episode. PROCESS is assigned when multiple subsystems contribute significantly. INSTRUMENT is assigned when the physics/identity score dominates with low subsystem scores. NORMAL timestamps are never classified as alerts." right />
          </div>
          <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap', marginTop: '4px' }}>
            {Object.entries(filteredClassDist).map(([cls, count]) => (
              <motion.span
                key={cls}
                style={{
                  ...styles.badge,
                  background: systemBgColor(cls) || '#F3E5F5',
                  color: classColor(cls),
                }}
                whileHover={{ scale: 1.05 }}
              >
                {cls} 
              </motion.span>
            ))}
          </div>
        </GlassCard>
      </div>

      <GlassCard delay={0.3} style={{ marginTop: '16px' }} intensity="strong">
        <div style={{ ...styles.statLabel, marginBottom: '12px' }}>
          Risk Gauges
          <InfoTooltip text="Risk score = dynamically weighted fusion of discovered subsystem scores (SYS_1, SYS_2, etc.) plus instrument validation score, multiplied by SQS confidence and downtime gate. During downtime, risk is forced to 0." />
        </div>
        <div style={styles.gaugeRow}>
          <GaugeWidget
            value={summary?.current_risk_score} label="Current Risk"
            color={gaugeColor(summary?.current_risk_score ?? 0)} size={130}
            tooltip="Latest timestamp risk_score value. Computed as dynamically weighted fusion of subsystem scores * SQS confidence * downtime gate."
            hoverDetail={topContrib ? `Top subsystem: ${topContrib.label} (mean ${topContrib.mean.toFixed(3)})` : null}
          />
          <GaugeWidget
            value={summary?.avg_risk_score} label="Average Risk"
            color={gaugeColor(summary?.avg_risk_score ?? 0)} size={130}
            tooltip="Mean risk_score across all timestamps in the dataset."
            hoverDetail={topContrib ? `Top subsystem: ${topContrib.label} (mean ${topContrib.mean.toFixed(3)})` : null}
          />
          <GaugeWidget
            value={summary?.max_risk_score} label="Peak Risk"
            color={gaugeColor(summary?.max_risk_score ?? 0)} size={130}
            tooltip="Maximum risk_score observed. HIGH severity is assigned when max_score >= 0.85 within an episode."
            hoverDetail={topContrib ? `Top subsystem: ${topContrib.label} (max ${(stats[topContrib.key]?.max ?? 0).toFixed(3)})` : null}
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