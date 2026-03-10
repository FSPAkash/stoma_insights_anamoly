import React from 'react';
import GlassCard from './GlassCard';
import InfoTooltip from './InfoTooltip';

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
  row: {
    display: 'flex',
    gap: '24px',
    flexWrap: 'wrap',
  },
  stat: {
    display: 'flex',
    flexDirection: 'column',
    gap: '4px',
  },
  statLabel: {
    fontSize: '12px',
    color: '#8A928A',
    display: 'flex',
    alignItems: 'center',
  },
  statValue: {
    fontSize: '20px',
    fontWeight: 600,
    color: '#1B5E20',
  },
  bar: {
    marginTop: '16px',
    height: '8px',
    borderRadius: '4px',
    background: '#E8ECE8',
    overflow: 'hidden',
  },
  barFill: {
    height: '100%',
    borderRadius: '4px',
    background: 'linear-gradient(90deg, #81C784, #4CAF50)',
    transition: 'width 1s ease',
  },
  note: {
    marginTop: '12px',
    fontSize: '12px',
    color: '#6B736B',
    lineHeight: 1.6,
    padding: '12px 16px',
    background: 'rgba(230, 244, 234, 0.5)',
    borderRadius: '10px',
    border: '1px solid rgba(168, 220, 168, 0.3)',
  },
};

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

function NormalBehaviorPanel({ normalData, filterLabel, onFilterClick }) {
  if (!normalData) return null;

  const normalPct = normalData.normal_stats?.normal_pct ?? 0;

  return (
    <GlassCard delay={0.4} style={{ marginTop: '16px' }} intensity="strong">
      <div style={styles.heading}>
        Normal Sensor Behavior
        <InfoTooltip text="NORMAL classification is assigned when the fused subsystem_score falls below the medium threshold (0.55). These timestamps represent healthy sensor behavior with no detected drift, periodicity anomalies, physics violations, or multivariate reconstruction errors. Normal periods are excluded from alert episodes." />
        {filterLabel && <span style={{ ...filterBadgeStyle, cursor: 'pointer' }} onClick={onFilterClick}>{filterLabel}</span>}
      </div>
      <div style={styles.row}>
        <div style={styles.stat}>
          <div style={styles.statLabel}>
            Normal Periods
            <InfoTooltip text="Percentage of all timestamps classified as NORMAL. Higher values indicate overall system stability." />
          </div>
          <div style={styles.statValue}>{normalPct !== null ? `${normalPct}%` : '--'}</div>
        </div>
        <div style={styles.stat}>
          <div style={styles.statLabel}>Normal Datapoints</div>
          <div style={styles.statValue}>
            {normalData.normal_stats?.normal_count?.toLocaleString() ?? '--'}
          </div>
        </div>
        <div style={styles.stat}>
          <div style={styles.statLabel}>
            Avg Risk (Normal)
            <InfoTooltip text="Average risk_score during NORMAL periods. Should be very low, confirming that the classification correctly identifies healthy operation." />
          </div>
          <div style={styles.statValue}>
            {normalData.normal_stats?.avg_risk_during_normal !== null &&
            normalData.normal_stats?.avg_risk_during_normal !== undefined
              ? Number(normalData.normal_stats.avg_risk_during_normal).toFixed(4)
              : '--'}
          </div>
        </div>
        <div style={styles.stat}>
          <div style={styles.statLabel}>
            Avg SQS (Normal)
            <InfoTooltip text="Average Signal Quality Score during normal periods. High SQS during normal operation confirms sensor reliability." />
          </div>
          <div style={styles.statValue}>
            {normalData.normal_stats?.avg_sqs_during_normal !== null &&
            normalData.normal_stats?.avg_sqs_during_normal !== undefined
              ? Number(normalData.normal_stats.avg_sqs_during_normal).toFixed(4)
              : '--'}
          </div>
        </div>
      </div>
      <div style={styles.bar}>
        <div style={{ ...styles.barFill, width: `${normalPct}%` }} />
      </div>

    </GlassCard>
  );
}

export default NormalBehaviorPanel;