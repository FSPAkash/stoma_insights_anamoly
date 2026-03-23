import React, { useMemo } from 'react';
import { motion } from 'framer-motion';
import { formatSensorName, formatScore } from '../utils/formatters';

const styles = {
  container: {
    marginTop: '20px',
  },
  heading: {
    fontSize: '13px',
    fontWeight: 600,
    color: '#1B5E20',
    marginBottom: '16px',
  },
  flowRow: {
    display: 'flex',
    alignItems: 'stretch',
    gap: '0',
    marginBottom: '2px',
    width: '100%',
  },
  subsystemBlock: (color, width) => ({
    width: `${Math.max(width, 8)}%`,
    minWidth: '80px',
    padding: '12px 14px',
    background: color,
    borderRadius: '10px 0 0 10px',
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'center',
    transition: 'all 0.3s ease',
  }),
  flowConnector: {
    width: '20px',
    position: 'relative',
    overflow: 'visible',
  },
  sensorBlock: {
    flex: 1,
    padding: '8px',
    display: 'flex',
    flexWrap: 'wrap',
    gap: '6px',
    alignItems: 'center',
    minHeight: '56px',
  },
  sensorChip: (intensity) => ({
    padding: '6px 12px',
    borderRadius: '8px',
    fontSize: '11px',
    fontWeight: 500,
    background: `rgba(27, 94, 32, ${Math.max(intensity * 0.15, 0.04)})`,
    border: `1px solid rgba(27, 94, 32, ${Math.max(intensity * 0.3, 0.08)})`,
    color: '#1B5E20',
    cursor: 'pointer',
    transition: 'all 0.2s ease',
    display: 'flex',
    flexDirection: 'column',
    gap: '2px',
  }),
  chipScore: {
    fontSize: '10px',
    color: '#6B736B',
    fontVariantNumeric: 'tabular-nums',
  },
  subLabel: {
    fontSize: '12px',
    fontWeight: 600,
    color: '#FFFFFF',
    letterSpacing: '0.02em',
  },
  subScore: {
    fontSize: '11px',
    color: 'rgba(255,255,255,0.85)',
    fontVariantNumeric: 'tabular-nums',
    marginTop: '2px',
  },
  tooltipOverlay: {
    position: 'absolute',
    bottom: '100%',
    left: '50%',
    transform: 'translateX(-50%)',
    background: 'rgba(255,255,255,0.95)',
    backdropFilter: 'blur(16px)',
    border: '1px solid rgba(203,230,200,0.5)',
    borderRadius: '10px',
    padding: '10px 14px',
    boxShadow: '0 8px 24px rgba(27,94,32,0.12)',
    whiteSpace: 'nowrap',
    zIndex: 50,
    fontSize: '12px',
  },
};

const subsystemColors = {
  MECH: 'linear-gradient(135deg, #1B5E20, #388E3C)',
  ELEC: 'linear-gradient(135deg, #388E3C, #4CAF50)',
  THERM: 'linear-gradient(135deg, #E65100, #FFA726)',
  INSTRUMENT: 'linear-gradient(135deg, #0D47A1, #1565C0)',
  SYS_1: 'linear-gradient(135deg, #1B5E20, #388E3C)',
  SYS_2: 'linear-gradient(135deg, #0D47A1, #1976D2)',
  SYS_3: 'linear-gradient(135deg, #E65100, #FB8C00)',
  SYS_4: 'linear-gradient(135deg, #4A148C, #7B1FA2)',
  ISOLATED: 'linear-gradient(135deg, #546E7A, #78909C)',
};

function SensorFlowDecomposition({ flowData, onSensorClick, alertClass, interactive = true }) {
  const [hoveredSensor, setHoveredSensor] = React.useState(null);

  const subsystemSensors = useMemo(() => {
    if (!flowData?.sensors) return {};
    const grouped = {};
    flowData.sensors.forEach((s) => {
      const sub = s.subsystem || 'UNKNOWN';
      if (!grouped[sub]) grouped[sub] = [];
      grouped[sub].push(s);
    });
    Object.keys(grouped).forEach((key) => {
      grouped[key].sort((a, b) => (b.total_risk || 0) - (a.total_risk || 0));
    });
    return grouped;
  }, [flowData]);

  const totalRisk = useMemo(() => {
    if (!flowData?.subsystems) return 1;
    return Object.values(flowData.subsystems).reduce((sum, s) => sum + (s.total_risk || 0), 0) || 1;
  }, [flowData]);

  if (!flowData || !flowData.subsystems) {
    return (
      <div style={styles.container}>
        <div style={styles.heading}>Sensor Decomposition</div>
        <div style={{ color: '#8A928A', fontSize: '13px', padding: '16px 0' }}>
          No decomposition data available for this episode.
        </div>
      </div>
    );
  }

  // Filter subsystems based on alert class
  let subsystems = Object.entries(flowData.subsystems)
    .sort(([, a], [, b]) => (b.total_risk || 0) - (a.total_risk || 0));

  if (alertClass && alertClass !== 'PROCESS' && alertClass !== 'NORMAL') {
    // For system-specific alerts, only show that subsystem
    subsystems = subsystems.filter(([name]) => name === alertClass);
  } else if (alertClass === 'PROCESS') {
    // For PROCESS alerts, show subsystems with meaningful risk contribution (> 1% of total)
    const topRisk = subsystems.length ? subsystems[0][1].total_risk || 0 : 0;
    if (topRisk > 0) {
      subsystems = subsystems.filter(([, data]) => (data.total_risk || 0) / topRisk > 0.05);
    }
  }

  return (
    <div style={styles.container}>
      <div style={styles.heading}>
        Risk Flow: {alertClass && alertClass !== 'PROCESS' ? `${alertClass.replace('_', ' ')} Breakdown` : 'Alert Episode to Sensor Level'}
      </div>
      <div
        style={{
          fontSize: '11px',
          color: '#8A928A',
          marginBottom: '16px',
        }}
      >
        {alertClass && alertClass !== 'PROCESS'
          ? `Sensor risk decomposition for the ${alertClass.replace('_', ' ')} subsystem during this alert episode.${interactive ? ' Click a sensor for detailed view.' : ''}`
          : `Each row shows subsystem contribution flowing into individual sensor risk components.${interactive ? ' Click a sensor for detailed view.' : ''}`}
      </div>

      {subsystems.map(([subsystem, data], idx) => {
        const sensors = subsystemSensors[subsystem] || [];
        const subWidth = ((data.total_risk || 0) / totalRisk) * 60 + 15;
        const maxSensorRisk = sensors.length
          ? Math.max(...sensors.map((s) => s.total_risk || 0))
          : 1;

        return (
          <motion.div
            key={subsystem}
            style={styles.flowRow}
            initial={{ opacity: 0, x: -20 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ delay: idx * 0.1, duration: 0.4 }}
          >
            <div style={styles.subsystemBlock(subsystemColors[subsystem] || subsystemColors.MECH, subWidth)}>
              <div style={styles.subLabel}>{subsystem}</div>
              <div style={styles.subScore}>
                Total: {formatScore(data.total_risk, 4)}
              </div>
              <div style={styles.subScore}>
                Sensors: {data.sensor_count || 0}
              </div>
            </div>

            <div style={styles.flowConnector}>
              <svg width="20" height="100%" style={{ position: 'absolute', top: 0, left: 0 }}>
                <line
                  x1="0"
                  y1="50%"
                  x2="20"
                  y2="50%"
                  stroke="#CBE6C8"
                  strokeWidth="2"
                  strokeDasharray="4 2"
                />
                <polygon points="16,45% 20,50% 16,55%" fill="#A8DCA8" />
              </svg>
            </div>

            <div style={styles.sensorBlock}>
              {sensors.slice(0, 12).map((sensor, si) => {
                const intensity = maxSensorRisk > 0 ? (sensor.total_risk || 0) / maxSensorRisk : 0;
                return (
                  <motion.div
                    key={sensor.sensor_id || si}
                    style={{
                      ...styles.sensorChip(intensity),
                      position: 'relative',
                      cursor: interactive ? 'pointer' : 'default',
                    }}
                    whileHover={{
                      scale: 1.05,
                      boxShadow: '0 4px 12px rgba(27,94,32,0.12)',
                      borderColor: '#4CAF50',
                    }}
                    onMouseEnter={() => setHoveredSensor(sensor.sensor_id)}
                    onMouseLeave={() => setHoveredSensor(null)}
                    onClick={() => interactive && onSensorClick && onSensorClick(sensor.sensor_id)}
                  >
                    <span>{formatSensorName(sensor.sensor_id)}</span>
                    <span style={styles.chipScore}>
                      Risk: {formatScore(sensor.total_risk, 4)}
                    </span>
                    {hoveredSensor === sensor.sensor_id && (
                      <div style={styles.tooltipOverlay}>
                        <div style={{ fontWeight: 600, color: '#1B5E20', marginBottom: '4px' }}>
                          {formatSensorName(sensor.sensor_id)}
                        </div>
                        <div>Mean Risk: {formatScore(sensor.mean_risk, 5)}</div>
                        <div>Max Risk: {formatScore(sensor.max_risk, 5)}</div>
                        <div>Avg Base: {formatScore(sensor.mean_base, 5)}</div>
                        <div>Avg Uplift: {formatScore(sensor.mean_uplift, 5)}</div>
                        <div>Confidence: {formatScore(sensor.mean_confidence, 3)}</div>
                      </div>
                    )}
                  </motion.div>
                );
              })}
              {sensors.length > 12 && (
                <span
                  style={{
                    fontSize: '11px',
                    color: '#8A928A',
                    padding: '6px',
                  }}
                >
                  +{sensors.length - 12} more
                </span>
              )}
            </div>
          </motion.div>
        );
      })}
    </div>
  );
}

export default SensorFlowDecomposition;
