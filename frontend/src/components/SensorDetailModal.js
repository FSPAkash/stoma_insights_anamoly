import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { getSensorDetail } from '../utils/api';
import { formatSensorName, formatScore, formatTimestamp, formatDuration } from '../utils/formatters';
import GaugeWidget from './GaugeWidget';

const styles = {
  overlay: {
    position: 'fixed',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: 'rgba(26, 31, 26, 0.4)',
    backdropFilter: 'blur(8px)',
    WebkitBackdropFilter: 'blur(8px)',
    zIndex: 2000,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    padding: '20px',
  },
  modal: {
    background: 'rgba(255, 255, 255, 0.92)',
    backdropFilter: 'blur(24px)',
    WebkitBackdropFilter: 'blur(24px)',
    borderRadius: '20px',
    border: '1px solid rgba(203, 230, 200, 0.5)',
    boxShadow: '0 24px 80px rgba(27,94,32,0.18)',
    width: '100%',
    maxWidth: '680px',
    maxHeight: '85vh',
    overflow: 'auto',
    padding: '32px',
  },
  header: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    marginBottom: '24px',
  },
  title: {
    fontSize: '18px',
    fontWeight: 600,
    color: '#1B5E20',
    letterSpacing: '-0.01em',
  },
  subtitle: {
    fontSize: '13px',
    color: '#6B736B',
    marginTop: '4px',
  },
  closeBtn: {
    width: '32px',
    height: '32px',
    borderRadius: '50%',
    border: '1px solid #CBE6C8',
    background: 'rgba(255,255,255,0.8)',
    cursor: 'pointer',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    fontSize: '14px',
    color: '#4A524A',
    transition: 'all 0.2s',
    flexShrink: 0,
  },
  section: {
    marginBottom: '24px',
  },
  sectionTitle: {
    fontSize: '12px',
    fontWeight: 500,
    color: '#6B736B',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    marginBottom: '12px',
    paddingBottom: '6px',
    borderBottom: '1px solid #E8ECE8',
  },
  configGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))',
    gap: '12px',
  },
  configItem: {
    padding: '10px 12px',
    background: '#F5F7F5',
    borderRadius: '10px',
    border: '1px solid #E8ECE8',
  },
  configLabel: {
    fontSize: '11px',
    color: '#8A928A',
    marginBottom: '4px',
  },
  configValue: {
    fontSize: '14px',
    fontWeight: 600,
    color: '#2D332D',
    fontVariantNumeric: 'tabular-nums',
  },
  alertRow: {
    padding: '10px 14px',
    background: '#F5F7F5',
    borderRadius: '10px',
    marginBottom: '6px',
    fontSize: '12px',
    display: 'grid',
    gridTemplateColumns: '1fr 1fr 1fr 1fr',
    gap: '8px',
  },
  loading: {
    textAlign: 'center',
    padding: '40px',
    color: '#8A928A',
    fontSize: '14px',
  },
};

function SensorDetailModal({ sensorId, onClose }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!sensorId) return;
    setLoading(true);
    getSensorDetail(sensorId)
      .then((res) => setData(res.data))
      .catch((err) => console.error(err))
      .finally(() => setLoading(false));
  }, [sensorId]);

  if (!sensorId) return null;

  return (
    <AnimatePresence>
      <motion.div
        style={styles.overlay}
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        onClick={onClose}
      >
        <motion.div
          style={styles.modal}
          initial={{ opacity: 0, scale: 0.95, y: 20 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.95, y: 20 }}
          transition={{ duration: 0.3, ease: [0.4, 0, 0.2, 1] }}
          onClick={(e) => e.stopPropagation()}
        >
          <div style={styles.header}>
            <div>
              <div style={styles.title}>{formatSensorName(sensorId)}</div>
              <div style={styles.subtitle}>{sensorId}</div>
            </div>
            <motion.button
              style={styles.closeBtn}
              onClick={onClose}
              whileHover={{ background: '#FFCDD2', borderColor: '#EF9A9A', color: '#C62828' }}
            >
              x
            </motion.button>
          </div>

          {loading ? (
            <div style={styles.loading}>Loading sensor data...</div>
          ) : data ? (
            <>
              {data.config && (
                <div style={styles.section}>
                  <div style={styles.sectionTitle}>Sensor Configuration</div>
                  <div style={styles.configGrid}>
                    {Object.entries(data.config)
                      .filter(([key]) => !['sensor', 'sensor_id'].includes(key))
                      .map(([key, val]) => (
                        <div key={key} style={styles.configItem}>
                          <div style={styles.configLabel}>
                            {key.replace(/_/g, ' ')}
                          </div>
                          <div style={styles.configValue}>
                            {val !== null && val !== undefined
                              ? typeof val === 'number'
                                ? Number(val).toFixed(4)
                                : String(val)
                              : '--'}
                          </div>
                        </div>
                      ))}
                  </div>
                  {data.config && (
                    <div
                      style={{
                        display: 'flex',
                        justifyContent: 'center',
                        gap: '24px',
                        marginTop: '16px',
                      }}
                    >
                      <GaugeWidget
                        value={data.config.missing_pct}
                        label="Missing %"
                        color={data.config.missing_pct > 0.15 ? '#EF5350' : '#4CAF50'}
                        size={100}
                      />
                      <GaugeWidget
                        value={data.config.variance ? Math.min(data.config.variance, 1) : 0}
                        label="Variance (norm)"
                        color="#388E3C"
                        size={100}
                      />
                    </div>
                  )}
                </div>
              )}

              {data.alerts && data.alerts.length > 0 && (
                <div style={styles.section}>
                  <div style={styles.sectionTitle}>
                    Alert Involvement ({data.alerts.length})
                  </div>
                  <div style={{ fontSize: '11px', color: '#8A928A', marginBottom: '8px' }}>
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr', gap: '8px', fontWeight: 500 }}>
                      <span>Time</span>
                      <span>Severity</span>
                      <span>Peak Score</span>
                      <span>Rank</span>
                    </div>
                  </div>
                  {data.alerts.slice(0, 20).map((a, i) => (
                    <div key={i} style={styles.alertRow}>
                      <span>{formatTimestamp(a.start_ts)}</span>
                      <span>
                        <span
                          style={{
                            padding: '2px 8px',
                            borderRadius: '10px',
                            fontSize: '10px',
                            fontWeight: 600,
                            background: a.severity === 'HIGH' ? '#FFCDD2' : '#FFE0B2',
                            color: a.severity === 'HIGH' ? '#C62828' : '#E65100',
                          }}
                        >
                          {a.severity}
                        </span>
                      </span>
                      <span style={{ fontVariantNumeric: 'tabular-nums' }}>
                        {formatScore(a.sensor_peak_score)}
                      </span>
                      <span style={{ fontVariantNumeric: 'tabular-nums' }}>
                        #{a.sensor_rank}
                      </span>
                    </div>
                  ))}
                </div>
              )}

              {data.risk_profile && data.risk_profile.length > 0 && (
                <div style={styles.section}>
                  <div style={styles.sectionTitle}>
                    Risk Decomposition Profile (last {Math.min(data.risk_profile.length, 500)} points)
                  </div>
                  <div
                    style={{
                      maxHeight: '200px',
                      overflow: 'auto',
                      fontSize: '11px',
                    }}
                  >
                    <table style={{ width: '100%', borderCollapse: 'separate', borderSpacing: '0 2px' }}>
                      <thead>
                        <tr>
                          <th style={{ textAlign: 'left', padding: '4px 8px', color: '#8A928A', fontSize: '10px' }}>Time</th>
                          <th style={{ textAlign: 'left', padding: '4px 8px', color: '#8A928A', fontSize: '10px' }}>Subsystem</th>
                          <th style={{ textAlign: 'right', padding: '4px 8px', color: '#8A928A', fontSize: '10px' }}>Risk Component</th>
                          <th style={{ textAlign: 'right', padding: '4px 8px', color: '#8A928A', fontSize: '10px' }}>Confidence</th>
                        </tr>
                      </thead>
                      <tbody>
                        {data.risk_profile.slice(0, 100).map((r, i) => (
                          <tr key={i} style={{ background: i % 2 === 0 ? '#F5F7F5' : 'transparent' }}>
                            <td style={{ padding: '3px 8px', fontVariantNumeric: 'tabular-nums' }}>
                              {r.timestamp_utc ? String(r.timestamp_utc).substring(11, 16) : '--'}
                            </td>
                            <td style={{ padding: '3px 8px' }}>{r.subsystem}</td>
                            <td style={{ padding: '3px 8px', textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                              {formatScore(r.risk_score_component, 5)}
                            </td>
                            <td style={{ padding: '3px 8px', textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                              {formatScore(r.confidence_factor, 3)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
          ) : (
            <div style={styles.loading}>No data available for this sensor.</div>
          )}
        </motion.div>
      </motion.div>
    </AnimatePresence>
  );
}

export default SensorDetailModal;