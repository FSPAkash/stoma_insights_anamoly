import React, { useState, useEffect } from 'react';
import ReactDOM from 'react-dom';
import { motion, AnimatePresence } from 'framer-motion';
import GlassCard from './GlassCard';
import InfoTooltip from './InfoTooltip';
import { getBetaOverview, getBetaInvalidSensors, getBetaSubsystems } from '../utils/api';
import { systemColor, systemBgColor, formatSensorName } from '../utils/formatters';

const styles = {
  grid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(5, 1fr)',
    gap: '16px',
  },
  cardInner: {
    height: '160px',
    overflowY: 'auto',
    padding: '20px',
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
};

function InvalidSensorsModal({ open, onClose, sensors, thresholdPct }) {
  if (!open) return null;
  return ReactDOM.createPortal(
    <AnimatePresence>
      {open && (
        <motion.div
          style={{
            position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
            background: 'rgba(0,0,0,0.35)', backdropFilter: 'blur(6px)', WebkitBackdropFilter: 'blur(6px)',
            zIndex: 9999, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '24px',
          }}
          initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
          onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
        >
          <motion.div
            style={{
              position: 'relative', width: '100%', maxWidth: '600px', maxHeight: '80vh', overflowY: 'auto',
              background: 'rgba(255,255,255,0.92)', backdropFilter: 'blur(28px)', WebkitBackdropFilter: 'blur(28px)',
              border: '1.5px solid rgba(203,230,200,0.5)', borderRadius: '20px', padding: '28px 32px',
              boxShadow: '0 24px 80px rgba(0,0,0,0.15)',
            }}
            initial={{ opacity: 0, scale: 0.92, y: 30 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.92, y: 30 }}
            onClick={(e) => e.stopPropagation()}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px', paddingBottom: '14px', borderBottom: '1px solid rgba(203,230,200,0.4)' }}>
              <div>
                <div style={{ fontSize: '16px', fontWeight: 700, color: '#1B5E20' }}>Invalid Sensors</div>
                <div style={{ fontSize: '12px', color: '#6B736B', marginTop: '2px' }}>
                  Sensors with >{thresholdPct}% missing/invalid data
                </div>
              </div>
              <motion.button
                style={{ width: '32px', height: '32px', borderRadius: '10px', background: 'rgba(27,94,32,0.08)', border: 'none', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '16px', color: '#6B736B', fontWeight: 600 }}
                onClick={onClose} whileHover={{ background: 'rgba(27,94,32,0.15)' }} whileTap={{ scale: 0.9 }}
              >X</motion.button>
            </div>
            {sensors.length === 0 ? (
              <div style={{ textAlign: 'center', padding: '32px', color: '#8A928A', fontSize: '14px' }}>
                All sensors within acceptable data quality thresholds.
              </div>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                {sensors.map((s, i) => (
                  <div key={s.sensor} style={{
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                    padding: '10px 14px', borderRadius: '12px',
                    background: s.missing_pct > 50 ? 'rgba(239,83,80,0.06)' : 'rgba(255,167,38,0.06)',
                    border: `1px solid ${s.missing_pct > 50 ? 'rgba(239,83,80,0.15)' : 'rgba(255,167,38,0.15)'}`,
                  }}>
                    <div>
                      <div style={{ fontSize: '13px', fontWeight: 500, color: '#2D332D' }}>{formatSensorName(s.sensor)}</div>
                      <div style={{ fontSize: '11px', color: '#8A928A', marginTop: '2px' }}>
                        {s.n_running > 0 ? `${s.n_running} running pts` : 'No running data'}
                      </div>
                    </div>
                    <div style={{
                      fontSize: '13px', fontWeight: 600,
                      color: s.missing_pct > 50 ? '#C62828' : '#E65100',
                    }}>
                      {s.missing_pct}% invalid
                    </div>
                  </div>
                ))}
              </div>
            )}
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>,
    document.body
  );
}

function SubsystemDetectedModal({ open, onClose, subsystems }) {
  if (!open) return null;
  return ReactDOM.createPortal(
    <AnimatePresence>
      {open && (
        <motion.div
          style={{
            position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
            background: 'rgba(0,0,0,0.35)', backdropFilter: 'blur(6px)', WebkitBackdropFilter: 'blur(6px)',
            zIndex: 9999, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '24px',
          }}
          initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
          onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
        >
          <motion.div
            style={{
              position: 'relative', width: '100%', maxWidth: '820px', maxHeight: '85vh', overflowY: 'auto',
              background: 'rgba(255,255,255,0.92)', backdropFilter: 'blur(28px)', WebkitBackdropFilter: 'blur(28px)',
              border: '1.5px solid rgba(203,230,200,0.5)', borderRadius: '20px', padding: '32px 36px 28px',
              boxShadow: '0 24px 80px rgba(0,0,0,0.15)',
            }}
            initial={{ opacity: 0, scale: 0.92, y: 30 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.92, y: 30 }}
            onClick={(e) => e.stopPropagation()}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '24px', paddingBottom: '16px', borderBottom: '1px solid rgba(203,230,200,0.4)' }}>
              <div>
                <div style={{ fontSize: '16px', fontWeight: 700, color: '#1B5E20' }}>Detected Subsystems</div>
                <div style={{ fontSize: '12px', color: '#6B736B', marginTop: '2px' }}>
                  {subsystems.length} subsystem{subsystems.length !== 1 ? 's' : ''} identified via hierarchical clustering
                </div>
              </div>
              <motion.button
                style={{ width: '32px', height: '32px', borderRadius: '10px', background: 'rgba(27,94,32,0.08)', border: 'none', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '16px', color: '#6B736B', fontWeight: 600 }}
                onClick={onClose} whileHover={{ background: 'rgba(27,94,32,0.15)' }} whileTap={{ scale: 0.9 }}
              >X</motion.button>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '14px' }}>
              {subsystems.map((sys, idx) => {
                const color = systemColor(sys.system_id, idx);
                const bg = systemBgColor(sys.system_id);
                return (
                  <motion.div
                    key={sys.system_id}
                    style={{
                      background: 'rgba(255,255,255,0.55)', backdropFilter: 'blur(20px)',
                      borderRadius: '18px', border: '1.5px solid rgba(203,230,200,0.45)',
                      padding: '20px 18px 18px', display: 'flex', flexDirection: 'column',
                    }}
                    initial={{ opacity: 0, y: 16 }} animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: idx * 0.06, duration: 0.4 }}
                  >
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
                      <div style={{ width: 10, height: 10, borderRadius: '50%', background: color, boxShadow: `0 0 8px ${color}55` }} />
                      <div style={{ fontSize: '18px', fontWeight: 700, color, textTransform: 'capitalize' }}>
                        {sys.system_id.replace(/_/g, ' ')}
                      </div>
                    </div>
                    <div style={{ fontSize: '12px', color: '#6B736B' }}>
                      {sys.sensor_count} sensor{sys.sensor_count !== 1 ? 's' : ''}
                      {sys.fusion_weight != null && ` | Weight ${(sys.fusion_weight * 100).toFixed(1)}%`}
                    </div>

                    {sys.sensors && sys.sensors.length > 0 && (
                      <div style={{ marginTop: '12px', paddingTop: '10px', borderTop: '1px solid rgba(203,230,200,0.3)' }}>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                          {sys.sensors.map((s) => (
                            <span key={s} style={{
                              display: 'inline-block', padding: '2px 8px', borderRadius: '7px',
                              fontSize: '10px', fontWeight: 500, background: bg, color: '#2D332D',
                              border: '1px solid rgba(0,0,0,0.05)',
                            }}>
                              {formatSensorName(s)}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}
                  </motion.div>
                );
              })}
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>,
    document.body
  );
}

function BetaOverviewCards() {
  const [overview, setOverview] = useState(null);
  const [invalidSensors, setInvalidSensors] = useState([]);
  const [invalidThreshold, setInvalidThreshold] = useState(10);
  const [subsystems, setSubsystems] = useState([]);
  const [showInvalid, setShowInvalid] = useState(false);
  const [showSystems, setShowSystems] = useState(false);

  useEffect(() => {
    getBetaOverview().then(res => setOverview(res.data)).catch(() => {});
    getBetaInvalidSensors(0.10).then(res => {
      setInvalidSensors(res.data.sensors || []);
      setInvalidThreshold(res.data.threshold_pct || 10);
    }).catch(() => {});
    getBetaSubsystems().then(res => setSubsystems(res.data.subsystems || [])).catch(() => {});
  }, []);

  const nonIsolated = subsystems.filter(s => s.system_id !== 'ISOLATED');

  return (
    <div>
      <div style={styles.grid}>
        {/* System Selected */}
        <GlassCard delay={0.05} intensity="strong" padding="0">
          <div style={styles.cardInner}>
            <div style={styles.statLabel}>
              System Selected
              <InfoTooltip text="The industrial system currently being monitored. All sensors belong to this system." />
            </div>
            <div style={styles.statValue}>{overview?.system_name || 'Shredder'}</div>
            <div style={styles.statSub}>
              {overview?.data_range?.start && (
                <span>{String(overview.data_range.start).substring(0, 10)} to {String(overview.data_range.end).substring(0, 10)}</span>
              )}
            </div>
          </div>
        </GlassCard>

        {/* Sensors Found */}
        <GlassCard delay={0.1} intensity="strong" padding="0">
          <div style={styles.cardInner}>
            <div style={styles.statLabel}>
              Sensors Found
              <InfoTooltip text="Total unique sensor channels discovered in the dataset after pivot and cleaning." />
            </div>
            <div style={styles.statValue}>{overview?.total_sensors ?? '--'}</div>
            <div style={styles.statSub}>active sensor channels</div>
          </div>
        </GlassCard>

        {/* Downtime Detected */}
        <GlassCard delay={0.15} intensity="strong" padding="0">
          <div style={styles.cardInner}>
            <div style={styles.statLabel}>
              Downtime Detected
              <InfoTooltip text="Downtime is detected when electrical signals (kW, current, voltage, frequency) fall below auto-computed thresholds. All risk scores are gated to 0 during downtime." />
            </div>
            <div style={styles.statValue}>
              {overview?.downtime_pct !== undefined ? `${overview.downtime_pct}%` : '--'}
            </div>
            <div style={styles.statSub}>
              {overview ? `${overview.downtime_minutes?.toLocaleString()} / ${overview.total_minutes?.toLocaleString()} min` : '--'}
            </div>
            <div style={{
              marginTop: '8px', height: '6px', borderRadius: '3px', background: '#E8ECE8', overflow: 'hidden',
            }}>
              <div style={{
                height: '100%', borderRadius: '3px',
                width: `${overview?.running_pct || 0}%`,
                background: 'linear-gradient(90deg, #81C784, #4CAF50)',
                transition: 'width 1s ease',
              }} />
            </div>
            <div style={{ fontSize: '11px', color: '#4CAF50', marginTop: '4px', fontWeight: 500 }}>
              {overview?.running_pct ?? '--'}% running
            </div>
          </div>
        </GlassCard>

        {/* Invalid Flagged */}
        <GlassCard delay={0.2} intensity="strong" padding="0">
          <div style={styles.cardInner}>
          <div style={styles.statLabel}>
            Invalid Flagged
            <InfoTooltip text="Sensors where invalid/missing data exceeds the threshold. These sensors may have unreliable readings during the evaluation window." />
          </div>
          <div style={styles.statValue}>{invalidSensors.length}</div>
          <div style={styles.statSub}>sensors with >{invalidThreshold}% missing</div>
          {invalidSensors.length > 0 && (
            <motion.button
              onClick={() => setShowInvalid(true)}
              style={{
                marginTop: '8px', display: 'inline-flex', alignItems: 'center', gap: '6px',
                padding: '5px 14px', background: 'rgba(239,83,80,0.08)',
                border: '1.5px solid rgba(239,83,80,0.2)', borderRadius: '16px',
                fontSize: '11px', fontWeight: 600, color: '#C62828', cursor: 'pointer',
              }}
              whileHover={{ background: 'rgba(239,83,80,0.14)', borderColor: 'rgba(239,83,80,0.35)' }}
              whileTap={{ scale: 0.97 }}
            >
              View sensors
            </motion.button>
          )}
          </div>
        </GlassCard>

        {/* Subsystems Detected */}
        <GlassCard delay={0.25} intensity="strong" padding="0">
          <div style={styles.cardInner}>
          <div style={styles.statLabel}>
            Subsystems Detected
            <InfoTooltip text="Subsystems discovered via hierarchical correlation clustering. Each groups sensors with correlated behavior. Isolated sensors don't cluster with any group." />
          </div>
          <div style={styles.statValue}>{nonIsolated.length || '--'}</div>
          <div style={styles.statSub}>
            {subsystems.find(s => s.system_id === 'ISOLATED')
              ? `+ ${subsystems.find(s => s.system_id === 'ISOLATED').sensor_count} isolated`
              : ''}
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px', marginTop: '8px' }}>
            {nonIsolated.map((sys, idx) => (
              <span key={sys.system_id} style={{
                display: 'inline-block', padding: '3px 10px', borderRadius: '12px',
                fontSize: '11px', fontWeight: 600,
                background: systemBgColor(sys.system_id),
                color: systemColor(sys.system_id, idx),
              }}>
                {sys.system_id.replace('_', ' ')} ({sys.sensor_count})
              </span>
            ))}
          </div>
          <motion.button
            onClick={() => setShowSystems(true)}
            style={{
              marginTop: '8px', display: 'inline-flex', alignItems: 'center', gap: '6px',
              padding: '5px 14px', background: 'rgba(27,94,32,0.08)',
              border: '1.5px solid rgba(27,94,32,0.2)', borderRadius: '16px',
              fontSize: '11px', fontWeight: 600, color: '#1B5E20', cursor: 'pointer',
            }}
            whileHover={{ background: 'rgba(230,244,234,0.9)', borderColor: 'rgba(27,94,32,0.45)' }}
            whileTap={{ scale: 0.97 }}
          >
            Check subsystems
          </motion.button>
          </div>
        </GlassCard>
      </div>

      <InvalidSensorsModal open={showInvalid} onClose={() => setShowInvalid(false)} sensors={invalidSensors} thresholdPct={invalidThreshold} />
      <SubsystemDetectedModal open={showSystems} onClose={() => setShowSystems(false)} subsystems={subsystems} />
    </div>
  );
}

export default BetaOverviewCards;
