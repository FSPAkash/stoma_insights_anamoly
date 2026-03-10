import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { formatSensorName, systemColor, systemBgColor } from '../utils/formatters';

const styles = {
  pill: {
    display: 'inline-flex',
    alignItems: 'center',
    gap: '6px',
    padding: '6px 16px',
    background: 'rgba(255,255,255,0.7)',
    backdropFilter: 'blur(16px)',
    WebkitBackdropFilter: 'blur(16px)',
    border: '1.5px solid rgba(27,94,32,0.25)',
    borderRadius: '20px',
    fontSize: '12px',
    fontWeight: 600,
    color: '#1B5E20',
    cursor: 'pointer',
    transition: 'all 0.2s',
    boxShadow: '0 2px 8px rgba(27,94,32,0.06)',
  },
  pillCount: {
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    minWidth: '20px',
    height: '20px',
    borderRadius: '10px',
    background: 'rgba(27,94,32,0.12)',
    fontSize: '11px',
    fontWeight: 700,
    color: '#1B5E20',
    padding: '0 6px',
  },
  overlay: {
    position: 'fixed',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: 'rgba(0,0,0,0.35)',
    backdropFilter: 'blur(6px)',
    WebkitBackdropFilter: 'blur(6px)',
    zIndex: 9999,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    padding: '24px',
  },
  modal: {
    position: 'relative',
    width: '100%',
    maxWidth: '820px',
    maxHeight: '85vh',
    overflowY: 'auto',
    background: 'rgba(255,255,255,0.92)',
    backdropFilter: 'blur(28px)',
    WebkitBackdropFilter: 'blur(28px)',
    border: '1.5px solid rgba(203,230,200,0.5)',
    borderRadius: '20px',
    padding: '32px 36px 28px',
    boxShadow: '0 24px 80px rgba(0,0,0,0.15), 0 4px 16px rgba(27,94,32,0.08)',
  },
  modalHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '24px',
    paddingBottom: '16px',
    borderBottom: '1px solid rgba(203,230,200,0.4)',
  },
  modalTitle: {
    fontSize: '16px',
    fontWeight: 700,
    color: '#1B5E20',
    letterSpacing: '0.02em',
  },
  modalSubtitle: {
    fontSize: '12px',
    fontWeight: 400,
    color: '#6B736B',
    marginTop: '2px',
  },
  closeBtn: {
    width: '32px',
    height: '32px',
    borderRadius: '10px',
    background: 'rgba(27,94,32,0.08)',
    border: 'none',
    cursor: 'pointer',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    fontSize: '16px',
    color: '#6B736B',
    fontWeight: 600,
    transition: 'all 0.15s',
  },
  grid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
    gap: '14px',
  },
  card: {
    position: 'relative',
    background: 'rgba(255,255,255,0.55)',
    backdropFilter: 'blur(20px)',
    WebkitBackdropFilter: 'blur(20px)',
    borderRadius: '18px',
    border: '1.5px solid rgba(203,230,200,0.45)',
    padding: '20px 18px 18px',
    cursor: 'default',
    boxShadow: '0 2px 16px rgba(27,94,32,0.06), 0 1px 3px rgba(0,0,0,0.04)',
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'flex-start',
    minHeight: '120px',
  },
  label: {
    fontSize: '11px',
    fontWeight: 600,
    color: '#8A928A',
    textTransform: 'uppercase',
    letterSpacing: '0.06em',
    marginBottom: '8px',
  },
  value: (color) => ({
    fontSize: '22px',
    fontWeight: 700,
    color: color,
    lineHeight: 1.1,
    marginBottom: '6px',
    textTransform: 'capitalize',
  }),
  sub: {
    fontSize: '12px',
    color: '#6B736B',
    lineHeight: 1.4,
  },
  weightLine: {
    fontSize: '11px',
    color: '#8A928A',
    marginTop: '2px',
  },
  dot: (color) => ({
    position: 'absolute',
    top: '16px',
    right: '16px',
    width: '10px',
    height: '10px',
    borderRadius: '50%',
    background: color,
    boxShadow: `0 0 8px ${color}55`,
  }),
  sensorSection: {
    marginTop: '12px',
    paddingTop: '10px',
    borderTop: '1px solid rgba(203,230,200,0.3)',
    width: '100%',
  },
  sensorSectionTitle: {
    fontSize: '10px',
    fontWeight: 600,
    color: '#8A928A',
    textTransform: 'uppercase',
    letterSpacing: '0.06em',
    marginBottom: '8px',
  },
  sensorList: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: '5px',
  },
  sensorTag: (bgColor) => ({
    display: 'inline-block',
    padding: '3px 9px',
    borderRadius: '7px',
    fontSize: '10.5px',
    fontWeight: 500,
    background: bgColor,
    color: '#2D332D',
    border: '1px solid rgba(0,0,0,0.05)',
  }),
};

function DetectedSystemCard({ sys, idx }) {
  const color = systemColor(sys.system_id, idx);
  const bg = systemBgColor(sys.system_id);

  return (
    <motion.div
      style={styles.card}
      whileHover={{
        boxShadow: `0 8px 32px rgba(27,94,32,0.10), 0 2px 8px ${color}15`,
        borderColor: `${color}44`,
        y: -3,
      }}
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: idx * 0.06, duration: 0.4 }}
    >
      <div style={styles.dot(color)} />
      <div style={styles.label}>System Detected</div>
      <div style={styles.value(color)}>
        {sys.system_id.replace(/_/g, ' ')}
      </div>
      <div style={styles.sub}>
        {sys.sensor_count} sensor{sys.sensor_count !== 1 ? 's' : ''} active
      </div>
      {sys.fusion_weight != null && (
        <div style={styles.weightLine}>
          Weight {(sys.fusion_weight * 100).toFixed(1)}%
          {sys.quality && ` -- ${sys.quality.replace(/[^\w\s]/g, '').trim()}`}
        </div>
      )}

      {sys.sensors && sys.sensors.length > 0 && (
        <div style={styles.sensorSection}>
          <div style={styles.sensorSectionTitle}>Sensors</div>
          <div style={styles.sensorList}>
            {sys.sensors.map((s) => (
              <span key={s} style={styles.sensorTag(bg)}>
                {formatSensorName(s)}
              </span>
            ))}
          </div>
        </div>
      )}
    </motion.div>
  );
}

function DetectedSystems({ systems }) {
  const [modalOpen, setModalOpen] = useState(false);

  if (!systems || !systems.length) return null;

  return (
    <>
      <motion.button
        style={styles.pill}
        onClick={() => setModalOpen(true)}
        whileHover={{
          background: 'rgba(230,244,234,0.9)',
          borderColor: 'rgba(27,94,32,0.45)',
          boxShadow: '0 4px 16px rgba(27,94,32,0.12)',
          y: -1,
        }}
        whileTap={{ scale: 0.97 }}
      >
        <span>Check detected systems</span>
        <span style={styles.pillCount}>{systems.length}</span>
      </motion.button>

      <AnimatePresence>
        {modalOpen && (
          <motion.div
            style={styles.overlay}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
            onClick={(e) => {
              if (e.target === e.currentTarget) setModalOpen(false);
            }}
          >
            <motion.div
              style={styles.modal}
              initial={{ opacity: 0, scale: 0.92, y: 30 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.92, y: 30 }}
              transition={{ duration: 0.3, ease: [0.23, 1, 0.32, 1] }}
              onClick={(e) => e.stopPropagation()}
            >
              <div style={styles.modalHeader}>
                <div>
                  <div style={styles.modalTitle}>Detected Shredder Systems</div>
                  <div style={styles.modalSubtitle}>
                    {systems.length} system{systems.length !== 1 ? 's' : ''} identified across all sensor groups
                  </div>
                </div>
                <motion.button
                  style={styles.closeBtn}
                  onClick={() => setModalOpen(false)}
                  whileHover={{ background: 'rgba(27,94,32,0.15)' }}
                  whileTap={{ scale: 0.9 }}
                >
                  X
                </motion.button>
              </div>

              <div style={styles.grid}>
                {systems.map((sys, idx) => (
                  <DetectedSystemCard key={sys.system_id} sys={sys} idx={idx} />
                ))}
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );
}

export default DetectedSystems;
