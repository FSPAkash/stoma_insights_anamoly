import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import AnalogClock from './AnalogClock';
import InfoTooltip from './InfoTooltip';

const LOGO_SRC = '/stoma.png';
const FS_LOGO_SRC = '/FS.png';

const keyframesStyle = `
@keyframes subtleGradientShift {
  0% {
    background-position: 0% 50%;
  }
  50% {
    background-position: 100% 50%;
  }
  100% {
    background-position: 0% 50%;
  }
}
`;

const styles = {
  bar: {
    position: 'sticky',
    top: 0,
    zIndex: 100,
    background: 'linear-gradient(135deg, rgba(255,255,255,0.65) 0%, rgba(27,94,32,0.08) 50%, rgba(255,255,255,0.65) 100%)',
    backgroundSize: '400% 400%',
    animation: 'subtleGradientShift 12s ease infinite',
    backdropFilter: 'blur(28px)',
    WebkitBackdropFilter: 'blur(28px)',
    borderBottom: '1px solid rgba(203, 230, 200, 0.35)',
    padding: '0 36px',
    height: '72px',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
  },
  left: {
    display: 'flex',
    alignItems: 'center',
    gap: '14px',
  },
  logo: {
    width: '46px',
    height: '46px',
  },
  brand: {
    fontSize: '21px',
    fontWeight: 600,
    color: '#1B5E20',
    letterSpacing: '-0.02em',
  },
  divider: {
    width: '1px',
    height: '28px',
    background: '#CBE6C8',
    margin: '0 10px',
  },
  pageTitle: {
    fontSize: '16px',
    fontWeight: 500,
    color: '#6B736B',
  },
  right: {
    display: 'flex',
    alignItems: 'center',
    gap: '24px',
  },
  clockWrap: {
    display: 'flex',
    alignItems: 'center',
    gap: '12px',
  },
  timeContainer: {
    display: 'flex',
    alignItems: 'center',
    gap: '6px',
  },
  timeText: {
    fontSize: '15px',
    fontWeight: 500,
    color: '#4A524A',
    fontVariantNumeric: 'tabular-nums',
  },
  utcLabel: {
    fontSize: '10px',
    fontWeight: 600,
    color: '#1B5E20',
    background: 'rgba(27, 94, 32, 0.1)',
    padding: '2px 5px',
    borderRadius: '3px',
    letterSpacing: '0.5px',
  },
  userWrap: {
    display: 'flex',
    alignItems: 'center',
    gap: '12px',
  },
  avatar: {
    width: '36px',
    height: '36px',
    borderRadius: '50%',
    background: 'linear-gradient(135deg, #1B5E20, #4CAF50)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    color: '#FFFFFF',
    fontSize: '15px',
    fontWeight: 600,
  },
  userName: {
    fontSize: '16px',
    fontWeight: 500,
    color: '#2D332D',
  },
  logoutBtn: {
    padding: '6px 14px',
    background: 'transparent',
    border: '1.5px solid #CBE6C8',
    borderRadius: '8px',
    fontSize: '13px',
    fontWeight: 500,
    color: '#4A524A',
    cursor: 'pointer',
    transition: 'all 0.2s ease',
  },
};

// Helper function to create a UTC Date object for the analog clock
function getUTCDate(date) {
  return new Date(
    date.getUTCFullYear(),
    date.getUTCMonth(),
    date.getUTCDate(),
    date.getUTCHours(),
    date.getUTCMinutes(),
    date.getUTCSeconds()
  );
}

function TopBar({ user, onLogout }) {
  const [time, setTime] = useState(new Date());

  useEffect(() => {
    const timer = setInterval(() => setTime(new Date()), 1000);
    return () => clearInterval(timer);
  }, []);

  // Create a UTC time object for the analog clock
  const utcTime = getUTCDate(time);

  // Format UTC time string
  const utcTimeString = time.toLocaleTimeString('en-US', {
    hour12: false,
    timeZone: 'UTC',
  });

  return (
    <>
      <style>{keyframesStyle}</style>
      <motion.div
        style={styles.bar}
        initial={{ opacity: 0, y: -10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4 }}
      >
        <div style={styles.left}>
          <img src={LOGO_SRC} alt="Stoma" style={styles.logo} />
          <div style={{ display: 'flex', flexDirection: 'column' }}>
            <span style={styles.brand}>Stoma Insights</span>
            <div style={{ display: 'flex', alignItems: 'center', gap: '5px', opacity: 0.55 }}>
              <span style={{ fontSize: '10px', color: '#9E9E9E', fontWeight: 400 }}>Powered by</span>
              <img src={FS_LOGO_SRC} alt="Findability Sciences" style={{ height: '14px' }} />
            </div>
          </div>
          <div style={styles.divider} />
          <span style={styles.pageTitle}>Anomaly Detection Dashboard</span>
          <InfoTooltip text="This dashboard displays outputs from a multi-engine anomaly detection pipeline. Data is read from 1-minute resampled sensor timeseries covering 49 channels (8 vibration, 11 temperature, 30 electrical). Anomalies are detected via Engine A (drift/baseline), Engine B (FFT periodicity), physics validation (identity checks), and PCA multivariate analysis. Results are fused into a unified risk score per timestamp." />
        </div>
        <div style={styles.right}>
          <div style={styles.clockWrap}>
            <AnalogClock size={36} time={utcTime} />
            <div style={styles.timeContainer}>
              <span style={styles.timeText}>{utcTimeString}</span>
              <span style={styles.utcLabel}>UTC</span>
            </div>
          </div>
          <div style={styles.userWrap}>
            <div style={styles.avatar}>{user ? user.charAt(0).toUpperCase() : 'U'}</div>
            <span style={styles.userName}>{user}</span>
          </div>
          <motion.button
            style={styles.logoutBtn}
            onClick={onLogout}
            whileHover={{
              borderColor: '#EF5350',
              color: '#C62828',
              background: 'rgba(239,83,80,0.05)',
            }}
            whileTap={{ scale: 0.97 }}
          >
            Sign Out
          </motion.button>
        </div>
      </motion.div>
    </>
  );
}

export default TopBar;