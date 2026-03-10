import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { login } from '../utils/api';

const LOGO_SRC = '/stoma.png';
const FS_LOGO_SRC = '/FS.png';

const styles = {
  container: {
    minHeight: '100vh',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    padding: '20px',
    position: 'relative',
    overflow: 'hidden',
    background: '#FFFFFF',
  },
  card: {
    background: 'rgba(255, 255, 255, 0.55)',
    backdropFilter: 'blur(32px)',
    WebkitBackdropFilter: 'blur(32px)',
    borderRadius: '24px',
    border: '1px solid rgba(255, 255, 255, 0.5)',
    padding: '24px 24px',
    width: '340px',
    boxShadow: '0 24px 80px rgba(27,94,32,0.12), 0 8px 24px rgba(27,94,32,0.06)',
    position: 'relative',
    zIndex: 1,
  },
  logoWrap: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    marginBottom: '16px',
  },
  logo: {
    width: '44px',
    height: '44px',
    marginBottom: '8px',
  },
  title: {
    fontSize: '17px',
    fontWeight: 600,
    color: '#1B5E20',
    letterSpacing: '-0.02em',
  },
  subtitle: {
    fontSize: '12px',
    color: '#6B736B',
    marginTop: '2px',
    fontWeight: 400,
  },
  form: {
    display: 'flex',
    flexDirection: 'column',
    gap: '10px',
  },
  inputGroup: {
    display: 'flex',
    flexDirection: 'column',
    gap: '6px',
  },
  label: {
    fontSize: '12px',
    fontWeight: 500,
    color: '#4A524A',
    letterSpacing: '0.01em',
  },
  input: {
    width: '100%',
    padding: '9px 12px',
    border: '1.5px solid rgba(255,255,255,0.6)',
    borderRadius: '10px',
    fontSize: '13px',
    color: '#1A1F1A',
    background: 'rgba(255,255,255,0.5)',
    backdropFilter: 'blur(12px)',
    WebkitBackdropFilter: 'blur(12px)',
    outline: 'none',
    transition: 'all 0.25s ease',
  },
  button: {
    width: '100%',
    padding: '9px',
    background: 'linear-gradient(135deg, #1B5E20 0%, #388E3C 100%)',
    color: '#FFFFFF',
    border: 'none',
    borderRadius: '10px',
    fontSize: '13px',
    fontWeight: 600,
    cursor: 'pointer',
    transition: 'all 0.2s ease',
    marginTop: '4px',
    letterSpacing: '0.01em',
  },
  error: {
    padding: '10px 14px',
    background: 'rgba(255, 205, 210, 0.75)',
    backdropFilter: 'blur(8px)',
    color: '#C62828',
    borderRadius: '10px',
    fontSize: '13px',
    fontWeight: 500,
    textAlign: 'center',
    border: '1px solid rgba(239,154,154,0.4)',
  },
};

function LoginPage({ onLogin }) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      const res = await login(username, password);
      if (res.data.success) {
        onLogin(res.data.username);
      }
    } catch (err) {
      setError('Invalid credentials. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={styles.container}>
      {/* Vivid aurora background for login */}
      <div style={{
        position: 'absolute',
        top: 0, left: 0, right: 0, bottom: 0,
        overflow: 'hidden',
        pointerEvents: 'none',
        zIndex: 0,
      }}>
        {/* Large vivid blobs */}
        <div style={{
          position: 'absolute',
          width: '70vw', height: '70vw',
          maxWidth: '900px', maxHeight: '900px',
          top: '-25%', left: '-15%',
          background: 'radial-gradient(ellipse at 45% 45%, #81C784 0%, #CBE6C8 30%, #E6F4EA 55%, transparent 75%)',
          borderRadius: '50%',
          filter: 'blur(60px)',
          opacity: 0.7,
          animation: 'loginMesh1 35s ease-in-out infinite, loginBreathe1 8s ease-in-out infinite',
        }} />
        <div style={{
          position: 'absolute',
          width: '65vw', height: '65vw',
          maxWidth: '850px', maxHeight: '850px',
          top: '-10%', right: '-20%',
          background: 'radial-gradient(ellipse at 55% 40%, #4CAF50 0%, #A8DCA8 25%, #E6F4EA 50%, transparent 75%)',
          borderRadius: '50%',
          filter: 'blur(70px)',
          opacity: 0.6,
          animation: 'loginMesh2 40s ease-in-out infinite, loginBreathe2 10s ease-in-out 2s infinite',
        }} />
        <div style={{
          position: 'absolute',
          width: '60vw', height: '60vw',
          maxWidth: '800px', maxHeight: '800px',
          bottom: '-20%', left: '10%',
          background: 'radial-gradient(ellipse at 50% 55%, #388E3C 0%, #81C784 20%, #CBE6C8 45%, transparent 70%)',
          borderRadius: '50%',
          filter: 'blur(65px)',
          opacity: 0.65,
          animation: 'loginMesh3 45s ease-in-out infinite, loginBreathe3 9s ease-in-out 4s infinite',
        }} />
        <div style={{
          position: 'absolute',
          width: '55vw', height: '55vw',
          maxWidth: '750px', maxHeight: '750px',
          bottom: '-15%', right: '-10%',
          background: 'radial-gradient(ellipse at 45% 50%, #A8DCA8 0%, #E6F4EA 35%, transparent 70%)',
          borderRadius: '50%',
          filter: 'blur(55px)',
          opacity: 0.6,
          animation: 'loginMesh4 38s ease-in-out infinite, loginBreathe1 11s ease-in-out 6s infinite',
        }} />
        <div style={{
          position: 'absolute',
          width: '45vw', height: '45vw',
          maxWidth: '600px', maxHeight: '600px',
          top: '30%', left: '25%',
          background: 'radial-gradient(ellipse at 50% 50%, #1B5E20 0%, #4CAF50 15%, #81C784 35%, #CBE6C8 55%, transparent 75%)',
          borderRadius: '50%',
          filter: 'blur(80px)',
          opacity: 0.45,
          animation: 'loginMesh5 50s ease-in-out infinite, loginBreathe2 12s ease-in-out 3s infinite',
        }} />

        {/* Smaller accent wisps */}
        <div style={{
          position: 'absolute',
          width: '30vw', height: '30vw',
          maxWidth: '400px', maxHeight: '400px',
          top: '15%', right: '15%',
          background: 'radial-gradient(ellipse at 50% 50%, #CBE6C8 0%, #E6F4EA 40%, transparent 70%)',
          borderRadius: '50%',
          filter: 'blur(50px)',
          opacity: 0.55,
          animation: 'loginMesh3 32s ease-in-out infinite, loginBreathe3 7s ease-in-out 1s infinite',
        }} />
        <div style={{
          position: 'absolute',
          width: '25vw', height: '25vw',
          maxWidth: '350px', maxHeight: '350px',
          bottom: '20%', left: '5%',
          background: 'radial-gradient(ellipse at 50% 50%, rgba(76,175,80,0.5) 0%, rgba(168,220,168,0.3) 40%, transparent 70%)',
          borderRadius: '50%',
          filter: 'blur(45px)',
          opacity: 0.5,
          animation: 'loginMesh1 28s ease-in-out infinite, loginBreathe2 8s ease-in-out 5s infinite',
        }} />
      </div>

      {/* Inline keyframes for login-specific animations */}
      <style>{`
        @keyframes loginMesh1 {
          0% { transform: translate(0%, 0%) rotate(0deg) scale(1); }
          20% { transform: translate(10%, -8%) rotate(20deg) scale(1.08); }
          40% { transform: translate(-6%, 12%) rotate(-15deg) scale(0.95); }
          60% { transform: translate(8%, 5%) rotate(25deg) scale(1.05); }
          80% { transform: translate(-10%, -4%) rotate(-8deg) scale(0.97); }
          100% { transform: translate(0%, 0%) rotate(0deg) scale(1); }
        }
        @keyframes loginMesh2 {
          0% { transform: translate(0%, 0%) rotate(0deg) scale(1); }
          25% { transform: translate(-12%, 10%) rotate(-25deg) scale(1.1); }
          50% { transform: translate(7%, -7%) rotate(15deg) scale(0.93); }
          75% { transform: translate(9%, 8%) rotate(-18deg) scale(1.06); }
          100% { transform: translate(0%, 0%) rotate(0deg) scale(1); }
        }
        @keyframes loginMesh3 {
          0% { transform: translate(0%, 0%) rotate(0deg) scale(1); }
          30% { transform: translate(8%, 12%) rotate(18deg) scale(1.06); }
          60% { transform: translate(-10%, -6%) rotate(-22deg) scale(0.94); }
          100% { transform: translate(0%, 0%) rotate(0deg) scale(1); }
        }
        @keyframes loginMesh4 {
          0% { transform: translate(0%, 0%) rotate(0deg) scale(1); }
          20% { transform: translate(-7%, -10%) rotate(12deg) scale(1.07); }
          45% { transform: translate(11%, 4%) rotate(-16deg) scale(0.95); }
          70% { transform: translate(-4%, 9%) rotate(20deg) scale(1.04); }
          100% { transform: translate(0%, 0%) rotate(0deg) scale(1); }
        }
        @keyframes loginMesh5 {
          0% { transform: translate(0%, 0%) rotate(0deg) scale(1); }
          35% { transform: translate(9%, -9%) rotate(-20deg) scale(1.08); }
          65% { transform: translate(-8%, 7%) rotate(14deg) scale(0.96); }
          100% { transform: translate(0%, 0%) rotate(0deg) scale(1); }
        }
        @keyframes loginBreathe1 {
          0%, 100% { opacity: 0.55; }
          50% { opacity: 0.8; }
        }
        @keyframes loginBreathe2 {
          0%, 100% { opacity: 0.45; }
          50% { opacity: 0.72; }
        }
        @keyframes loginBreathe3 {
          0%, 100% { opacity: 0.5; }
          50% { opacity: 0.75; }
        }
      `}</style>

      <motion.div
        style={styles.card}
        initial={{ opacity: 0, y: 30, scale: 0.96 }}
        animate={{ opacity: 1, y: 0, scale: 1 }}
        transition={{ duration: 0.6, ease: [0.4, 0, 0.2, 1] }}
      >
        <div style={styles.logoWrap}>
          <motion.img
            src={LOGO_SRC}
            alt="Stoma Insights"
            style={styles.logo}
            animate={{
              scale: [1, 1.05, 1],
              rotate: [0, 2, -2, 0],
            }}
            transition={{
              duration: 4,
              ease: 'easeInOut',
              repeat: Infinity,
              repeatType: 'loop',
            }}
          />
          <div style={styles.title}>Stoma Insights</div>
          <div style={styles.subtitle}>Anomaly Detection Platform</div>
        </div>
        <form style={styles.form} onSubmit={handleSubmit}>
          <div style={styles.inputGroup}>
            <label style={styles.label}>Username</label>
            <input
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              style={styles.input}
              placeholder="Enter username"
              onFocus={(e) => {
                e.target.style.borderColor = 'rgba(76,175,80,0.5)';
                e.target.style.boxShadow = '0 0 0 3px rgba(76,175,80,0.08)';
                e.target.style.background = 'rgba(255,255,255,0.7)';
              }}
              onBlur={(e) => {
                e.target.style.borderColor = 'rgba(255,255,255,0.6)';
                e.target.style.boxShadow = 'none';
                e.target.style.background = 'rgba(255,255,255,0.5)';
              }}
            />
          </div>
          <div style={styles.inputGroup}>
            <label style={styles.label}>Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              style={styles.input}
              placeholder="Enter password"
              onFocus={(e) => {
                e.target.style.borderColor = 'rgba(76,175,80,0.5)';
                e.target.style.boxShadow = '0 0 0 3px rgba(76,175,80,0.08)';
                e.target.style.background = 'rgba(255,255,255,0.7)';
              }}
              onBlur={(e) => {
                e.target.style.borderColor = 'rgba(255,255,255,0.6)';
                e.target.style.boxShadow = 'none';
                e.target.style.background = 'rgba(255,255,255,0.5)';
              }}
            />
          </div>
          {error && (
            <motion.div
              style={styles.error}
              initial={{ opacity: 0, y: -5 }}
              animate={{ opacity: 1, y: 0 }}
            >
              {error}
            </motion.div>
          )}
          <motion.button
            type="submit"
            style={{
              ...styles.button,
              opacity: loading ? 0.7 : 1,
            }}
            disabled={loading}
            whileHover={{ scale: 1.01, boxShadow: '0 8px 28px rgba(27,94,32,0.3)' }}
            whileTap={{ scale: 0.98 }}
          >
            {loading ? 'Authenticating...' : 'Sign In'}
          </motion.button>
        </form>
        <div style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          gap: '6px',
          marginTop: '14px',
          paddingTop: '10px',
          borderTop: '1px solid rgba(203, 230, 200, 0.3)',
        }}>
          <span style={{ fontSize: '11px', color: '#9E9E9E', fontWeight: 400 }}>Powered by</span>
          <img src={FS_LOGO_SRC} alt="Findability Sciences" style={{ height: '18px' }} />
        </div>
      </motion.div>
    </div>
  );
}

export default LoginPage;