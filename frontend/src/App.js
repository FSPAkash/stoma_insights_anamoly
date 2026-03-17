import React, { useState, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import LoginPage from './components/LoginPage';
import Dashboard from './components/Dashboard';
import DashboardBeta from './components/DashboardBeta';

function App() {
  const [user, setUser] = useState(null);
  const [mode, setMode] = useState('stable'); // 'stable' or 'beta'

  const handleLogin = useCallback((username, loginMode) => {
    setUser(username);
    setMode(loginMode || 'stable');
  }, []);

  const handleLogout = useCallback(() => {
    setUser(null);
    setMode('stable');
  }, []);

  const DashboardComponent = mode === 'beta' ? DashboardBeta : Dashboard;

  return (
    <AnimatePresence mode="wait">
      {!user ? (
        <motion.div
          key="login"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.4 }}
        >
          <LoginPage onLogin={handleLogin} />
        </motion.div>
      ) : (
        <motion.div
          key={`dashboard-${mode}`}
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -10 }}
          transition={{ duration: 0.5 }}
        >
          <DashboardComponent user={user} onLogout={handleLogout} isBeta={mode === 'beta'} />
        </motion.div>
      )}
    </AnimatePresence>
  );
}

export default App;