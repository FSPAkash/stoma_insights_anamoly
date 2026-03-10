import React, { useState, useEffect, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import TopBar from './TopBar';
import SummaryCards from './SummaryCards';
import SubsystemGauges from './SubsystemGauges';
import NormalBehaviorPanel from './NormalBehaviorPanel';
import RiskTimeline from './RiskTimeline';
import ScoresOverview from './ScoresOverview';
import AlertEpisodeCards from './AlertEpisodeCards';
import AlertDetailModal from './AlertDetailModal';
import {
  getDashboardSummary,
  getAlerts,
  getScoresTimeseries,
  getScores,
  getNormalPeriods,
} from '../utils/api';
import InfoTooltip from './InfoTooltip';
import FeedbackWidget from './FeedbackWidget';
import PipelineAnimation from './PipelineAnimation';

const AuroraBg = () => (
  <div className="aurora-bg">
    <div className="aurora-mesh mesh-1" />
    <div className="aurora-mesh mesh-2" />
    <div className="aurora-mesh mesh-3" />
    <div className="aurora-mesh mesh-4" />
    <div className="aurora-mesh mesh-5" />
    <div className="aurora-mesh mesh-6" />
    <div className="aurora-mesh mesh-7" />
    <div className="aurora-mesh mesh-8" />
    <div className="aurora-mesh mesh-9" />
    <div className="aurora-mesh mesh-10" />
  </div>
);

const styles = {
  page: {
    minHeight: '100vh',
    position: 'relative',
  },
  contentWrap: {
    position: 'relative',
    zIndex: 1,
  },
  content: {
    maxWidth: '1400px',
    margin: '0 auto',
    padding: '24px 32px 60px',
  },
  sectionHeader: {
    fontSize: '11px',
    fontWeight: 600,
    color: '#8A928A',
    textTransform: 'uppercase',
    letterSpacing: '0.08em',
    marginTop: '36px',
    marginBottom: '4px',
    display: 'flex',
    alignItems: 'center',
  },
  refreshBar: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '20px',
  },
  refreshBtn: {
    padding: '8px 18px',
    background: 'rgba(255,255,255,0.7)',
    backdropFilter: 'blur(16px)',
    WebkitBackdropFilter: 'blur(16px)',
    border: '1px solid rgba(203,230,200,0.5)',
    borderRadius: '10px',
    fontSize: '13px',
    fontWeight: 500,
    color: '#388E3C',
    cursor: 'pointer',
    transition: 'all 0.2s',
  },
  lastUpdated: {
    fontSize: '12px',
    color: '#8A928A',
  },
  loadingScreen: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    height: '60vh',
    gap: '16px',
  },
  spinner: {
    width: '40px',
    height: '40px',
    border: '3px solid rgba(203,230,200,0.5)',
    borderTop: '3px solid #1B5E20',
    borderRadius: '50%',
    animation: 'spin 1s linear infinite',
  },
};

function Dashboard({ user, onLogout }) {
  const [summary, setSummary] = useState(null);
  const [alerts, setAlerts] = useState([]);
  const [timeseries, setTimeseries] = useState([]);
  const [timestampCol, setTimestampCol] = useState('timestamp_utc');
  const [stats, setStats] = useState(null);
  const [normalData, setNormalData] = useState(null);
  const [selectedAlert, setSelectedAlert] = useState(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [pipelineStatus, setPipelineStatus] = useState(null); // null | 'running' | 'finished'

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [summaryRes, alertsRes, tsRes, scoresRes, normalRes] = await Promise.allSettled([
        getDashboardSummary(),
        getAlerts(),
        getScoresTimeseries(5),
        getScores({ limit: 100 }),
        getNormalPeriods(),
      ]);

      if (summaryRes.status === 'fulfilled') setSummary(summaryRes.value.data);
      else console.error('Failed to load summary:', summaryRes.reason);

      if (alertsRes.status === 'fulfilled') setAlerts(alertsRes.value.data.alerts || []);
      else console.error('Failed to load alerts:', alertsRes.reason);

      if (tsRes.status === 'fulfilled') {
        const tsData = tsRes.value.data;
        setTimeseries(tsData.timeseries || []);
        setTimestampCol(tsData.timestamp_col || 'timestamp_utc');
      } else {
        console.error('Failed to load timeseries:', tsRes.reason);
      }

      if (scoresRes.status === 'fulfilled') setStats(scoresRes.value.data.stats);
      else console.error('Failed to load scores:', scoresRes.reason);

      if (normalRes.status === 'fulfilled') setNormalData(normalRes.value.data);
      else console.error('Failed to load normal periods:', normalRes.reason);

      setLastUpdated(new Date());
    } catch (err) {
      console.error('Failed to load dashboard data:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  // DEMO: Fake a 5-second data update instead of running the real pipeline
  const handleUpdateData = useCallback(() => {
    setPipelineStatus('running');
    setTimeout(() => {
      setPipelineStatus('finished');
      setLastUpdated(new Date());
      // Auto-dismiss after showing success briefly
      setTimeout(() => setPipelineStatus(null), 2000);
    }, 5000);
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);

  return (
    <div style={styles.page}>
      <AuroraBg />

      <div style={styles.contentWrap}>
        <TopBar user={user} onLogout={onLogout} />

        <div style={styles.content}>
          <div style={styles.refreshBar}>
            <div>

              {summary?.data_range?.start && (
                <div style={{ fontSize: '13px', color: '#6B736B' }}>
                  Data range: {summary.data_range.start} to {summary.data_range.end}
                </div>
              )}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
              {lastUpdated && (
                <span style={styles.lastUpdated}>Updated {lastUpdated.toLocaleTimeString()}</span>
              )}
              <motion.button
                style={styles.refreshBtn}
                onClick={handleUpdateData}
                whileHover={{ background: 'rgba(230,244,234,0.9)', borderColor: '#81C784' }}
                whileTap={{ scale: 0.97 }}
              >
                Update Data
              </motion.button>
            </div>
          </div>

          {loading ? (
            <div style={styles.loadingScreen}>
              <div style={styles.spinner} />
              <div style={{ color: '#6B736B', fontSize: '14px' }}>Loading dashboard...</div>
            </div>
          ) : (
            <>
              <SummaryCards summary={summary} stats={stats} />
              <SubsystemGauges stats={stats} />
              <NormalBehaviorPanel normalData={normalData} />

              <RiskTimeline timeseries={timeseries} timestampCol={timestampCol} />
              <ScoresOverview stats={stats} />
              <AlertEpisodeCards alerts={alerts} onSelectAlert={setSelectedAlert} />
            </>
          )}
        </div>
      </div>

      {selectedAlert && (
        <AlertDetailModal alert={selectedAlert} onClose={() => setSelectedAlert(null)} />
      )}

      <FeedbackWidget user={user} />

      <AnimatePresence>
        {pipelineStatus && (
          <PipelineAnimation status={pipelineStatus} />
        )}
      </AnimatePresence>
    </div>
  );
}

export default Dashboard;