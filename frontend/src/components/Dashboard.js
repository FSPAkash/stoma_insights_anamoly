import React, { useState, useEffect, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import TopBar from './TopBar';
import SummaryCards from './SummaryCards';
import SubsystemGauges from './SubsystemGauges';
import NormalBehaviorPanel from './NormalBehaviorPanel';
import SubsystemBehaviorChart from './SubsystemBehaviorChart';
import AlertEpisodeCards from './AlertEpisodeCards';
import AlertDetailModal from './AlertDetailModal';
import {
  getDashboardSummary,
  getAlerts,
  getScoresTimeseries,
  getScores,
  getNormalPeriods,
  getSystems,
} from '../utils/api';
import FeedbackWidget from './FeedbackWidget';
import PipelineAnimation from './PipelineAnimation';
import TimeFilter from './TimeFilter';
import useTimeFilter from '../hooks/useTimeFilter';

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
  refreshBar: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '20px',
  },
  refreshBtn: {
    padding: '8px 18px',
    backgroundColor: 'rgba(255,255,255,0.7)',
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
  // Section divider styles
  sectionDivider: {
    width: '100%',
    height: '2px',
    background: 'linear-gradient(90deg, transparent 0%, rgba(27,94,32,0.18) 20%, rgba(27,94,32,0.18) 80%, transparent 100%)',
    margin: '24px 0 4px 0',
  },
  sectionWrap: {
    position: 'relative',
    borderLeft: '3px solid rgba(27,94,32,0.13)',
    paddingLeft: '20px',
    marginLeft: '4px',
    marginTop: '0',
  },
  sectionLabel: {
    fontSize: '10.5px',
    fontWeight: 600,
    color: '#8A928A',
    textTransform: 'uppercase',
    letterSpacing: '0.09em',
    marginBottom: '16px',
    marginTop: '8px',
  },
  floatingWrap: {
    position: 'fixed',
    bottom: '24px',
    left: '24px',
    zIndex: 1000,
  },
  widgetCollapsed: {
    backgroundColor: 'rgba(255, 255, 255, 0.25)',
    backdropFilter: 'blur(24px)',
    WebkitBackdropFilter: 'blur(24px)',
    borderRadius: '14px',
    boxShadow: '0 8px 36px rgba(27, 94, 28, 0.65), 0 2px 10px rgba(0,0,0,0.06)',
    overflow: 'hidden',
    cursor: 'pointer',
    transition: 'background-color 0.15s',
  },
  widgetCollapsedActive: {
    border: '1.5px solid rgba(27,94,32,0.35)',
  },
  widgetCollapsedInactive: {
    border: '1.5px solid rgba(203,230,200,0.5)',
  },
  infoCardRow: {
    padding: '12px 16px',
    display: 'flex',
    alignItems: 'center',
    gap: '10px',
  },
  infoCardIcon: {
    width: '32px',
    height: '32px',
    borderRadius: '10px',
    backgroundColor: 'rgba(27,94,32,0.08)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    flexShrink: 0,
  },
  infoCardIconSvg: {
    width: '16px',
    height: '16px',
  },
  infoCardText: {
    display: 'flex',
    flexDirection: 'column',
    gap: '1px',
    overflow: 'hidden',
    flex: 1,
  },
  infoCardLabel: {
    fontSize: '9.5px',
    fontWeight: 700,
    color: '#8A928A',
    textTransform: 'uppercase',
    letterSpacing: '0.08em',
    lineHeight: 1.2,
  },
  infoCardValue: {
    fontSize: '12px',
    fontWeight: 600,
    color: '#1B5E20',
    lineHeight: 1.3,
    whiteSpace: 'nowrap',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
  },
  infoCardValueDefault: {
    fontSize: '12px',
    fontWeight: 500,
    color: '#6B736B',
    lineHeight: 1.3,
  },
  activeDot: {
    width: '8px',
    height: '8px',
    borderRadius: '50%',
    backgroundColor: '#1B5E20',
    boxShadow: '0 0 6px rgba(27,94,32,0.4)',
    flexShrink: 0,
  },
  expandedPanel: {
    position: 'fixed',
    bottom: '24px',
    left: '24px',
    zIndex: 1001,
    width: '380px',
    maxHeight: 'calc(100vh - 48px)',
    overflowY: 'auto',
    backgroundColor: 'rgba(255, 255, 255, 0.25)',
    backdropFilter: 'blur(28px)',
    WebkitBackdropFilter: 'blur(28px)',
    border: '1.5px solid rgba(27,94,32,0.3)',
    borderRadius: '18px',
    boxShadow: '0 20px 60px rgba(27,94,32,0.16), 0 4px 16px rgba(0,0,0,0.1)',
    overflow: 'hidden',
  },
  expandedHeader: {
    padding: '16px 18px 12px',
    display: 'flex',
    alignItems: 'center',
    gap: '10px',
    borderBottom: '1px solid rgba(27,94,32,0.1)',
  },
  expandedHeaderText: {
    flex: 1,
  },
  expandedTitle: {
    fontSize: '13px',
    fontWeight: 700,
    color: '#1B5E20',
    letterSpacing: '0.02em',
  },
  expandedSubtitle: {
    fontSize: '11px',
    fontWeight: 500,
    color: '#6B736B',
    marginTop: '1px',
  },
  closeBtn: {
    width: '30px',
    height: '30px',
    borderRadius: '9px',
    backgroundColor: 'rgba(27,94,32,0.08)',
    border: 'none',
    cursor: 'pointer',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    fontSize: '14px',
    color: '#6B736B',
    fontWeight: 600,
    transition: 'background-color 0.15s',
    flexShrink: 0,
  },
  expandedBody: {
    padding: '16px 18px 18px',
  },
  filterSectionTitle: {
    fontSize: '10px',
    fontWeight: 700,
    color: '#1B5E20',
    textTransform: 'uppercase',
    letterSpacing: '0.1em',
    marginBottom: '12px',
  },
  filterHint: {
    fontSize: '10.5px',
    color: '#6B736B',
    marginTop: '12px',
    lineHeight: '1.5',
    borderTop: '1px solid rgba(27,94,32,0.08)',
    paddingTop: '10px',
  },
};

function FilterClockIcon() {
  return (
    <svg
      style={styles.infoCardIconSvg}
      viewBox="0 0 24 24"
      fill="none"
      stroke="#1B5E20"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <circle cx="12" cy="12" r="10" />
      <polyline points="12 6 12 12 16 14" />
    </svg>
  );
}

function Dashboard({ user, onLogout }) {
  const [summary, setSummary] = useState(null);
  const [alerts, setAlerts] = useState([]);
  const [timeseries, setTimeseries] = useState([]);
  const [timestampCol, setTimestampCol] = useState('timestamp_utc');
  const [stats, setStats] = useState(null);
  const [normalData, setNormalData] = useState(null);
  const [systems, setSystems] = useState([]);
  const [selectedAlert, setSelectedAlert] = useState(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [pipelineStatus, setPipelineStatus] = useState(null);
  const [filterOpen, setFilterOpen] = useState(false);
  const [widgetHovered, setWidgetHovered] = useState(false);

  const timeFilter = useTimeFilter(timeseries, timestampCol);

  const hasActiveFilter = !!(timeFilter.filterLabel && timeFilter.filterLabel !== 'all data');

  const resolvedSystems = systems.length > 0 ? systems : summary?.systems;

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [summaryRes, alertsRes, tsRes, scoresRes, normalRes, systemsRes] = await Promise.allSettled([
        getDashboardSummary(),
        getAlerts(),
        getScoresTimeseries(5),
        getScores({ limit: 100 }),
        getNormalPeriods(),
        getSystems(),
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

      if (systemsRes.status === 'fulfilled') setSystems(systemsRes.value.data.systems || []);
      else console.error('Failed to load systems:', systemsRes.reason);

      setLastUpdated(new Date());
    } catch (err) {
      console.error('Failed to load dashboard data:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  const handleUpdateData = useCallback(() => {
    setPipelineStatus('running');
    setTimeout(() => {
      setPipelineStatus('finished');
      setLastUpdated(new Date());
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
                whileHover={{ backgroundColor: 'rgba(230,244,234,0.9)', borderColor: '#81C784' }}
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
              {/* Dashboard Overview Section */}
              <div style={styles.sectionDivider} />

              <div style={styles.sectionWrap}>
                <div style={styles.sectionLabel}>Dashboard Overview</div>
                <SummaryCards summary={summary} stats={stats} systems={resolvedSystems} />
              </div>

              {/* Filtered Results Section */}
              <div style={styles.sectionDivider} />

              <div style={styles.sectionWrap}>
                <div style={styles.sectionLabel}>
                  Filtered Results: {timeFilter.filterLabel || 'All Data'}
                </div>

                <SubsystemGauges 
                  stats={timeFilter.filteredStats} 
                  filterLabel={timeFilter.filterLabel} 
                  systems={systems} 
                  onFilterClick={() => setFilterOpen(true)} 
                />
                <NormalBehaviorPanel 
                  normalData={timeFilter.filteredNormalData} 
                  filterLabel={timeFilter.filterLabel} 
                  onFilterClick={() => setFilterOpen(true)} 
                />
                <SubsystemBehaviorChart
                  systems={resolvedSystems}
                  filterLabel={timeFilter.filterLabel}
                  selectedDay={timeFilter.selectedDay}
                  isLatestMode={timeFilter.isLatestMode}
                  lastNHours={timeFilter.lastNHours}
                  startTime={timeFilter.startTime}
                  endTime={timeFilter.endTime}
                  onFilterClick={() => setFilterOpen(true)}
                  alerts={alerts}
                  onSelectAlert={setSelectedAlert}
                />
                <AlertEpisodeCards 
                  alerts={alerts} 
                  onSelectAlert={setSelectedAlert} 
                  selectedDay={timeFilter.selectedDay} 
                  filterLabel={timeFilter.filterLabel} 
                />
              </div>
            </>
          )}
        </div>
      </div>

      {selectedAlert && (
        <AlertDetailModal alert={selectedAlert} onClose={() => setSelectedAlert(null)} />
      )}

      <FeedbackWidget user={user} />

      {!loading && (
        <div style={styles.floatingWrap}>
          <AnimatePresence mode="wait">
            {!filterOpen ? (
              <motion.div
                key="collapsed"
                style={{
                  ...styles.widgetCollapsed,
                  ...(hasActiveFilter ? styles.widgetCollapsedActive : styles.widgetCollapsedInactive),
                  backgroundColor: widgetHovered ? 'rgba(126, 162, 239, 0.18)' : 'rgba(126, 162, 239, 0.1)',
                }}
                onClick={() => setFilterOpen(true)}
                onMouseEnter={() => setWidgetHovered(true)}
                onMouseLeave={() => setWidgetHovered(false)}
                initial={{ opacity: 0, scale: 0.9 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.95 }}
                transition={{ duration: 0.2 }}
              >
                <div style={styles.infoCardRow}>
                  <div style={styles.infoCardIcon}>
                    <FilterClockIcon />
                  </div>
                  <div style={styles.infoCardText}>
                    <div style={styles.infoCardLabel}>Time Filter</div>
                    {hasActiveFilter ? (
                      <div style={styles.infoCardValue}>{timeFilter.filterLabel}</div>
                    ) : (
                      <div style={styles.infoCardValueDefault}>All data</div>
                    )}
                  </div>
                  {hasActiveFilter && <div style={styles.activeDot} />}
                </div>
              </motion.div>
            ) : (
              <motion.div
                key="expanded"
                style={styles.expandedPanel}
                initial={{ opacity: 0, y: 20, scale: 0.95 }}
                animate={{ opacity: 1, y: 0, scale: 1 }}
                exit={{ opacity: 0, y: 20, scale: 0.95 }}
                transition={{ duration: 0.3, ease: [0.23, 1, 0.32, 1] }}
              >
                <div style={styles.expandedHeader}>
                  <div style={styles.infoCardIcon}>
                    <FilterClockIcon />
                  </div>
                  <div style={styles.expandedHeaderText}>
                    <div style={styles.expandedTitle}>Time Filter</div>
                    <div style={styles.expandedSubtitle}>
                      {hasActiveFilter ? timeFilter.filterLabel : 'All data -- no filter applied'}
                    </div>
                  </div>
                  <button
                    style={styles.closeBtn}
                    onClick={() => setFilterOpen(false)}
                    onMouseEnter={(e) => { e.currentTarget.style.backgroundColor = 'rgba(27,94,32,0.15)'; }}
                    onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = 'rgba(27,94,32,0.08)'; }}
                  >
                    X
                  </button>
                </div>

                <div style={styles.expandedBody}>
                  <div style={styles.filterSectionTitle}>Select Day and Time Range</div>

                  <TimeFilter
                    availableDays={timeFilter.availableDays}
                    selectedDay={timeFilter.selectedDay}
                    isLatestMode={timeFilter.isLatestMode}
                    lastNHours={timeFilter.lastNHours}
                    startTime={timeFilter.startTime}
                    endTime={timeFilter.endTime}
                    onSelectDay={timeFilter.handleDayChange}
                    onLatestClick={timeFilter.handleLatestClick}
                    onLastNHoursChange={timeFilter.setLastNHours}
                    onStartTimeChange={timeFilter.setStartTime}
                    onEndTimeChange={timeFilter.setEndTime}
                    onReset={timeFilter.handleReset}
                  />

                  <div style={styles.filterHint}>
                    All filtered panels update with the time window selected here.
                    {hasActiveFilter && (
                      <span style={{ display: 'block', marginTop: '4px', fontWeight: 600, color: '#1B5E20' }}>
                        Active: {timeFilter.filterLabel}
                      </span>
                    )}
                  </div>
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      )}

      <AnimatePresence>
        {pipelineStatus && (
          <PipelineAnimation status={pipelineStatus} />
        )}
      </AnimatePresence>
    </div>
  );
}

export default Dashboard;
