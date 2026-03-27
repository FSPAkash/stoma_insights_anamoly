import React, { useState, useEffect, useCallback, useRef } from 'react';
import TopBarBeta from './TopBarBeta';
import BetaOverviewCards from './BetaOverviewCards';
import BetaAlertDetailModal from './BetaAlertDetailModal';
import SensorQualityGrid from './SensorQualityGrid';
import SubsystemBehaviorChartBeta from './SubsystemBehaviorChartBeta';
import SensorValidationReport from './SensorValidationReport';
import FeedbackWidget from './FeedbackWidget';
import TimeFilter from './TimeFilter';
import useTimeFilter from '../hooks/useTimeFilter';
import {
  getBetaScoresTimeseries,
  getBetaSubsystems,
  getBetaRadarFingerprints,
} from '../utils/api';

const BETA_FEEDBACK_SECTIONS = [
  { id: 'sensor_validation', label: 'Sensor Validation', desc: 'Initial per-sensor quality checks, pass/fail results, and removal reasons' },
  { id: 'pipeline_overview', label: 'Pipeline Overview', desc: 'Overview cards, high-level health, and summary stats' },
  { id: 'sensor_quality_plots', label: 'Sensor Quality Plots', desc: 'Subsystem score, sensor breakdown, anomaly overlays, isolated sensor handling, and chart interactions' },
  { id: 'radar_fingerprints', label: 'Radar Fingerprints', desc: 'Radar chart overlays showing sensor fault signatures during anomaly events' },
  { id: 'subsystem_behavior', label: 'Subsystem Behavior', desc: 'Raw sensor traces, downtime bands, anomaly overlays, and click-through behavior' },
  { id: 'system_alarms', label: 'System Anomalies', desc: 'Anomaly cards, minute vs span view, filters, sorting, expand/collapse, and anomaly info details' },
  { id: 'system_alarm_detail', label: 'System Anomaly Detail', desc: 'Anomaly detail modal, sensor rankings, severity mix, radar fingerprints, and decomposition' },
  { id: 'isolated_sensors', label: 'Isolated Sensors', desc: 'Isolated sensor selection, exclusion from subsystem scores, and standalone quality views' },
  { id: 'loading_overlays', label: 'Loading & Transitions', desc: 'Loading overlays, data refresh indicators, and panel transition behavior' },
  { id: 'time_filter', label: 'Time Filter', desc: 'Floating day and time filter controls across the dashboard' },
  { id: 'overall_beta_dashboard', label: 'Overall Beta Dashboard', desc: 'General usability, clarity, responsiveness, and confidence in the experience' },
];

const MAIN_SYSTEM_OPTIONS = ['Shredder'];

function LazySection({ children, height = '400px' }) {
  const ref = useRef(null);
  const [visible, setVisible] = useState(false);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      ([entry]) => { if (entry.isIntersecting) { setVisible(true); observer.disconnect(); } },
      { rootMargin: '200px' }
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, []);
  return (
    <div ref={ref}>
      {visible ? children : (
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height, color: '#8A928A', fontSize: '13px' }}>
          Scroll to load...
        </div>
      )}
    </div>
  );
}

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
  page: { minHeight: '100vh', position: 'relative' },
  contentWrap: { position: 'relative', zIndex: 1 },
  content: { maxWidth: '1400px', margin: '0 auto', padding: '24px 32px 60px' },
  refreshBar: { display: 'flex', justifyContent: 'flex-start', alignItems: 'stretch', gap: '12px', marginBottom: '20px', paddingLeft: '20px', marginLeft: '4px' },
  selectorWrap: { minWidth: '260px', maxWidth: '300px' },
  selectorCard: {
    position: 'relative',
    width: '100%',
    backgroundColor: 'rgba(243,248,243,0.78)',
    backdropFilter: 'blur(12px)', WebkitBackdropFilter: 'blur(12px)',
    borderRadius: '12px',
    border: '1px solid rgba(176,205,174,0.55)',
    overflow: 'hidden',
  },
  selectorChevron: {
    fontSize: '16px',
    color: '#2B6A30',
    fontWeight: 700,
    lineHeight: 1,
    padding: '0 2px',
    flexShrink: 0,
  },
  selectorInputOverlay: {
    position: 'absolute',
    inset: 0,
    width: '100%',
    height: '100%',
    opacity: 0,
    cursor: 'pointer',
    outline: 'none',
    border: 'none',
  },
  refreshRight: { display: 'flex', flexDirection: 'column', alignItems: 'stretch', gap: '8px', position: 'relative', width: '100%' },
  filterWidgetWrap: { position: 'relative', width: '100%' },
  loadingScreen: { display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '60vh', gap: '16px' },
  spinner: { width: '40px', height: '40px', border: '3px solid rgba(203,230,200,0.5)', borderTop: '3px solid #1B5E20', borderRadius: '50%', animation: 'spin 1s linear infinite' },
  sectionDivider: { width: '100%', height: '2px', background: 'linear-gradient(90deg, transparent 0%, rgba(27,94,32,0.18) 20%, rgba(27,94,32,0.18) 80%, transparent 100%)', margin: '24px 0 4px 0' },
  sectionWrap: { position: 'relative', paddingLeft: '20px', marginLeft: '4px', marginTop: '0' },
  widgetCollapsed: {
    width: '100%',
    backgroundColor: 'rgba(243,248,243,0.78)',
    backdropFilter: 'blur(12px)', WebkitBackdropFilter: 'blur(12px)',
    borderRadius: '12px',
    boxShadow: 'none',
    overflow: 'hidden',
    cursor: 'pointer',
    transition: 'background-color 0.15s, border-color 0.15s',
    border: '1px solid rgba(176,205,174,0.55)',
  },
  infoCardRow: { padding: '10px 14px', display: 'flex', alignItems: 'center', gap: '10px' },
  infoCardIcon: { width: '32px', height: '32px', borderRadius: '10px', backgroundColor: 'rgba(27,94,32,0.08)', display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 },
  infoCardIconSvg: { width: '16px', height: '16px' },
  infoCardText: { display: 'flex', flexDirection: 'column', gap: '1px', overflow: 'hidden', flex: 1, minWidth: 0 },
  infoCardLabel: { fontSize: '9.5px', fontWeight: 700, color: '#8A928A', textTransform: 'uppercase', letterSpacing: '0.08em', lineHeight: 1.2 },
  infoCardValue: { fontSize: '12px', fontWeight: 600, color: '#1B5E20', lineHeight: 1.3, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' },
  infoCardValueDefault: { fontSize: '12px', fontWeight: 500, color: '#6B736B', lineHeight: 1.3 },
  activeDot: { width: '8px', height: '8px', borderRadius: '50%', backgroundColor: '#1B5E20', boxShadow: '0 0 6px rgba(27,94,32,0.4)', flexShrink: 0 },
  expandedPanel: {
    position: 'relative',
    zIndex: 1,
    width: '100%',
    maxHeight: 'none',
    overflowY: 'visible',
    backgroundColor: 'rgba(243,248,243,0.82)',
    backdropFilter: 'blur(14px)', WebkitBackdropFilter: 'blur(14px)',
    border: '1px solid rgba(176,205,174,0.55)',
    borderRadius: '12px',
    boxShadow: 'none',
    overflow: 'hidden',
  },
  expandedHeader: { padding: '10px 14px', display: 'flex', alignItems: 'center', gap: '10px', borderBottom: '1px solid rgba(27,94,32,0.1)' },
  expandedHeaderText: { flex: 1 },
  expandedTitle: { fontSize: '13px', fontWeight: 700, color: '#1B5E20', letterSpacing: '0.02em' },
  expandedSubtitle: { fontSize: '11px', fontWeight: 500, color: '#6B736B', marginTop: '1px' },
  closeBtn: { width: '30px', height: '30px', borderRadius: '9px', backgroundColor: 'rgba(27,94,32,0.08)', border: 'none', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '14px', color: '#6B736B', fontWeight: 600, transition: 'background-color 0.15s', flexShrink: 0 },
  expandedBody: { padding: '12px 14px 14px' },
  filterSectionTitle: { fontSize: '10px', fontWeight: 700, color: '#1B5E20', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: '12px' },
  filterHint: { fontSize: '10.5px', color: '#6B736B', marginTop: '12px', lineHeight: '1.5', borderTop: '1px solid rgba(27,94,32,0.08)', paddingTop: '10px' },
};

function FilterClockIcon() {
  return (
    <svg style={styles.infoCardIconSvg} viewBox="0 0 24 24" fill="none" stroke="#1B5E20" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="10" />
      <polyline points="12 6 12 12 16 14" />
    </svg>
  );
}

function SystemSelectorIcon() {
  return (
    <svg style={styles.infoCardIconSvg} viewBox="0 0 24 24" fill="none" stroke="#1B5E20" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="4" y="4" width="16" height="16" rx="3" />
      <path d="M8 12h8" />
    </svg>
  );
}

function DashboardBeta({ user, onLogout }) {
  const [timeseries, setTimeseries] = useState([]);
  const [timestampCol, setTimestampCol] = useState('timestamp_utc');
  const [loading, setLoading] = useState(true);
  const [filterOpen, setFilterOpen] = useState(false);
  const [selectedAlert, setSelectedAlert] = useState(null);
  const [widgetHovered, setWidgetHovered] = useState(false);
  const [subsystems, setSubsystems] = useState([]);
  const [fingerprints, setFingerprints] = useState([]);
  const [selectedMainSystem, setSelectedMainSystem] = useState(MAIN_SYSTEM_OPTIONS[0]);

  const timeFilter = useTimeFilter(timeseries, timestampCol);
  const hasActiveFilter = !!(timeFilter.filterLabel && timeFilter.filterLabel !== 'all data');

  // Snapshot of time filter state before a chart zoom, so we can restore on reset
  const [preZoomState, setPreZoomState] = useState(null);

  const handleChartZoom = useCallback((zoom) => {
    if (!zoom) return;
    // Save current time filter state only before the first zoom
    if (!preZoomState) {
      setPreZoomState({
        selectedDay: timeFilter.selectedDay,
        isLatestMode: timeFilter.isLatestMode,
        lastNHours: timeFilter.lastNHours,
        startTime: timeFilter.startTime,
        endTime: timeFilter.endTime,
      });
    }
    const startTs = String(zoom.start);
    const endTs = String(zoom.end);
    const day = startTs.substring(0, 10);
    const startHhmm = startTs.substring(11, 16);
    const endHhmm = endTs.substring(11, 16);
    timeFilter.handleDayChange(day);
    timeFilter.setStartTime(startHhmm);
    timeFilter.setEndTime(endHhmm);
  }, [timeFilter, preZoomState]);

  const handleZoomReset = useCallback(() => {
    if (!preZoomState) return;
    if (preZoomState.isLatestMode) {
      timeFilter.handleLatestClick();
      timeFilter.setLastNHours(preZoomState.lastNHours);
    } else {
      timeFilter.handleDayChange(preZoomState.selectedDay);
      timeFilter.setStartTime(preZoomState.startTime);
      timeFilter.setEndTime(preZoomState.endTime);
    }
    setPreZoomState(null);
  }, [preZoomState, timeFilter]);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [tsRes, subsRes, fpRes] = await Promise.allSettled([
        getBetaScoresTimeseries(),
        getBetaSubsystems(),
        getBetaRadarFingerprints(),
      ]);

      if (subsRes.status === 'fulfilled') setSubsystems(subsRes.value.data.subsystems || []);
      if (fpRes.status === 'fulfilled') setFingerprints(fpRes.value.data.fingerprints || []);
      if (tsRes.status === 'fulfilled') {
        const tsData = tsRes.value.data;
        setTimeseries(tsData.timeseries || []);
        setTimestampCol(tsData.timestamp_col || 'timestamp_utc');
      }
    } catch (err) {
      console.error('Failed to load beta dashboard data:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadData(); }, [loadData]);

  return (
    <div style={styles.page}>
      <AuroraBg />
      <div style={styles.contentWrap}>
        <TopBarBeta user={user} onLogout={onLogout} />
        <div style={styles.content}>
          <div style={styles.refreshBar}>
            <div style={styles.selectorWrap}>
              <div style={styles.selectorCard}>
                <div style={styles.infoCardRow}>
                  <div style={styles.infoCardIcon}><SystemSelectorIcon /></div>
                  <div style={styles.infoCardText}>
                    <div style={styles.infoCardLabel}>System Selector</div>
                    <div style={styles.infoCardValue}>{selectedMainSystem}</div>
                  </div>
                  <div style={styles.selectorChevron}>v</div>
                </div>
                <select
                  value={selectedMainSystem}
                  onChange={(e) => setSelectedMainSystem(e.target.value)}
                  style={styles.selectorInputOverlay}
                >
                  {MAIN_SYSTEM_OPTIONS.map((systemName) => (
                    <option key={systemName} value={systemName}>{systemName}</option>
                  ))}
                </select>
              </div>
            </div>
            <div style={styles.refreshRight}>
              {!loading && (
                <div style={styles.filterWidgetWrap}>
                  {!filterOpen ? (
                    <div
                      style={{
                        ...styles.widgetCollapsed,
                        backgroundColor: widgetHovered ? 'rgba(228,239,227,0.92)' : styles.widgetCollapsed.backgroundColor,
                        borderColor: widgetHovered ? 'rgba(120,170,118,0.6)' : 'rgba(176,205,174,0.55)',
                      }}
                      onClick={() => setFilterOpen(true)}
                      onMouseEnter={() => setWidgetHovered(true)}
                      onMouseLeave={() => setWidgetHovered(false)}
                    >
                      <div style={styles.infoCardRow}>
                        <div style={styles.infoCardIcon}><FilterClockIcon /></div>
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
                    </div>
                  ) : (
                    <div style={styles.expandedPanel}>
                      <div style={styles.expandedHeader}>
                        <div style={styles.infoCardIcon}><FilterClockIcon /></div>
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
                        >X</button>
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
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>

          {loading ? (
            <div style={styles.loadingScreen}>
              <div style={styles.spinner} />
              <div style={{ color: '#6B736B', fontSize: '14px' }}>Loading dashboard...</div>
            </div>
          ) : (
            <>
              {/* Sensor Validation Report */}
              <div style={styles.sectionDivider} />
              <div style={styles.sectionWrap}>
                <SensorValidationReport />
              </div>

              {/* Pipeline Overview */}
              <div style={styles.sectionDivider} />
              <div style={styles.sectionWrap}>
                <BetaOverviewCards
                  selectedSystemName={selectedMainSystem}
                  fingerprints={fingerprints}
                  timeseries={timeseries}
                  timestampCol={timestampCol}
                  selectedDay={timeFilter.selectedDay}
                  isLatestMode={timeFilter.isLatestMode}
                  lastNHours={timeFilter.lastNHours}
                  startTime={timeFilter.startTime}
                  endTime={timeFilter.endTime}
                />
              </div>

              {/* Sensor Quality Grid */}
              <div style={styles.sectionDivider} />
              <div style={styles.sectionWrap}>
                <SensorQualityGrid
                  onSelectAlert={setSelectedAlert}
                  selectedDay={timeFilter.selectedDay}
                  isLatestMode={timeFilter.isLatestMode}
                  lastNHours={timeFilter.lastNHours}
                  startTime={timeFilter.startTime}
                  endTime={timeFilter.endTime}
                  onZoomChange={handleChartZoom}
                  onZoomReset={handleZoomReset}
                  isZoomed={!!preZoomState}
                  subsystems={subsystems}
                  fingerprints={fingerprints}
                />
              </div>

              {/* Subsystem Behavior */}
              <div style={styles.sectionDivider} />
              <div style={styles.sectionWrap}>
                <LazySection height="400px">
                <SubsystemBehaviorChartBeta
                  selectedDay={timeFilter.selectedDay}
                  isLatestMode={timeFilter.isLatestMode}
                  lastNHours={timeFilter.lastNHours}
                  startTime={timeFilter.startTime}
                  endTime={timeFilter.endTime}
                  onZoomChange={handleChartZoom}
                  onZoomReset={handleZoomReset}
                  isZoomed={!!preZoomState}
                  onSelectAlert={setSelectedAlert}
                  subsystems={subsystems}
                />
                </LazySection>
              </div>

            </>
          )}
        </div>
      </div>

      {selectedAlert && (
        <BetaAlertDetailModal alert={selectedAlert} onClose={() => setSelectedAlert(null)} />
      )}

      <FeedbackWidget
        user={user}
        title="Beta Dashboard Feedback"
        sections={BETA_FEEDBACK_SECTIONS}
      />
    </div>
  );
}

export default DashboardBeta;
