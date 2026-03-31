import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import TopBarBeta from './TopBarBeta';
import BetaOverviewCards from './BetaOverviewCards';
import BetaAlertDetailModal from './BetaAlertDetailModal';
import SensorQualityGrid from './SensorQualityGrid';
import SubsystemBehaviorChartBeta from './SubsystemBehaviorChartBeta';
import SensorValidationReport from './SensorValidationReport';
import StandaloneSensorChart from './StandaloneSensorChart';
import FeedbackWidget from './FeedbackWidget';
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

const MAIN_SYSTEM_OPTIONS = ['Shredder', 'Placeholder1', 'Placeholder2'];

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
  loadingScreen: { display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '60vh', gap: '16px' },
  spinner: { width: '40px', height: '40px', border: '3px solid rgba(203,230,200,0.5)', borderTop: '3px solid #1B5E20', borderRadius: '50%', animation: 'spin 1s linear infinite' },
  sectionDivider: { width: '100%', height: '2px', background: 'linear-gradient(90deg, transparent 0%, rgba(27,94,32,0.18) 20%, rgba(27,94,32,0.18) 80%, transparent 100%)', margin: '24px 0' },
  sectionWrap: { position: 'relative', paddingLeft: '20px', marginLeft: '4px', borderLeft: '2px solid rgba(76,175,80,0.12)' },
};

function DashboardBeta({ user, onLogout }) {
  const [timeseries, setTimeseries] = useState([]);
  const [timestampCol, setTimestampCol] = useState('timestamp_utc');
  const [loading, setLoading] = useState(true);
  const [selectedAlert, setSelectedAlert] = useState(null);
  const [subsystems, setSubsystems] = useState([]);
  const [fingerprints, setFingerprints] = useState([]);
  const [selectedMainSystem, setSelectedMainSystem] = useState(MAIN_SYSTEM_OPTIONS[0]);
  const [statusByDay, setStatusByDay] = useState({});

  // Gate all data by the selected system -- only Shredder has real data for now
  const hasData = selectedMainSystem === 'Shredder';
  const activeTimeseries = useMemo(() => hasData ? timeseries : [], [hasData, timeseries]);
  const activeSubsystems = useMemo(() => hasData ? subsystems : [], [hasData, subsystems]);
  const activeFingerprints = useMemo(() => hasData ? fingerprints : [], [hasData, fingerprints]);

  const timeFilter = useTimeFilter(activeTimeseries, timestampCol);
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
        allDaysMode: timeFilter.allDaysMode,
      });
    }
    const startTs = String(zoom.start);
    const endTs = String(zoom.end);
    const startDay = startTs.substring(0, 10);
    const endDay = endTs.substring(0, 10);
    if (startDay !== endDay) {
      // Cross-day zoom: show the clicked day fully
      const clickDay = zoom.clickDay || startDay;
      timeFilter.handleDayChange(clickDay);
    } else {
      const startHhmm = startTs.substring(11, 16);
      const endHhmm = endTs.substring(11, 16);
      timeFilter.handleDayChange(startDay);
      timeFilter.setStartTime(startHhmm);
      timeFilter.setEndTime(endHhmm);
    }
  }, [timeFilter, preZoomState]);

  const handleScrollDayChange = useCallback((day) => {
    if (day && timeFilter.allDaysMode) {
      timeFilter.setSelectedDay(day);
    }
  }, [timeFilter]);

  const handleZoomReset = useCallback(() => {
    if (!preZoomState) return;
    if (preZoomState.allDaysMode) {
      timeFilter.handleAllDaysClick();
    } else if (preZoomState.isLatestMode) {
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
          {loading ? (
            <div style={styles.loadingScreen}>
              <div style={styles.spinner} />
              <div style={{ color: '#6B736B', fontSize: '14px' }}>Loading dashboard...</div>
            </div>
          ) : (
            <>
              {/* Pipeline Overview (with system selector built into the card) */}
              <div style={styles.sectionWrap}>
                <BetaOverviewCards
                  selectedSystemName={selectedMainSystem}
                  systemOptions={MAIN_SYSTEM_OPTIONS}
                  onSystemChange={setSelectedMainSystem}
                  fingerprints={activeFingerprints}
                  timeseries={activeTimeseries}
                  timestampCol={timestampCol}
                  selectedDay={timeFilter.selectedDay}
                  isLatestMode={timeFilter.isLatestMode}
                  lastNHours={timeFilter.lastNHours}
                  startTime={timeFilter.startTime}
                  endTime={timeFilter.endTime}
                  hasData={hasData}
                  onStatusByDay={setStatusByDay}
                />
              </div>

              {/* Sensor Validation Report */}
              <div style={styles.sectionDivider} />
              <div style={styles.sectionWrap}>
                <SensorValidationReport hasData={hasData} />
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
                  allDaysMode={timeFilter.allDaysMode}
                  onScrollDayChange={handleScrollDayChange}
                  onZoomChange={handleChartZoom}
                  onZoomReset={handleZoomReset}
                  isZoomed={!!preZoomState}
                  subsystems={activeSubsystems}
                  fingerprints={activeFingerprints}
                  hasData={hasData}
                  statusByDay={statusByDay}
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
                  allDaysMode={timeFilter.allDaysMode}
                  onScrollDayChange={handleScrollDayChange}
                  onZoomChange={handleChartZoom}
                  onZoomReset={handleZoomReset}
                  isZoomed={!!preZoomState}
                  onSelectAlert={setSelectedAlert}
                  subsystems={activeSubsystems}
                  hasData={hasData}
                  statusByDay={statusByDay}
                />
                </LazySection>
              </div>

              {/* Standalone Sensor Analysis */}
              <div style={styles.sectionDivider} />
              <div style={styles.sectionWrap}>
                <LazySection height="400px">
                <StandaloneSensorChart
                  selectedDay={timeFilter.selectedDay}
                  isLatestMode={timeFilter.isLatestMode}
                  lastNHours={timeFilter.lastNHours}
                  startTime={timeFilter.startTime}
                  endTime={timeFilter.endTime}
                  allDaysMode={timeFilter.allDaysMode}
                  onScrollDayChange={handleScrollDayChange}
                  onZoomChange={handleChartZoom}
                  onZoomReset={handleZoomReset}
                  isZoomed={!!preZoomState}
                  onSelectAlert={setSelectedAlert}
                  hasData={hasData}
                  statusByDay={statusByDay}
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
