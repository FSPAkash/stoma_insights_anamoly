import axios from 'axios';

const API_BASE = (process.env.REACT_APP_API_URL || '/api').replace(/\/+$/, '');

const api = axios.create({
  baseURL: API_BASE,
  timeout: 120000,
  headers: {
    'Content-Type': 'application/json',
  },
});

api.interceptors.response.use((response) => {
  if (typeof response.data === 'string') {
    try { response.data = JSON.parse(response.data); } catch { /* not JSON */ }
  }
  return response;
});

// In-flight request dedup: prevents identical GET requests from firing concurrently
const _inflight = new Map();
function dedupGet(url, config = {}) {
  const key = url + JSON.stringify(config.params || {});
  if (_inflight.has(key)) return _inflight.get(key);
  const promise = api.get(url, config).finally(() => _inflight.delete(key));
  _inflight.set(key, promise);
  return promise;
}

export const login = (username, password) =>
  api.post('/login', { username, password });

export const getDashboardSummary = () =>
  api.get('/dashboard/summary');

export const getAlerts = () =>
  api.get('/alerts');

export const getAlertsSensorLevel = (params = {}) =>
  api.get('/alerts_sensor_level', { params });

export const getScoresTimeseries = (downsample = 10) =>
  api.get('/scores/timeseries', { params: { downsample } });

export const getScores = (params = {}) =>
  api.get('/scores', { params });

export const getRiskDecomposition = (params = {}) =>
  api.get('/risk_decomposition', { params });

export const getRiskDecompositionForEpisode = (startTs, endTs) =>
  api.get('/risk_decomposition/episode', { params: { start_ts: startTs, end_ts: endTs } });

export const getSensorConfig = () =>
  api.get('/sensor_config');

export const getSensorDetail = (sensorId) =>
  api.get(`/sensor/${encodeURIComponent(sensorId)}/detail`);

export const getNormalPeriods = () =>
  api.get('/normal_periods');

export const getSystems = () =>
  api.get('/systems');

export const getSystemSensorValues = (systemId, downsample = 5) =>
  api.get(`/systems/${encodeURIComponent(systemId)}/sensors`, { params: { downsample } });

export const submitFeedback = (feedback) =>
  api.post('/feedback', feedback);

export const runPipeline = () =>
  api.post('/run_pipeline');

export const getPipelineStatus = () =>
  api.get('/pipeline_status');

// =============================================================================
// BETA API
// =============================================================================
export const betaLogin = (username, password) =>
  api.post('/beta/login', { username, password });

export const getBetaOverview = () =>
  api.get('/beta/overview');

export const getBetaInvalidSensors = (threshold = 0.10) =>
  api.get('/beta/invalid_sensors', { params: { threshold } });

export const getBetaSensorValidationReport = () =>
  api.get('/beta/sensor_validation_report');

export const getBetaSubsystems = () =>
  api.get('/beta/subsystems');

export const getBetaSensorQuality = (systemId, downsample = 1, params = {}) =>
  api.get(`/beta/sensor_quality/${encodeURIComponent(systemId)}`, { params: { downsample, ...params } });

export const getBetaSensorQualityWindow = (systemId, startTs, endTs, downsample = 4) =>
  api.get(`/beta/sensor_quality/${encodeURIComponent(systemId)}`, { params: { downsample, start_ts: startTs, end_ts: endTs } });

export const getBetaSubsystemScores = (downsample = 1) =>
  api.get('/beta/subsystem_scores', { params: { downsample } });

export const getBetaAeMetadata = () =>
  api.get('/beta/ae_metadata');

export const getBetaAlerts = (params = {}) =>
  dedupGet('/beta/alerts', { params });

export const getBetaScoresTimeseries = (downsample = 1) =>
  api.get('/beta/scores/timeseries', { params: { downsample } });

export const getBetaDashboardSummary = (params = {}) =>
  api.get('/beta/dashboard/summary', { params });

export const getBetaScores = (params = {}) =>
  api.get('/beta/scores', { params });

export const getBetaNormalPeriods = () =>
  api.get('/beta/normal_periods');

export const getBetaSystems = () =>
  api.get('/beta/systems');

export const getBetaSubsystemBehavior = (systemId, downsample = 1, params = {}) =>
  api.get(`/beta/subsystem_behavior/${encodeURIComponent(systemId)}`, { params: { downsample, ...params } });

export const getBetaAlertsSensorLevel = (params = {}) =>
  dedupGet('/beta/alerts_sensor_level', { params });

export const getBetaRadarFingerprints = () =>
  dedupGet('/beta/radar_fingerprints');

export const getBetaSensorContributions = (systemId, downsample = 1) =>
  api.get(`/beta/sensor_contributions/${encodeURIComponent(systemId)}`, { params: { downsample } });

export const getBetaRiskDecompositionForEpisode = (startTs, endTs) =>
  dedupGet('/beta/risk_decomposition/episode', { params: { start_ts: startTs, end_ts: endTs } });

export default api;
