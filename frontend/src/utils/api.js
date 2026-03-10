import axios from 'axios';

const API_BASE = (process.env.REACT_APP_API_URL || '/api').replace(/\/+$/, '');

const api = axios.create({
  baseURL: API_BASE,
  timeout: 30000,
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

export const submitFeedback = (feedback) =>
  api.post('/feedback', feedback);

export const runPipeline = () =>
  api.post('/run_pipeline');

export const getPipelineStatus = () =>
  api.get('/pipeline_status');

export default api;
