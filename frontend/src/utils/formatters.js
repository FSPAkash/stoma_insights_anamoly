export const formatScore = (value, decimals = 3) => {
  if (value === null || value === undefined) return '--';
  return Number(value).toFixed(decimals);
};

export const formatPercent = (value, decimals = 1) => {
  if (value === null || value === undefined) return '--';
  return `${Number(value).toFixed(decimals)}%`;
};

export const formatTimestamp = (ts) => {
  if (!ts) return '--';
  try {
    const d = new Date(ts);
    return d.toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
      timeZone: 'UTC',
    }) + ' UTC';
  } catch {
    return String(ts).substring(0, 19);
  }
};

export const formatDuration = (minutes) => {
  if (minutes === null || minutes === undefined) return '--';
  const m = Number(minutes);
  if (m < 60) return `${Math.round(m)}m`;
  const h = Math.floor(m / 60);
  const rem = Math.round(m % 60);
  return rem > 0 ? `${h}h ${rem}m` : `${h}h`;
};

export const formatSensorName = (name) => {
  if (!name) return '--';
  return String(name)
    .replace(/^DESF_TA__/, '')
    .replace(/__/g, ' ')
    .replace(/_/g, ' ');
};

export const severityColor = (severity) => {
  switch (severity) {
    case 'HIGH': return { bg: '#FFCDD2', text: '#C62828', border: '#EF9A9A' };
    case 'MEDIUM': return { bg: '#FFE0B2', text: '#E65100', border: '#FFCC80' };
    default: return { bg: '#E6F4EA', text: '#1B5E20', border: '#A8DCA8' };
  }
};

const SYSTEM_COLORS = [
  '#1B5E20', '#0D47A1', '#E65100', '#4A148C',
  '#004D40', '#BF360C', '#1A237E', '#33691E',
];

export const classColor = (cls) => {
  const map = {
    MECH: '#1B5E20',
    ELEC: '#388E3C',
    THERM: '#FFA726',
    PROCESS: '#7B1FA2',
    INSTRUMENT: '#1565C0',
    NORMAL: '#81C784',
  };
  if (map[cls]) return map[cls];
  const match = cls && cls.match(/^SYS_(\d+)$/);
  if (match) {
    const idx = (parseInt(match[1], 10) - 1) % SYSTEM_COLORS.length;
    return SYSTEM_COLORS[idx];
  }
  return '#6B736B';
};

export const systemColor = (sysId, index) => {
  const match = sysId && sysId.match(/^SYS_(\d+)$/);
  if (match) {
    const idx = (parseInt(match[1], 10) - 1) % SYSTEM_COLORS.length;
    return SYSTEM_COLORS[idx];
  }
  return SYSTEM_COLORS[(index || 0) % SYSTEM_COLORS.length];
};

export const systemBgColor = (sysId) => {
  const colorMap = {
    SYS_1: '#E8F5E9',
    SYS_2: '#E3F2FD',
    SYS_3: '#FFF3E0',
    SYS_4: '#F3E5F5',
    PROCESS: '#F3E5F5',
    INSTRUMENT: '#E3F2FD',
  };
  return colorMap[sysId] || '#F5F7F5';
};