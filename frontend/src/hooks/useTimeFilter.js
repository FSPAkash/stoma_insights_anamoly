import { useState, useMemo, useEffect } from 'react';

export default function useTimeFilter(timeseries, timestampCol) {
  const [selectedDay, setSelectedDay] = useState(null);
  const [isLatestMode, setIsLatestMode] = useState(false);
  const [lastNHours, setLastNHours] = useState(24);
  const [startTime, setStartTime] = useState('00:00');
  const [endTime, setEndTime] = useState('23:59');
  const [allDaysMode, setAllDaysMode] = useState(true);

  // Extract unique days
  const availableDays = useMemo(() => {
    if (!timeseries || !timeseries.length) return [];
    const daySet = new Set();
    timeseries.forEach((row) => {
      const ts = row[timestampCol];
      if (ts) daySet.add(String(ts).substring(0, 10));
    });
    return Array.from(daySet).sort();
  }, [timeseries, timestampCol]);

  const latestDay = availableDays.length ? availableDays[availableDays.length - 1] : null;

  // Default to most recent day
  useEffect(() => {
    if (availableDays.length && !availableDays.includes(selectedDay)) {
      setSelectedDay(availableDays[availableDays.length - 1]);
      setIsLatestMode(true);
    }
  }, [availableDays, selectedDay]);

  const handleLatestClick = () => {
    setSelectedDay(latestDay);
    setIsLatestMode(true);
    setLastNHours(24);
    setAllDaysMode(false);
  };

  const handleDayChange = (day) => {
    setSelectedDay(day);
    setIsLatestMode(false);
    setStartTime('00:00');
    setEndTime('23:59');
    setAllDaysMode(false);
  };

  const handleAllDaysClick = () => {
    setAllDaysMode(true);
    setIsLatestMode(false);
  };

  const handleReset = () => {
    setStartTime('00:00');
    setEndTime('23:59');
  };

  // Filter timeseries by current day + time selection
  const filteredTimeseries = useMemo(() => {
    if (!timeseries || !timeseries.length) return [];
    if (allDaysMode) return timeseries;
    if (!selectedDay) return [];
    // Filter by day
    const dayData = timeseries.filter((row) => {
      const ts = row[timestampCol];
      return ts && String(ts).substring(0, 10) === selectedDay;
    });
    // Apply time filter
    if (isLatestMode && lastNHours < 24 && dayData.length > 0) {
      const lastTs = dayData[dayData.length - 1][timestampCol];
      if (lastTs) {
        const end = new Date(String(lastTs));
        const start = new Date(end.getTime() - lastNHours * 60 * 60 * 1000);
        return dayData.filter((row) => new Date(String(row[timestampCol])) >= start);
      }
    } else if (!isLatestMode) {
      return dayData.filter((row) => {
        const hhmm = String(row[timestampCol]).substring(11, 16);
        return hhmm >= startTime && hhmm <= endTime;
      });
    }
    return dayData;
  }, [timeseries, timestampCol, selectedDay, isLatestMode, lastNHours, startTime, endTime, allDaysMode]);

  // Compute stats from filtered timeseries
  const filteredStats = useMemo(() => {
    if (!filteredTimeseries.length) return null;
    // Fixed keys + dynamic score_SYS_* and risk_SYS_* keys from timeseries
    const fixedKeys = ['risk_score', 'mech_score', 'elec_score', 'therm_score', 'physics_score', 'subsystem_score', 'sqs_mean'];
    const dynamicKeys = filteredTimeseries.length > 0
      ? Object.keys(filteredTimeseries[0]).filter((k) => k.startsWith('score_SYS_') || k.startsWith('risk_SYS_'))
      : [];
    const scoreKeys = [...new Set([...fixedKeys, ...dynamicKeys])];
    const stats = {};
    for (const key of scoreKeys) {
      const vals = filteredTimeseries
        .map((r) => r[key])
        .filter((v) => v !== null && v !== undefined && !isNaN(v))
        .map(Number);
      if (!vals.length) continue;
      const mean = vals.reduce((a, b) => a + b, 0) / vals.length;
      const max = Math.max(...vals);
      const min = Math.min(...vals);
      const variance = vals.reduce((a, b) => a + (b - mean) ** 2, 0) / vals.length;
      stats[key] = {
        mean,
        max,
        min,
        std: Math.sqrt(variance),
      };
    }
    return Object.keys(stats).length ? stats : null;
  }, [filteredTimeseries]);

  // Compute normal stats from filtered timeseries
  const filteredNormalData = useMemo(() => {
    if (!filteredTimeseries.length) return null;
    const total = filteredTimeseries.length;
    const normalRows = filteredTimeseries.filter((r) => r.class === 'NORMAL');
    const normalCount = normalRows.length;
    const normalPct = total > 0 ? Math.round((normalCount / total) * 100) : 0;
    const riskVals = normalRows.map((r) => Number(r.risk_score)).filter((v) => !isNaN(v));
    const sqsVals = normalRows.map((r) => Number(r.sqs_mean)).filter((v) => !isNaN(v));
    const avg = (arr) => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null;
    return {
      normal_stats: {
        normal_pct: normalPct,
        normal_count: normalCount,
        avg_risk_during_normal: avg(riskVals),
        avg_sqs_during_normal: avg(sqsVals),
      },
    };
  }, [filteredTimeseries]);

  // Human-readable label for the active filter
  const filterLabel = useMemo(() => {
    if (allDaysMode) {
      if (availableDays.length >= 2) {
        const first = new Date(availableDays[0] + 'T00:00:00').toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        const last = new Date(availableDays[availableDays.length - 1] + 'T00:00:00').toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        return `Now showing All Days -- ${first} to ${last} (UTC)`;
      }
      return 'Now showing All Days (UTC)';
    }
    if (!selectedDay) return '';
    const d = new Date(selectedDay + 'T00:00:00');
    const dayStr = d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    if (isLatestMode) {
      return lastNHours >= 24 ? `Now showing ${dayStr} -- Full Day (UTC)` : `Now showing ${dayStr} -- Last ${lastNHours}h (UTC)`;
    }
    const timeRange = (startTime === '00:00' && endTime === '23:59')
      ? 'Full Day'
      : `${startTime} to ${endTime}`;
    return `Now showing ${dayStr} -- ${timeRange} (UTC)`;
  }, [selectedDay, isLatestMode, lastNHours, startTime, endTime, allDaysMode, availableDays]);

  return {
    // Filter state
    selectedDay,
    isLatestMode,
    lastNHours,
    startTime,
    endTime,
    availableDays,
    filterLabel,
    allDaysMode,
    // Handlers
    handleLatestClick,
    handleDayChange,
    handleAllDaysClick,
    setLastNHours,
    setStartTime: (v) => setStartTime(v),
    setEndTime: (v) => setEndTime(v),
    setSelectedDay,
    handleReset,
    // Filtered data
    filteredTimeseries,
    filteredStats,
    filteredNormalData,
  };
}
