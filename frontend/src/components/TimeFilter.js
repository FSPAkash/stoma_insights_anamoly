import React from 'react';

const HOUR_OPTIONS = [1, 2, 4, 8, 12, 24];

const timeInputStyle = {
  fontSize: '12px',
  padding: '4px 8px',
  borderRadius: '6px',
  border: '1px solid rgba(203,230,200,0.6)',
  background: 'rgba(255,255,255,0.8)',
  color: '#1B5E20',
  fontWeight: 500,
  fontVariantNumeric: 'tabular-nums',
  outline: 'none',
  cursor: 'pointer',
};

const pillStyle = (active) => ({
  padding: '4px 10px',
  fontSize: '11px',
  fontWeight: active ? 600 : 400,
  color: active ? '#fff' : '#4a524a',
  background: active ? '#1B5E20' : 'rgba(0,0,0,0.04)',
  border: active ? '1px solid #1B5E20' : '1px solid rgba(203,230,200,0.5)',
  borderRadius: '6px',
  cursor: 'pointer',
  transition: 'all 0.15s ease',
  userSelect: 'none',
  whiteSpace: 'nowrap',
});

function TimeFilter({
  availableDays,
  selectedDay,
  isLatestMode,
  lastNHours,
  startTime,
  endTime,
  allDaysMode,
  onSelectDay,
  onLatestClick,
  onAllDaysClick,
  onLastNHoursChange,
  onStartTimeChange,
  onEndTimeChange,
  onReset,
}) {
  const latestDay = availableDays.length ? availableDays[availableDays.length - 1] : null;

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '16px', flexWrap: 'wrap', marginBottom: '14px', padding: '8px 14px', background: 'rgba(245,248,245,0.6)', borderRadius: '10px', border: '1px solid rgba(203,230,200,0.35)' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
        <div style={pillStyle(allDaysMode)} onClick={onAllDaysClick}>
          All Days
        </div>
        <div style={pillStyle(isLatestMode && !allDaysMode)} onClick={onLatestClick}>
          Latest{latestDay ? ` (${new Date(latestDay + 'T00:00:00').toLocaleDateString('en-US', { month: 'short', day: 'numeric' })})` : ''}
        </div>
        <span style={{ fontSize: '10px', color: '#8A928A', fontWeight: 500 }}>or</span>
        <select
          value={isLatestMode ? '' : (selectedDay || '')}
          onChange={(e) => onSelectDay(e.target.value)}
          style={{ ...timeInputStyle, paddingRight: '4px', color: isLatestMode ? '#8A928A' : '#1B5E20' }}
        >
          {isLatestMode && <option value="">Select date...</option>}
          {availableDays.map((day) => {
            const d = new Date(day + 'T00:00:00');
            const label = d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
            return <option key={day} value={day}>{label}</option>;
          })}
        </select>
      </div>
      <span style={{ fontSize: '9px', color: '#8A928A', fontWeight: 600, letterSpacing: '0.05em' }}>UTC</span>
      <div style={{ width: '1px', height: '20px', background: 'rgba(203,230,200,0.6)' }} />
      {isLatestMode ? (
        <div style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
          {HOUR_OPTIONS.map((h) => (
            <div key={h} style={pillStyle(lastNHours === h)} onClick={() => onLastNHoursChange(h)}>
              {h === 24 ? 'Full Day' : `${h}h`}
            </div>
          ))}
        </div>
      ) : (
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <span style={{ fontSize: '10px', fontWeight: 600, color: '#8A928A', textTransform: 'uppercase', letterSpacing: '0.05em' }}>From</span>
          <input type="time" value={startTime} onChange={(e) => onStartTimeChange(e.target.value)} style={timeInputStyle} />
          <span style={{ fontSize: '10px', fontWeight: 600, color: '#8A928A', textTransform: 'uppercase', letterSpacing: '0.05em' }}>To</span>
          <input type="time" value={endTime} onChange={(e) => onEndTimeChange(e.target.value)} style={timeInputStyle} />
          {(startTime !== '00:00' || endTime !== '23:59') && (
            <div
              onClick={onReset}
              style={{ fontSize: '10px', color: '#8A928A', cursor: 'pointer', padding: '2px 6px', borderRadius: '4px', border: '1px solid rgba(203,230,200,0.5)' }}
            >
              Reset
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default TimeFilter;
