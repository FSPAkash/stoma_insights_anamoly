import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import InfoTooltip from './InfoTooltip';

function GaugeWidget({ value, max = 1, label, size = 120, color = '#4CAF50', sublabel, tooltip, hoverDetail }) {
  const [hovered, setHovered] = useState(false);
  const normalized = Math.min(Math.max((value || 0) / max, 0), 1);
  const angle = normalized * 240 - 120;
  const cx = size / 2;
  const cy = size / 2;
  const r = size / 2 - 14;

  const arcPath = (startAngle, endAngle) => {
    const s = ((startAngle - 90) * Math.PI) / 180;
    const e = ((endAngle - 90) * Math.PI) / 180;
    const x1 = cx + r * Math.cos(s);
    const y1 = cy + r * Math.sin(s);
    const x2 = cx + r * Math.cos(e);
    const y2 = cy + r * Math.sin(e);
    const largeArc = endAngle - startAngle > 180 ? 1 : 0;
    return `M ${x1} ${y1} A ${r} ${r} 0 ${largeArc} 1 ${x2} ${y2}`;
  };

  const needleEnd = {
    x: cx + (r - 8) * Math.cos(((angle - 90) * Math.PI) / 180),
    y: cy + (r - 8) * Math.sin(((angle - 90) * Math.PI) / 180),
  };

  const ticks = [];
  for (let i = 0; i <= 10; i++) {
    const tickAngle = -120 + i * 24;
    const rad = ((tickAngle - 90) * Math.PI) / 180;
    const outerR = r + 4;
    const innerR = r - (i % 5 === 0 ? 6 : 3);
    ticks.push(
      <line
        key={i}
        x1={cx + innerR * Math.cos(rad)}
        y1={cy + innerR * Math.sin(rad)}
        x2={cx + outerR * Math.cos(rad)}
        y2={cy + outerR * Math.sin(rad)}
        stroke={i % 5 === 0 ? '#4A524A' : '#B0B8B0'}
        strokeWidth={i % 5 === 0 ? 1.5 : 0.8}
        strokeLinecap="round"
      />
    );
  }

  return (
    <div
      style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '4px', position: 'relative' }}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      <svg width={size} height={size * 0.75} viewBox={`0 0 ${size} ${size * 0.85}`}>
        <path d={arcPath(-120, 120)} fill="none" stroke="#E8ECE8" strokeWidth="6" strokeLinecap="round" />
        <path
          d={arcPath(-120, -120 + normalized * 240)}
          fill="none"
          stroke={color}
          strokeWidth="6"
          strokeLinecap="round"
          opacity="0.8"
        />
        {ticks}
        <motion.line
          x1={cx} y1={cy} x2={needleEnd.x} y2={needleEnd.y}
          stroke="#1A1F1A" strokeWidth="2" strokeLinecap="round"
          initial={{ x2: cx, y2: cy - r + 8 }}
          animate={{ x2: needleEnd.x, y2: needleEnd.y }}
          transition={{ duration: 1.2, ease: [0.4, 0, 0.2, 1] }}
        />
        <circle cx={cx} cy={cy} r="4" fill="#1A1F1A" />
        <text x={cx} y={cy + 20} textAnchor="middle" fontSize="16" fontWeight="600" fill="#1A1F1A" fontFamily="Inter, sans-serif">
          {value !== null && value !== undefined ? Number(value).toFixed(3) : '--'}
        </text>
      </svg>
      <div style={{ fontSize: '12px', fontWeight: 500, color: '#4A524A', textAlign: 'center', letterSpacing: '0.01em', display: 'flex', alignItems: 'center', gap: '2px' }}>
        {label}
        {tooltip && <InfoTooltip text={tooltip} />}
      </div>
      {sublabel && (
        <div style={{ fontSize: '11px', color: '#8A928A', textAlign: 'center' }}>{sublabel}</div>
      )}
      <AnimatePresence>
        {hovered && hoverDetail && (
          <motion.div
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: 6 }}
            transition={{ duration: 0.15 }}
            style={{
              position: 'absolute',
              top: '100%',
              left: '50%',
              transform: 'translateX(-50%)',
              marginTop: '4px',
              background: 'rgba(255,255,255,0.96)',
              backdropFilter: 'blur(16px)',
              WebkitBackdropFilter: 'blur(16px)',
              border: '1px solid rgba(203,230,200,0.6)',
              borderRadius: '10px',
              padding: '8px 12px',
              boxShadow: '0 8px 24px rgba(27,94,32,0.12)',
              fontSize: '11px',
              color: '#2D332D',
              whiteSpace: 'nowrap',
              zIndex: 100,
              textTransform: 'none',
              letterSpacing: 'normal',
              pointerEvents: 'none',
            }}
          >
            {hoverDetail}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

export default GaugeWidget;
