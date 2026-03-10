import React from 'react';

function AnalogClock({ size = 40, time }) {
  const hours = time.getHours() % 12;
  const minutes = time.getMinutes();
  const seconds = time.getSeconds();

  const hourAngle = (hours + minutes / 60) * 30;
  const minuteAngle = (minutes + seconds / 60) * 6;
  const secondAngle = seconds * 6;

  const cx = size / 2;
  const cy = size / 2;
  const r = size / 2 - 2;

  const polarToCart = (angle, length) => {
    const rad = ((angle - 90) * Math.PI) / 180;
    return {
      x: cx + length * Math.cos(rad),
      y: cy + length * Math.sin(rad),
    };
  };

  const hourHand = polarToCart(hourAngle, r * 0.5);
  const minuteHand = polarToCart(minuteAngle, r * 0.7);
  const secondHand = polarToCart(secondAngle, r * 0.8);

  const ticks = [];
  for (let i = 0; i < 12; i++) {
    const angle = i * 30;
    const outer = polarToCart(angle, r - 1);
    const inner = polarToCart(angle, r - (i % 3 === 0 ? 5 : 3));
    ticks.push(
      <line
        key={i}
        x1={inner.x}
        y1={inner.y}
        x2={outer.x}
        y2={outer.y}
        stroke={i % 3 === 0 ? '#1B5E20' : '#A8DCA8'}
        strokeWidth={i % 3 === 0 ? 1.5 : 0.8}
        strokeLinecap="round"
      />
    );
  }

  return (
    <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
      <circle
        cx={cx}
        cy={cy}
        r={r}
        fill="rgba(255,255,255,0.9)"
        stroke="#CBE6C8"
        strokeWidth="1.5"
      />
      {ticks}
      <line
        x1={cx}
        y1={cy}
        x2={hourHand.x}
        y2={hourHand.y}
        stroke="#1B5E20"
        strokeWidth="2"
        strokeLinecap="round"
      />
      <line
        x1={cx}
        y1={cy}
        x2={minuteHand.x}
        y2={minuteHand.y}
        stroke="#388E3C"
        strokeWidth="1.5"
        strokeLinecap="round"
      />
      <line
        x1={cx}
        y1={cy}
        x2={secondHand.x}
        y2={secondHand.y}
        stroke="#4CAF50"
        strokeWidth="0.8"
        strokeLinecap="round"
      />
      <circle cx={cx} cy={cy} r="2" fill="#1B5E20" />
    </svg>
  );
}

export default AnalogClock;