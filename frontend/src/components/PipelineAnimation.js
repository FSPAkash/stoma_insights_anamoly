import React, { useState, useEffect, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';

const TRUNK_COLOR = '#5D4037';
const BRANCH_COLOR = '#6D4C41';

function generateBranches() {
  const branches = [];
  branches.push({ x1: 150, y1: 280, x2: 150, y2: 100, width: 12, delay: 0 });
  branches.push({ x1: 150, y1: 150, x2: 110, y2: 90, width: 7, delay: 0.3 });
  branches.push({ x1: 150, y1: 150, x2: 190, y2: 85, width: 7, delay: 0.4 });
  branches.push({ x1: 150, y1: 190, x2: 100, y2: 130, width: 6, delay: 0.5 });
  branches.push({ x1: 150, y1: 190, x2: 200, y2: 125, width: 6, delay: 0.6 });
  branches.push({ x1: 150, y1: 120, x2: 130, y2: 65, width: 5, delay: 0.7 });
  branches.push({ x1: 150, y1: 120, x2: 170, y2: 60, width: 5, delay: 0.8 });
  branches.push({ x1: 110, y1: 90, x2: 85, y2: 60, width: 4, delay: 1.0 });
  branches.push({ x1: 190, y1: 85, x2: 215, y2: 55, width: 4, delay: 1.1 });
  return branches;
}

function generateLeafPositions() {
  return [
    { x: 140, y: 50 }, { x: 160, y: 45 }, { x: 130, y: 60 }, { x: 170, y: 55 },
    { x: 150, y: 38 }, { x: 95, y: 65 }, { x: 80, y: 58 }, { x: 105, y: 80 },
    { x: 200, y: 60 }, { x: 215, y: 52 }, { x: 205, y: 78 }, { x: 85, y: 95 },
    { x: 115, y: 70 }, { x: 185, y: 68 }, { x: 150, y: 55 }, { x: 225, y: 48 },
    { x: 120, y: 48 }, { x: 180, y: 42 },
  ];
}

const BRANCHES = generateBranches();
const LEAF_POSITIONS = generateLeafPositions();

// DEMO: This component is used for the fake data update animation
function PipelineAnimation({ status }) {
  const [visibleLeaves, setVisibleLeaves] = useState(0);
  const intervalRef = useRef(null);
  const isRunning = status === 'running';
  const isFinished = status === 'finished';

  useEffect(() => {
    if (isRunning) {
      setVisibleLeaves(0);
      let count = 0;
      intervalRef.current = setInterval(() => {
        count++;
        if (count >= LEAF_POSITIONS.length) count = 0;
        setVisibleLeaves(count + 1);
      }, 400);
    }
    if (isFinished) {
      setVisibleLeaves(LEAF_POSITIONS.length);
      if (intervalRef.current) clearInterval(intervalRef.current);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [isRunning, isFinished]);

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.3 }}
      style={{
        position: 'fixed',
        inset: 0,
        zIndex: 9999,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        background: 'rgba(0, 0, 0, 0.3)',
        backdropFilter: 'blur(8px)',
        WebkitBackdropFilter: 'blur(8px)',
      }}
    >
      <motion.div
        initial={{ opacity: 0, scale: 0.9, y: 20 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        exit={{ opacity: 0, scale: 0.9, y: 20 }}
        transition={{ duration: 0.35, ease: 'easeOut' }}
        style={{
          background: 'rgba(255, 255, 255, 0.55)',
          backdropFilter: 'blur(24px)',
          WebkitBackdropFilter: 'blur(24px)',
          borderRadius: '24px',
          border: '1px solid rgba(255, 255, 255, 0.6)',
          boxShadow: '0 8px 32px rgba(0, 0, 0, 0.08), 0 2px 8px rgba(0, 0, 0, 0.04)',
          padding: '36px 48px',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          minWidth: '360px',
        }}
      >
        <svg width="300" height="300" viewBox="0 0 300 300" style={{ overflow: 'visible' }}>
          {/* Ground */}
          <ellipse cx="150" cy="285" rx="110" ry="15" fill="#2E7D32" opacity="0.18" />

          {/* Branches */}
          {BRANCHES.map((b, i) => (
            <motion.line
              key={i}
              x1={b.x1} y1={b.y1} x2={b.x1} y2={b.y1}
              animate={{ x2: b.x2, y2: b.y2 }}
              transition={{ duration: 0.5, delay: b.delay, ease: 'easeOut' }}
              stroke={i === 0 ? TRUNK_COLOR : BRANCH_COLOR}
              strokeWidth={b.width}
              strokeLinecap="round"
            />
          ))}

          {/* Roots */}
          <motion.line x1={150} y1={280} x2={130} y2={295} stroke={TRUNK_COLOR} strokeWidth={4} strokeLinecap="round"
            initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.2 }} />
          <motion.line x1={150} y1={280} x2={170} y2={293} stroke={TRUNK_COLOR} strokeWidth={3} strokeLinecap="round"
            initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.3 }} />

          {/* Leaves */}
          <AnimatePresence>
            {LEAF_POSITIONS.slice(0, visibleLeaves).map((pos, i) => (
              <motion.image
                key={i}
                href="/stoma.png"
                x={pos.x - 16}
                y={pos.y - 16}
                width={32}
                height={32}
                initial={{ opacity: 0, scale: 0 }}
                animate={{ opacity: 0.92, scale: 1, rotate: (i % 3 - 1) * 15 }}
                exit={{ opacity: 0, scale: 0 }}
                transition={{ duration: 0.4, ease: 'backOut' }}
              />
            ))}
          </AnimatePresence>
        </svg>

        <motion.div
          style={{ marginTop: '12px', textAlign: 'center' }}
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
        >
          {isRunning && (
            <div style={{ fontSize: '15px', color: '#2E7D32', fontWeight: 500 }}>
              Updating data
              <motion.span
                animate={{ opacity: [1, 0.3, 1] }}
                transition={{ duration: 1.5, repeat: Infinity }}
              >
                ...
              </motion.span>
            </div>
          )}
          {isFinished && (
            <motion.div
              initial={{ opacity: 0, y: 5 }}
              animate={{ opacity: 1, y: 0 }}
              style={{ fontSize: '15px', color: '#1B5E20', fontWeight: 600 }}
            >
              Data updated successfully
            </motion.div>
          )}
        </motion.div>
      </motion.div>
    </motion.div>
  );
}

export default PipelineAnimation;
