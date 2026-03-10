import React, { useState, useRef, useCallback, useEffect } from 'react';
import ReactDOM from 'react-dom';

const TOOLTIP_STYLE = {
  position: 'fixed',
  background: 'rgba(255, 255, 255, 0.96)',
  backdropFilter: 'blur(20px)',
  WebkitBackdropFilter: 'blur(20px)',
  border: '1px solid rgba(203, 230, 200, 0.6)',
  borderRadius: '12px',
  padding: '12px 16px',
  boxShadow: '0 12px 40px rgba(27,94,32,0.15), 0 4px 12px rgba(27,94,32,0.08)',
  minWidth: '240px',
  maxWidth: '340px',
  fontSize: '12px',
  fontWeight: 400,
  color: '#2D332D',
  lineHeight: 1.55,
  zIndex: 99999,
  pointerEvents: 'none',
  textAlign: 'left',
  textTransform: 'none',
  letterSpacing: 'normal',
};

function InfoTooltip({ text }) {
  const [visible, setVisible] = useState(false);
  const [pos, setPos] = useState({ top: 0, left: 0 });
  const triggerRef = useRef(null);
  const tooltipRef = useRef(null);

  const updatePosition = useCallback(() => {
    if (!triggerRef.current) return;
    const rect = triggerRef.current.getBoundingClientRect();
    const tooltipW = 300;
    const tooltipH = 160;
    const pad = 8;

    let top = rect.top - tooltipH - pad;
    let left = rect.left + rect.width / 2 - tooltipW / 2;

    // If clipped at top, show below
    if (top < pad) {
      top = rect.bottom + pad;
    }
    // If clipped at bottom too, just pin to top
    if (top + tooltipH > window.innerHeight - pad) {
      top = pad;
    }
    // Clamp horizontal
    if (left < pad) left = pad;
    if (left + tooltipW > window.innerWidth - pad) {
      left = window.innerWidth - tooltipW - pad;
    }

    setPos({ top, left });
  }, []);

  const handleEnter = useCallback(() => {
    updatePosition();
    setVisible(true);
  }, [updatePosition]);

  const handleLeave = useCallback(() => {
    setVisible(false);
  }, []);

  useEffect(() => {
    if (!visible || !tooltipRef.current || !triggerRef.current) return;
    const rect = triggerRef.current.getBoundingClientRect();
    const tr = tooltipRef.current.getBoundingClientRect();
    const pad = 8;

    let top = rect.top - tr.height - pad;
    let left = rect.left + rect.width / 2 - tr.width / 2;

    if (top < pad) top = rect.bottom + pad;
    if (top + tr.height > window.innerHeight - pad) top = pad;
    if (left < pad) left = pad;
    if (left + tr.width > window.innerWidth - pad) left = window.innerWidth - tr.width - pad;

    setPos({ top, left });
  }, [visible]);

  return (
    <>
      <span
        ref={triggerRef}
        className="tooltip-trigger"
        onMouseEnter={handleEnter}
        onMouseLeave={handleLeave}
      >
        i
        <span className="tooltip-content">{text}</span>
      </span>
      {visible && ReactDOM.createPortal(
        <div ref={tooltipRef} style={{ ...TOOLTIP_STYLE, top: pos.top, left: pos.left }}>
          {text}
        </div>,
        document.body
      )}
    </>
  );
}

export default InfoTooltip;
