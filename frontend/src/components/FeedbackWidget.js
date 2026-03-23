import React, { useState, useCallback } from 'react';
import ReactDOM from 'react-dom';
import { motion, AnimatePresence } from 'framer-motion';
import { submitFeedback } from '../utils/api';

const SECTIONS = [
  { id: 'summary_cards', label: 'Summary Cards', desc: 'Alerts, sensors, SQS, uptime, classification' },
  { id: 'risk_gauges', label: 'Risk Gauges', desc: 'Current, average, peak risk and SQS dials' },
  { id: 'subsystem_gauges', label: 'Subsystem Scores', desc: 'Mechanical, electrical, thermal, physics gauges' },
  { id: 'normal_behavior', label: 'Normal Behavior Panel', desc: 'Normal period statistics' },
  { id: 'risk_timeline', label: 'Risk Timeline Chart', desc: 'Time series chart with line toggle' },
  { id: 'scores_overview', label: 'Scores Overview', desc: 'Score statistics table' },
  { id: 'alert_episodes_list', label: 'Alert Episode List', desc: 'Episode cards, sorting, expand/collapse' },
  { id: 'alert_episode_detail', label: 'Alert Episode Detail', desc: 'Modal with gauges, metadata, sensor table' },
  { id: 'sensor_decomposition', label: 'Sensor Decomposition', desc: 'Risk breakdown by sensor within an episode' },
  { id: 'sensor_detail', label: 'Sensor Detail Modal', desc: 'Individual sensor drill-down view' },
  { id: 'detected_systems', label: 'Detected Systems', desc: 'Auto-discovered system tags in header' },
  { id: 'subsystem_behavior_chart', label: 'Subsystem Behavior Chart', desc: 'Raw sensor traces per system with alert/downtime bands' },
  { id: 'time_filter', label: 'Time Filter', desc: 'Floating time filter widget and date/hour controls' },
  { id: 'overall', label: 'Overall Dashboard', desc: 'General look, feel, and usability' },
];

const RATINGS = [
  { value: 1, label: 'Poor' },
  { value: 2, label: 'Fair' },
  { value: 3, label: 'Good' },
  { value: 4, label: 'Great' },
  { value: 5, label: 'Excellent' },
];

const st = {
  fab: {
    position: 'fixed',
    bottom: '28px',
    right: '28px',
    padding: '0 18px',
    height: '40px',
    borderRadius: '20px',
    background: 'rgba(27,94,32,0.75)',
    color: '#fff',
    border: 'none',
    fontSize: '13px',
    fontWeight: 600,
    letterSpacing: '0.02em',
    cursor: 'pointer',
    boxShadow: '0 6px 24px rgba(27,94,32,0.3)',
    zIndex: 10000,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
  },
  panel: {
    position: 'fixed',
    bottom: '88px',
    right: '28px',
    width: '380px',
    maxHeight: '70vh',
    background: 'rgba(129,199,132,0.05)',
    backdropFilter: 'blur(20px)',
    WebkitBackdropFilter: 'blur(20px)',
    border: '1px solid rgba(203,230,200,0.6)',
    borderRadius: '16px',
    boxShadow: '0 16px 48px rgba(27,94,32,0.15), 0 4px 12px rgba(0,0,0,0.06)',
    zIndex: 10000,
    display: 'flex',
    flexDirection: 'column',
    overflow: 'hidden',
  },
  header: {
    padding: '16px 20px 12px',
    borderBottom: '1px solid rgba(203,230,200,0.4)',
    fontSize: '14px',
    fontWeight: 600,
    color: '#1B5E20',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
  },
  body: {
    padding: '16px 20px',
    overflowY: 'auto',
    flex: 1,
  },
  sectionBtn: (active) => ({
    display: 'block',
    width: '100%',
    textAlign: 'left',
    padding: '10px 14px',
    marginBottom: '6px',
    borderRadius: '10px',
    border: active ? '1px solid rgba(129,199,132,0.5)' : '1px solid rgba(203,230,200,0.3)',
    background: active ? 'rgba(230,244,234,0.6)' : 'rgba(255,255,255,0.5)',
    cursor: 'pointer',
    transition: 'all 0.15s ease',
    fontSize: '13px',
    fontWeight: 500,
    color: active ? '#1B5E20' : '#4A524A',
  }),
  sectionDesc: {
    fontSize: '11px',
    color: '#8A928A',
    marginTop: '2px',
    fontWeight: 400,
  },
  ratingRow: {
    display: 'flex',
    gap: '6px',
    marginTop: '12px',
    marginBottom: '12px',
  },
  ratingBtn: (active) => ({
    flex: 1,
    padding: '8px 4px',
    borderRadius: '8px',
    border: active ? '1px solid #4CAF50' : '1px solid rgba(203,230,200,0.4)',
    background: active ? 'rgba(76,175,80,0.12)' : 'rgba(255,255,255,0.6)',
    cursor: 'pointer',
    fontSize: '11px',
    fontWeight: active ? 600 : 400,
    color: active ? '#1B5E20' : '#6B736B',
    textAlign: 'center',
    transition: 'all 0.15s ease',
  }),
  textarea: {
    width: '100%',
    minHeight: '80px',
    padding: '10px 12px',
    borderRadius: '10px',
    border: '1px solid rgba(203,230,200,0.5)',
    background: 'rgba(255,255,255,0.8)',
    fontSize: '12px',
    color: '#2D332D',
    resize: 'vertical',
    outline: 'none',
    fontFamily: 'inherit',
    transition: 'border-color 0.2s',
    boxSizing: 'border-box',
  },
  submitBtn: (disabled) => ({
    width: '100%',
    padding: '10px',
    marginTop: '12px',
    borderRadius: '10px',
    border: 'none',
    background: disabled ? '#B0B8B0' : '#1B5E20',
    color: '#fff',
    fontSize: '13px',
    fontWeight: 600,
    cursor: disabled ? 'default' : 'pointer',
    transition: 'all 0.2s',
    opacity: disabled ? 0.6 : 1,
  }),
  successMsg: {
    textAlign: 'center',
    padding: '24px 16px',
    color: '#1B5E20',
    fontSize: '13px',
    fontWeight: 500,
  },
  backBtn: {
    background: 'none',
    border: 'none',
    color: '#6B736B',
    fontSize: '12px',
    cursor: 'pointer',
    padding: '4px 8px',
    borderRadius: '6px',
  },
};

function FeedbackWidget({ user, sections = SECTIONS, title = 'Dashboard Feedback' }) {
  const [open, setOpen] = useState(false);
  const [selectedSection, setSelectedSection] = useState(null);
  const [rating, setRating] = useState(null);
  const [comment, setComment] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [submitted, setSubmitted] = useState(false);
  const [issueUrl, setIssueUrl] = useState(null);
  const [contextNote, setContextNote] = useState('');

  const reset = useCallback(() => {
    setSelectedSection(null);
    setRating(null);
    setComment('');
    setSubmitted(false);
    setIssueUrl(null);
    setContextNote('');
  }, []);

  const handleSubmit = useCallback(async () => {
    if (!selectedSection || !rating) return;
    setSubmitting(true);
    try {
      const section = sections.find((s) => s.id === selectedSection);
      const res = await submitFeedback({
        user: user || '',
        section: section?.label || selectedSection,
        rating,
        comment: comment.trim(),
        context: contextNote.trim(),
      });
      setIssueUrl(res.data?.issue_url || null);
      setSubmitted(true);
      setTimeout(() => {
        reset();
      }, 3000);
    } catch (err) {
      console.error('Feedback submit failed:', err);
    } finally {
      setSubmitting(false);
    }
  }, [selectedSection, rating, comment, contextNote, reset, sections, user]);

  const handleClose = useCallback(() => {
    setOpen(false);
    setTimeout(reset, 300);
  }, [reset]);

  const widget = (
    <>
      <motion.button
        style={st.fab}
        onClick={() => open ? handleClose() : setOpen(true)}
        whileHover={{ scale: 1.08, boxShadow: '0 8px 28px rgba(27,94,32,0.4)' }}
        whileTap={{ scale: 0.94 }}
        title="Send Feedback"
      >
        {open ? 'Close' : 'Feedback'}
      </motion.button>

      <AnimatePresence>
        {open && (
          <motion.div
            style={st.panel}
            initial={{ opacity: 0, y: 20, scale: 0.95 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 20, scale: 0.95 }}
            transition={{ duration: 0.2 }}
          >
            <div style={st.header}>
              <span>
                {selectedSection ? (
                  <>
                    <button style={st.backBtn} onClick={reset}>&larr; Back</button>
                    {sections.find((s) => s.id === selectedSection)?.label}
                  </>
                ) : (
                  title
                )}
              </span>
              <button
                style={{ ...st.backBtn, fontSize: '16px' }}
                onClick={handleClose}
              >
                &times;
              </button>
            </div>

            <div style={st.body}>
              {submitted ? (
                <motion.div
                  style={st.successMsg}
                  initial={{ opacity: 0, scale: 0.9 }}
                  animate={{ opacity: 1, scale: 1 }}
                >
                  Feedback submitted as a GitHub issue. Thank you!
                  {issueUrl && (
                    <div style={{ marginTop: '8px' }}>
                      <a
                        href={issueUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                        style={{ color: '#1B5E20', textDecoration: 'underline', fontSize: '12px' }}
                      >
                        View issue on GitHub
                      </a>
                    </div>
                  )}
                </motion.div>
              ) : !selectedSection ? (
                <>
                  <div style={{ fontSize: '12px', color: '#6B736B', marginBottom: '12px' }}>
                    Select a section to provide feedback:
                  </div>
                  {sections.map((sec) => (
                    <motion.button
                      key={sec.id}
                      style={st.sectionBtn(false)}
                      onClick={() => setSelectedSection(sec.id)}
                      whileHover={{ background: 'rgba(230,244,234,0.5)', borderColor: 'rgba(129,199,132,0.4)' }}
                    >
                      {sec.label}
                      <div style={st.sectionDesc}>{sec.desc}</div>
                    </motion.button>
                  ))}
                </>
              ) : (
                <>
                  <div style={{ fontSize: '12px', color: '#6B736B', marginBottom: '4px' }}>Rating</div>
                  <div style={st.ratingRow}>
                    {RATINGS.map((r) => (
                      <motion.button
                        key={r.value}
                        style={st.ratingBtn(rating === r.value)}
                        onClick={() => setRating(r.value)}
                        whileHover={{ borderColor: '#81C784' }}
                        whileTap={{ scale: 0.95 }}
                      >
                        <div style={{ fontSize: '16px', marginBottom: '2px' }}>{r.value}</div>
                        {r.label}
                      </motion.button>
                    ))}
                  </div>

                  <div style={{ fontSize: '12px', color: '#6B736B', marginBottom: '6px' }}>Comments</div>
                  <textarea
                    style={st.textarea}
                    value={comment}
                    onChange={(e) => setComment(e.target.value)}
                    placeholder="What works well? What could be improved?"
                    onFocus={(e) => { e.target.style.borderColor = '#81C784'; }}
                    onBlur={(e) => { e.target.style.borderColor = 'rgba(203,230,200,0.5)'; }}
                  />

                  <div style={{ fontSize: '12px', color: '#6B736B', marginBottom: '6px', marginTop: '10px' }}>
                    Context (optional)
                  </div>
                  <input
                    type="text"
                    style={{ ...st.textarea, minHeight: 'auto', height: '36px' }}
                    value={contextNote}
                    onChange={(e) => setContextNote(e.target.value)}
                    placeholder="e.g. expanded episode #3, sensor detail modal"
                  />

                  <motion.button
                    style={st.submitBtn(!rating || submitting)}
                    onClick={handleSubmit}
                    disabled={!rating || submitting}
                    whileHover={rating && !submitting ? { background: '#2E7D32' } : {}}
                    whileTap={rating && !submitting ? { scale: 0.98 } : {}}
                  >
                    {submitting ? 'Submitting...' : 'Submit Feedback'}
                  </motion.button>
                </>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );

  return ReactDOM.createPortal(widget, document.body);
}

export default FeedbackWidget;
