import React from 'react';
import { motion } from 'framer-motion';

const GlassCard = ({
  children,
  style = {},
  className = '',
  hover = true,
  delay = 0,
  onClick,
  padding = '24px',
  intensity = 'normal',
}) => {
  const intensityMap = {
    light: {
      background: 'rgba(255, 255, 255, 0.55)',
      blur: '16px',
      border: 'rgba(203, 230, 200, 0.3)',
      shadow: '0 2px 8px rgba(27,94,32,0.04)',
    },
    normal: {
      background: 'rgba(255, 255, 255, 0.68)',
      blur: '24px',
      border: 'rgba(203, 230, 200, 0.45)',
      shadow: '0 4px 16px rgba(27,94,32,0.06), 0 2px 6px rgba(27,94,32,0.03)',
    },
    strong: {
      background: 'rgba(255, 255, 255, 0.82)',
      blur: '32px',
      border: 'rgba(168, 220, 168, 0.5)',
      shadow: '0 8px 24px rgba(27,94,32,0.08), 0 2px 8px rgba(27,94,32,0.04)',
    },
  };

  const glass = intensityMap[intensity] || intensityMap.normal;

  const baseStyle = {
    background: glass.background,
    backdropFilter: `blur(${glass.blur})`,
    WebkitBackdropFilter: `blur(${glass.blur})`,
    borderRadius: '18px',
    border: `1px solid ${glass.border}`,
    padding: padding,
    boxShadow: glass.shadow,
    cursor: onClick ? 'pointer' : 'default',
    position: 'relative',
    ...style,
  };

  return (
    <motion.div
      style={baseStyle}
      className={className}
      onClick={onClick}
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{
        duration: 0.45,
        delay: delay,
        ease: [0.4, 0, 0.2, 1],
      }}
      whileHover={
        hover && onClick
          ? {
              boxShadow: '0 12px 36px rgba(27,94,32,0.12), 0 4px 12px rgba(27,94,32,0.06)',
              borderColor: 'rgba(129, 199, 132, 0.6)',
              y: -3,
              background: 'rgba(255, 255, 255, 0.78)',
            }
          : hover
          ? {
              boxShadow: '0 8px 24px rgba(27,94,32,0.08)',
            }
          : {}
      }
    >
      {children}
    </motion.div>
  );
};

export default GlassCard;