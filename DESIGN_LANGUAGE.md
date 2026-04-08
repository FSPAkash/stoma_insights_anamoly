# Design Language — Anamoly Dashboard

## Overview

The Anamoly dashboard uses a **light glassmorphism** aesthetic built on a green monochromatic palette. The core visual motif is frosted glass layered over an animated aurora mesh background, creating depth without heaviness. All design decisions favor clarity and readability in a data-dense monitoring context.

---

## Color

### CSS Variables (`:root`)

All colors are defined as CSS custom properties and should always be referenced through these tokens.

```css
--green-900: #1B5E20
--green-600: #388E3C
--green-500: #4CAF50
--green-300: #81C784
--green-200: #A8DCA8
--green-100: #CBE6C8
--green-50:  #E6F4EA

--white:     #FFFFFF
--off-white: #F8FAF8

--gray-50:  #F5F7F5
--gray-100: #E8ECE8
--gray-200: #D0D5D0
--gray-300: #B0B8B0
--gray-400: #8A928A
--gray-500: #6B736B
--gray-600: #4A524A
--gray-700: #2D332D
--gray-800: #1A1F1A

--red-500:   #EF5350
--red-100:   #FFCDD2
--amber-500: #FFA726
--amber-100: #FFE0B2
```

### Severity Colors

| Level  | Background | Text    | Border  |
|--------|-----------|---------|---------|
| HIGH   | #FFCDD2   | #C62828 | #EF9A9A |
| MEDIUM | #FFE0B2   | #E65100 | #FFCC80 |
| NORMAL | #E6F4EA   | #1B5E20 | #A8DCA8 |

### Subsystem Classification Colors

| Subsystem   | Color   |
|-------------|---------|
| MECH        | #1B5E20 |
| ELEC        | #388E3C |
| THERM       | #FFA726 |
| PROCESS     | #7B1FA2 |
| INSTRUMENT  | #1565C0 |
| SYS_1       | #1B5E20 |
| SYS_2       | #0D47A1 |
| SYS_3       | #E65100 |
| SYS_4       | #4A148C |

---

## Typography

**Font family**: `'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`  
Referenced via `--font-sans`.

| Role               | Size  | Weight | Notes                              |
|--------------------|-------|--------|------------------------------------|
| Brand / Logo       | 21px  | 600    | letter-spacing: -0.02em            |
| Modal Title        | 22px  | 600    |                                    |
| Stat Value         | 28px  | 600    | tabular-nums                       |
| Card Title         | 14–22px | 600–700 |                                 |
| Page Title         | 16px  | 500    |                                    |
| Body Text          | 12–14px | 400–500 |                                |
| Section Heading    | 12px  | 500    | uppercase, letter-spacing: 0.05em  |
| Sublabel / Meta    | 10–11px | 500–600 | uppercase, letter-spacing: 0.04–0.06em |
| Small / Fine Print | 9–10px | 400   |                                    |

Numbers in data displays use `font-variant-numeric: tabular-nums` for column alignment.

---

## Glassmorphism

The app uses three tiers of glass intensity depending on the surface's visual weight.

### Light (Modals, Overlays)
```css
background: rgba(255, 255, 255, 0.55);
backdrop-filter: blur(16px);
border: 1px solid rgba(203, 230, 200, 0.30);
box-shadow: 0 2px 8px rgba(27, 94, 32, 0.04);
border-radius: 18px;
```

### Normal (Standard Cards)
```css
background: rgba(255, 255, 255, 0.68);
backdrop-filter: blur(24px);
border: 1px solid rgba(203, 230, 200, 0.45);
box-shadow: 0 4px 16px rgba(27, 94, 32, 0.06), 0 2px 6px rgba(27, 94, 32, 0.03);
border-radius: 18px;
padding: 20–24px;
```

### Strong (Primary Content Cards)
```css
background: rgba(255, 255, 255, 0.82);
backdrop-filter: blur(32px);
border: 1px solid rgba(168, 220, 168, 0.50);
box-shadow: 0 8px 24px rgba(27, 94, 32, 0.08), 0 2px 8px rgba(27, 94, 32, 0.04);
border-radius: 18px;
```

All shadows are tinted with `rgba(27, 94, 32, ...)` (green-900) rather than neutral black, reinforcing the palette.

---

## Components

### Buttons

**Primary**
```css
background: linear-gradient(135deg, #1B5E20 0%, #388E3C 100%);
color: #FFFFFF;
border-radius: 10px;
padding: 9px;
font-size: 13px;
font-weight: 600;
transition: all 0.2s ease;
```

**Secondary / Ghost**
```css
background: transparent;
border: 1.5px solid #CBE6C8;
border-radius: 8px;
padding: 6px 14px;
font-size: 13px;
font-weight: 500;
color: #4A524A;
/* hover: border #EF5350, color #C62828 */
```

### Input Fields
```css
padding: 9px 12px;
border: 1.5px solid rgba(255, 255, 255, 0.60);
border-radius: 10px;
background: rgba(255, 255, 255, 0.50);
backdrop-filter: blur(12px);
font-size: 13px;
transition: all 0.25s ease;
```

### Filter Pills

**Inactive**
```css
border: 1.5px solid rgba(203, 230, 200, 0.60);
background: rgba(255, 255, 255, 0.60);
color: #6B736B;
padding: 5px 14px;
border-radius: 20px;
```

**Active**
```css
border: 1.5px solid #388E3C;
background: rgba(76, 175, 80, 0.12);
color: #1B5E20;
transition: all 0.2s ease;
```

### Severity Badges
```css
padding: 3px 10px; /* or 4px 12px */
border-radius: 20px;
font-size: 12px;
font-weight: 600;
display: inline-block;
/* colors per severity table above */
```

Alert cards carry a **4px colored left border** matching the severity color.

### Tooltip Trigger
```css
width: 18px; height: 18px;
border-radius: 50%;
background: rgba(203, 230, 200, 0.50);
border: 1px solid rgba(129, 199, 132, 0.40);
color: #388E3C;
font-size: 10px; font-weight: 600;
/* hover: scale(1.1) */
```

### Tooltip Content
```css
background: rgba(255, 255, 255, 0.96);
backdrop-filter: blur(20px);
border: 1px solid rgba(203, 230, 200, 0.60);
border-radius: 12px;
padding: 12px 16px;
box-shadow: 0 12px 40px rgba(27, 94, 32, 0.15), 0 4px 12px rgba(27, 94, 32, 0.08);
font-size: 12px;
line-height: 1.55;
min-width: 240px; max-width: 340px;
```

### Avatar
```css
width: 36px; height: 36px;
border-radius: 50%;
background: linear-gradient(135deg, #1B5E20, #4CAF50);
color: #FFFFFF;
font-size: 15px; font-weight: 600;
```

---

## Layout

### Top Bar
```css
height: 72px;
padding: 0 36px;
position: sticky; top: 0;
border-bottom: 1px solid rgba(203, 230, 200, 0.35);
z-index: 100;
```

### Spacing Scale

| Token | Value |
|-------|-------|
| xs    | 6px   |
| sm    | 8–10px |
| md    | 12–16px |
| lg    | 24px  |
| xl    | 36px  |

### Grid

Cards use responsive auto-fill grids:
```css
grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); /* small cards */
grid-template-columns: repeat(auto-fill, minmax(340px, 1fr)); /* large cards */
gap: 12–16px;
```

Modal max-width: `920px`, max-height: `90vh`.

---

## Effects & Animation

### Shadow Scale

| Name        | Value |
|-------------|-------|
| Subtle      | `0 2px 8px rgba(27,94,32,0.04)` |
| Light       | `0 4px 16px rgba(27,94,32,0.06)` |
| Medium      | `0 8px 24px rgba(27,94,32,0.08)` |
| Heavy       | `0 24px 80px rgba(27,94,32,0.12)` |
| Modal       | `0 32px 100px rgba(27,94,32,0.20)` |
| Interactive | `0 12px 36px rgba(27,94,32,0.12)` |
| FAB         | `0 6px 24px rgba(27,94,32,0.30)` |

### CSS Keyframes

| Name               | Description |
|--------------------|-------------|
| `fadeInUp`         | opacity 0→1, translateY(20px→0) |
| `fadeIn`           | opacity 0→1 |
| `scaleIn`          | opacity 0→1, scale(0.95→1) |
| `spin`             | 360deg rotation |
| `meshDrift1–5`     | Complex blob drift: translate + rotate + scale, 38–58s |
| `meshBreathe1–3`   | Opacity pulse on blobs, 9–16s |
| `subtleGradientShift` | Top bar background position, 12s |
| `systemCardPulse`  | Card pulse, 3s ease-in-out infinite |

### Framer Motion

**Enter animations** (cards, modals, lists):
```js
initial: { opacity: 0, y: 16, scale: 0.97 }
animate: { opacity: 1, y: 0,  scale: 1 }
transition: { duration: 0.35–0.5, ease: [0.4, 0, 0.2, 1] }
```

Staggered children use `0.03s` delay increments.

**Hover**:
```js
whileHover: { y: -3, boxShadow: '<enhanced shadow>' }
```

**Tap**:
```js
whileTap: { scale: 0.97 }
```

### Transition Defaults

```css
transition: all 0.2s ease;          /* standard */
transition: all 0.25–0.35s ease;    /* extended */
```

---

## Aurora Mesh Background

Ten animated blob layers create a living, organic background.

**Large blobs (mesh-1 through mesh-6)**:
- Size: 40–55vw, capped at 600–700px
- Blur: 80–95px
- Colors: green stops from `--green-50` through `--green-300`
- Opacity: 0.25–0.55
- Animation: unique `meshDrift` + `meshBreathe`, 38–58s

**Accent wisps (mesh-7 through mesh-10)**:
- Size: 22–30vw, capped at 300–400px
- Blur: 60–75px
- Colors: RGBA with transparency
- Opacity: 0.12–0.25

All blobs use `will-change: transform, opacity` for GPU compositing.

---

## Scrollbar

```css
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: #D0D5D0; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #B0B8B0; }
```

---

## Mode

Light mode only. No dark mode. The palette is calibrated for white/off-white backgrounds exclusively.

---

## Key Files

| File | Purpose |
|------|---------|
| [frontend/src/theme.js](frontend/src/theme.js) | Design token definitions |
| [frontend/src/index.css](frontend/src/index.css) | Global CSS, variables, keyframes |
| [frontend/src/components/](frontend/src/components/) | Component-level inline styles |
