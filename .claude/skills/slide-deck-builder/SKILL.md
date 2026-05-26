---
name: slide-deck-builder
description: >
  Build a polished, interactive HTML slide deck presentation from any content.
  Use this skill whenever the user wants to create a presentation, slide deck,
  overview deck, roadmap, pitch, or any multi-slide visual document — even if
  they say "slides", "deck", "overview", "trình bày", or "bài thuyết trình".
  Also use when asked to update, add slides to, or restyle an existing deck.
  The output is a single self-contained HTML file with keyboard navigation,
  dark theme, and reusable component patterns.
---

# Slide Deck Builder

Produces a self-contained HTML slide deck: dark background, consistent design
system, keyboard + button navigation. One HTML file, no dependencies.

---

## Quick Start

1. Read the content / context the user provides
2. Plan slide structure (aim for 8–20 slides; see Slide Types below)
3. Write the full HTML using the Design System in this file
4. Save to `/mnt/user-data/outputs/<name>.html` and call `present_files`

---

## Design System

### Base HTML shell

```html
<!DOCTYPE html>
<html lang="...">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DECK TITLE</title>
<style>
/* === PASTE FULL CSS BLOCK HERE (see CSS section below) === */
</style>
</head>
<body>
<div class="deck">
  <!-- slides go here -->
</div>
<div id="counter">1 / N</div>
<button id="prev">←</button>
<button id="next">→</button>
<script>
  const slides = document.querySelectorAll('.slide');
  let cur = 0;
  function show(n) {
    slides[cur].classList.remove('active');
    cur = (n + slides.length) % slides.length;
    slides[cur].classList.add('active');
    document.getElementById('counter').textContent = (cur+1)+' / '+slides.length;
  }
  document.getElementById('next').addEventListener('click', () => show(cur+1));
  document.getElementById('prev').addEventListener('click', () => show(cur-1));
  document.addEventListener('keydown', e => {
    if (e.key==='ArrowRight'||e.key==='ArrowDown') show(cur+1);
    if (e.key==='ArrowLeft' ||e.key==='ArrowUp')   show(cur-1);
  });
</script>
</body>
</html>
```

Replace `N` in counter with the actual slide count.

---

### Full CSS Block

Copy this verbatim into `<style>`:

```css
* { margin:0; padding:0; box-sizing:border-box; }
body { background:#0a0a0a; color:#fff;
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
  overflow:hidden; }
.deck { width:100vw; height:100vh; position:relative; }
.slide { position:absolute; inset:0; display:none; flex-direction:column;
  justify-content:center; align-items:center; padding:60px; text-align:center;
  opacity:0; transition:opacity 0.4s ease; }
.slide.active { display:flex; opacity:1; }
.slide-inner { max-width:960px; width:100%; }

/* typography */
h1  { font-size:64px; font-weight:800; line-height:1.1; }
h2  { font-size:48px; font-weight:700; line-height:1.2; margin-bottom:8px; }
p   { font-size:22px; color:#a0a0a0; line-height:1.6; margin-top:16px; }
.accent  { color:#3B82F6; }
.label   { font-size:14px; letter-spacing:2px; text-transform:uppercase;
           color:#3B82F6; font-weight:600; margin-bottom:16px; }
.sub     { font-size:26px; color:#a0a0a0; margin-top:16px; }
.divider { width:60px; height:3px; background:#3B82F6;
           border-radius:2px; margin:16px auto; }

/* navigation */
#counter { position:fixed; bottom:24px; right:32px;
           font-size:13px; color:#555; letter-spacing:1px; z-index:100; }
#prev, #next { position:fixed; bottom:20px; background:#1a1a1a;
  border:1px solid #2a2a2a; color:#a0a0a0; cursor:pointer;
  padding:8px 18px; border-radius:8px; font-size:14px; z-index:100;
  transition:all 0.2s; }
#prev { left:32px; } #next { left:90px; }
#prev:hover, #next:hover { background:#2a2a2a; color:#fff; }

/* === COMPONENTS (include all; unused ones cost nothing) === */

/* two-col */
.two-col { display:grid; grid-template-columns:1fr 1fr; gap:24px;
           margin-top:36px; text-align:left; }
.col-box { background:#1a1a1a; border:1px solid #2a2a2a;
           border-radius:16px; padding:28px; }
.col-box h4 { font-size:18px; font-weight:700; margin-bottom:14px; }
.row-item { display:flex; gap:10px; align-items:flex-start;
            margin-bottom:12px; font-size:15px; color:#ccc; line-height:1.4; }
.row-item .dot { width:8px; height:8px; border-radius:50%;
                 background:#3B82F6; flex-shrink:0; margin-top:5px; }

/* card grid */
.card-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr));
             gap:16px; margin-top:36px; }
.card { background:#1a1a1a; border:1px solid #2a2a2a;
        border-radius:16px; padding:28px 20px; text-align:center; }
.card .icon  { font-size:36px; margin-bottom:12px; }
.card .ctitle { font-size:18px; font-weight:600; margin-bottom:6px; }
.card .cdesc  { font-size:14px; color:#777; line-height:1.5; }

/* step flow */
.steps { display:flex; align-items:flex-start; justify-content:center;
         gap:0; margin-top:40px; flex-wrap:nowrap; }
.step  { display:flex; flex-direction:column; align-items:center;
         text-align:center; flex:1; min-width:0; }
.step-num   { width:44px; height:44px; border-radius:50%; background:#3B82F6;
              color:#fff; display:flex; align-items:center; justify-content:center;
              font-weight:700; font-size:18px; flex-shrink:0; }
.step-label { font-size:15px; font-weight:600; margin-top:10px; line-height:1.3; }
.step-sub   { font-size:12px; color:#666; margin-top:4px; line-height:1.4; padding:0 4px; }
.arrow      { font-size:24px; color:#2a2a2a; padding:0 6px;
              margin-top:10px; flex-shrink:0; }

/* table */
.tc-table   { width:100%; border-collapse:collapse; margin-top:28px;
              font-size:15px; text-align:left; }
.tc-table th { background:#1a1a1a; color:#3B82F6; padding:10px 14px;
               font-size:13px; letter-spacing:1px; text-transform:uppercase;
               border-bottom:1px solid #2a2a2a; }
.tc-table td { padding:10px 14px; border-bottom:1px solid #1a1a1a;
               color:#ccc; vertical-align:middle; }
.tc-table tr:hover td { background:#131313; }
.badge       { display:inline-block; padding:3px 10px; border-radius:20px;
               font-size:12px; font-weight:600; }

/* rule grid (2-col info cards) */
.rule-grid { display:grid; grid-template-columns:repeat(2,1fr);
             gap:12px; margin-top:28px; text-align:left; }
.rule-card  { background:#1a1a1a; border:1px solid #2a2a2a;
              border-radius:12px; padding:16px 18px; }
.rule-card .rtitle { font-size:15px; font-weight:700; color:#3B82F6; margin-bottom:6px; }
.rule-card .rtext  { font-size:13px; color:#888; line-height:1.5; }
.rule-card code    { background:#0d1117; color:#93c5fd; padding:2px 6px;
                     border-radius:4px; font-size:12px; font-family:monospace; }

/* artifact flow */
.artifact-row { display:flex; gap:14px; margin-top:36px; align-items:stretch; }
.artifact-box { flex:1; background:#1a1a1a; border:1px solid #2a2a2a;
                border-radius:14px; padding:20px 16px; text-align:center; }
.artifact-box .aicon  { font-size:28px; margin-bottom:8px; }
.artifact-box .atitle { font-size:14px; font-weight:700; margin-bottom:4px; }
.artifact-box .afile  { font-size:11px; color:#666; font-family:monospace; }
.artifact-type { font-size:11px; letter-spacing:1px; text-transform:uppercase;
                 font-weight:600; margin-bottom:6px; }
.art-arrow { display:flex; align-items:center; color:#333;
             font-size:20px; flex-shrink:0; }

/* error / severity badges */
.err-row   { display:flex; gap:12px; margin-top:12px; align-items:center; }
.err-badge { padding:4px 12px; border-radius:20px; font-size:13px;
             font-weight:700; white-space:nowrap; }
.err-text  { font-size:15px; color:#aaa; }

/* section banner (for milestone / phase labels) */
.section-banner { display:inline-block; background:#1e3a5f;
  border:1px solid #3B82F6; border-radius:20px; padding:4px 18px;
  font-size:13px; font-weight:700; color:#60a5fa;
  letter-spacing:1px; text-transform:uppercase; margin-bottom:20px; }

/* formula highlight box */
.formula { background:#111827; border:1px solid #1e3a5f;
           border-radius:16px; padding:32px 40px; margin:32px auto; max-width:700px; }
.formula .big { font-size:52px; font-weight:800; color:#3B82F6; }
.formula .eq  { font-size:24px; color:#a0a0a0; margin-top:8px; }

/* code block */
.code-block { background:#0d1117; border:1px solid #1e3a5f; border-radius:10px;
  padding:16px 20px; font-family:monospace; font-size:13px;
  line-height:1.8; text-align:left; margin-top:12px; }
```

---

## Colour Palette

| Token | Hex | Use |
|---|---|---|
| `bg` | `#0a0a0a` | Page background |
| `surface` | `#1a1a1a` | Cards, col-boxes |
| `border` | `#2a2a2a` | Default borders |
| `blue` | `#3B82F6` | Primary accent, labels, dots |
| `blue-light` | `#60a5fa` | Gate badges, hover text |
| `blue-dim` | `#1e3a5f` | Badge bg, formula border |
| `green` | `#4ade80` | Success, positive |
| `green-dim` | `#1a2e1a` | Success surface |
| `red` | `#f87171` | Error, danger |
| `red-dim` | `#2a1a1a` | Error surface |
| `amber` | `#f59e0b` | Warning accent |
| `yellow` | `#facc15` | Warning badge |
| `purple` | `#8B5CF6` | Milestone 2 / secondary phase |
| `purple-dim` | `#4c1d95` | Purple surface border |
| `code-blue` | `#93c5fd` | Inline code colour |
| `muted` | `#a0a0a0` | Body text |
| `dim` | `#666` | Subtitles, step-sub |

For **multi-phase decks**, assign one accent colour per phase and add a
modifier class (e.g. `.m2`) that overrides `.label`, `.accent`, `.step-num`,
and `.dot`:

```css
.m2 .label    { color:#8B5CF6; }
.m2 .accent   { color:#8B5CF6; }
.m2 .step-num { background:#8B5CF6; }
.m2 .dot      { background:#8B5CF6 !important; }
.m2 .divider  { background:#8B5CF6; }
```

---

## Slide Types

Use these patterns. Mix and match per content need.

### TITLE — opening slide
```html
<div class="slide active">
  <div class="slide-inner">
    <div class="label">SECTION LABEL</div>
    <h1>Main Headline<br><span class="accent">Accent Line</span></h1>
    <div class="divider"></div>
    <p class="sub">Subtitle or date</p>
  </div>
</div>
```

### SECTION DIVIDER — between major phases
```html
<div class="slide m2">  <!-- add phase class if applicable -->
  <div class="slide-inner">
    <div class="section-banner">🚀 Phase Name</div>
    <h2>Phase Title — <span style="color:#8B5CF6;">Accent</span></h2>
    <!-- two-col comparison, or just a p tag -->
  </div>
</div>
```

### TWO-COLUMN — before/after, comparison, pros/cons
```html
<div class="slide">
  <div class="slide-inner">
    <div class="label">LABEL</div>
    <h2>Title</h2>
    <div class="two-col">
      <div class="col-box">
        <h4 style="color:#f87171;">❌ Before</h4>
        <div class="row-item"><div class="dot" style="background:#f87171;"></div>Point one</div>
        <div class="row-item"><div class="dot" style="background:#f87171;"></div>Point two</div>
      </div>
      <div class="col-box">
        <h4 style="color:#4ade80;">✅ After</h4>
        <div class="row-item"><div class="dot"></div>Point one</div>
        <div class="row-item"><div class="dot"></div>Point two</div>
      </div>
    </div>
  </div>
</div>
```

### STEP FLOW — pipeline / process
```html
<div class="slide">
  <div class="slide-inner">
    <div class="label">PROCESS</div>
    <h2>N-Step <span class="accent">Pipeline</span></h2>
    <div class="steps">
      <div class="step">
        <div class="step-num">1</div>
        <div class="step-label">Step Name</div>
        <div class="step-sub">Short description here</div>
      </div>
      <div class="arrow">→</div>
      <div class="step">
        <div class="step-num">2</div>
        <div class="step-label">Step Name</div>
        <div class="step-sub">Short description here</div>
      </div>
      <!-- repeat step + arrow pairs -->
    </div>
  </div>
</div>
```
Keep steps ≤ 6 to avoid overflow.

### CARD GRID — features, modules, takeaways
```html
<div class="slide">
  <div class="slide-inner">
    <div class="label">LABEL</div>
    <h2>Title</h2>
    <div class="card-grid" style="grid-template-columns:repeat(3,1fr);">
      <div class="card">
        <div class="icon">🎯</div>
        <div class="ctitle">Card Title</div>
        <div class="cdesc">Supporting description text here.</div>
      </div>
      <!-- repeat cards -->
    </div>
  </div>
</div>
```
Use `repeat(2,1fr)` for 2–4 cards, `repeat(3,1fr)` for 5–6.

### TABLE — structured comparison, specs, TC list
```html
<div class="slide">
  <div class="slide-inner">
    <div class="label">LABEL</div>
    <h2>Title</h2>
    <table class="tc-table">
      <thead><tr><th>Col 1</th><th>Col 2</th><th>Col 3</th></tr></thead>
      <tbody>
        <tr><td>Value</td><td>Value</td><td>Value</td></tr>
      </tbody>
    </table>
  </div>
</div>
```
Keep tables ≤ 7 columns and ≤ 8 rows to fit on screen.

### RULE GRID — definitions, acceptance criteria, rules
```html
<div class="slide">
  <div class="slide-inner">
    <div class="label">LABEL</div>
    <h2>Title</h2>
    <div class="rule-grid">
      <div class="rule-card" style="border-color:#1e3a5f;">
        <div class="rtitle">AC-1 · Short title</div>
        <div class="rtext">Full requirement text. Use <code>inline code</code> for values.</div>
      </div>
      <!-- up to 4 cards per slide -->
    </div>
  </div>
</div>
```

### ARTIFACT FLOW — input → process → output
```html
<div class="slide">
  <div class="slide-inner">
    <div class="label">ARTIFACTS</div>
    <h2>Input &amp; Output</h2>
    <div class="artifact-row">
      <div class="artifact-box" style="border-color:#1e3a5f;">
        <div class="artifact-type" style="color:#60a5fa;">INPUT</div>
        <div class="aicon">📄</div>
        <div class="atitle">File Name</div>
        <div class="afile">pattern_*.ext</div>
      </div>
      <div class="art-arrow">→</div>
      <div class="artifact-box" style="border-color:#1a2e1a;">
        <div class="artifact-type" style="color:#4ade80;">INTERNAL</div>
        <div class="aicon">⚙️</div>
        <div class="atitle">Process</div>
        <div class="afile">output.yaml</div>
      </div>
      <div class="art-arrow">→</div>
      <div class="artifact-box" style="border-color:#2a1a1a;">
        <div class="artifact-type" style="color:#f87171;">DELIVERY</div>
        <div class="aicon">📋</div>
        <div class="atitle">Report</div>
        <div class="afile">report.xlsx</div>
      </div>
    </div>
  </div>
</div>
```

### ERROR / SEVERITY LIST
```html
<div class="slide">
  <div class="slide-inner">
    <div class="label">ERROR HANDLING</div>
    <h2>Severity Levels</h2>
    <div style="margin-top:28px; text-align:left; max-width:800px; margin:28px auto 0;">
      <div class="err-row">
        <span class="err-badge" style="background:#2a0a0a;color:#f87171;">❌ Fatal</span>
        <span class="err-text">Description of fatal error condition</span>
      </div>
      <div class="err-row">
        <span class="err-badge" style="background:#2a1a00;color:#fb923c;">⛔ Unit</span>
        <span class="err-text">Description of unit-level error</span>
      </div>
      <div class="err-row">
        <span class="err-badge" style="background:#1a1a00;color:#facc15;">⚠️ Warn</span>
        <span class="err-text">Description of warning</span>
      </div>
    </div>
  </div>
</div>
```

### FORMULA HIGHLIGHT — key metric or formula
```html
<div class="slide">
  <div class="slide-inner">
    <div class="label">FORMULA</div>
    <h2>Title</h2>
    <div class="formula">
      <div class="big">N + 4</div>
      <div class="eq">explanation of the formula</div>
    </div>
    <!-- optional table below -->
  </div>
</div>
```

### CLOSING / SUMMARY
```html
<div class="slide">
  <div class="slide-inner">
    <div class="label">SUMMARY</div>
    <h2><span class="accent">3 Key Points</span></h2>
    <div class="card-grid" style="grid-template-columns:repeat(3,1fr); margin-top:40px;">
      <div class="card"><div class="icon">🔑</div>
        <div class="ctitle">Point 1</div>
        <div class="cdesc">Brief explanation.</div>
      </div>
      <div class="card"><div class="icon">📐</div>
        <div class="ctitle">Point 2</div>
        <div class="cdesc">Brief explanation.</div>
      </div>
      <div class="card"><div class="icon">🛡</div>
        <div class="ctitle">Point 3</div>
        <div class="cdesc">Brief explanation.</div>
      </div>
    </div>
    <div class="divider" style="margin-top:44px;"></div>
    <p style="margin-top:16px; font-size:18px;">Footer note</p>
  </div>
</div>
```

---

## Slide Planning Guidelines

| Deck type | Recommended structure |
|---|---|
| Product / feature overview | Title → Problem → Pipeline → Key concepts (2–4 slides) → CLI/API → Summary |
| Technical spec / requirements | Title → Context → Architecture → Per-requirement slides → Summary |
| Roadmap | Title → Current state → Milestone divider → Per-milestone detail slides → Summary |
| Retrospective | Title → What we built → Metrics → Learnings → Next steps |

**Slide count:** 8 minimum, 20 maximum per deck. Split into multiple decks if content is larger.

**Content density per slide:**
- Max 2 columns of 4 bullet points each
- Max 6 steps in a flow
- Max 4 rule-cards per slide
- Max 7 columns / 8 rows in a table

**Language:** Match the user's language (Vietnamese, English, etc.) for all text content. CSS class names stay in English.

---

## Checklist before output

- [ ] `counter` div shows correct total slide count
- [ ] First slide has class `active`
- [ ] All slides have `<div class="slide-inner">` wrapper
- [ ] No slide overflows — reduce content if needed
- [ ] Multi-phase deck uses modifier class (`.m2`, etc.) consistently
- [ ] File saved to `/mnt/user-data/outputs/` and `present_files` called