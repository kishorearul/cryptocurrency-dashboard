/* CRYPTO DASHBOARD frontend: WS stream + icons + smoothed sparklines + COMMAND prompt */
const ICONS = {
  BTC: 'https://assets.coingecko.com/coins/images/1/large/bitcoin.png?1696501400',
  ETH: 'https://assets.coingecko.com/coins/images/279/large/ethereum.png?1696501628',
  SOL: 'https://assets.coingecko.com/coins/images/4128/large/solana.png?1696504756',
  ADA: 'https://assets.coingecko.com/coins/images/975/large/cardano.png?1696502090',
  MATIC: 'https://assets.coingecko.com/coins/images/4713/large/matic-token-icon.png?1696501449',
  XRP: 'https://assets.coingecko.com/coins/images/44/large/xrp-symbol-white-128.png?1696501442',
};
// warm icon cache early so table + tape never flash empty
Object.values(ICONS).forEach((src) => { const im = new Image(); im.src = src; });

const state = {
  assets: new Map(),
  rows: new Map(),
  hist: new Map(),
  ws: null,
  wsOk: false,
  paused: false,
  filter: '',
  sortKey: 'mcap',
  sortDir: -1,
  selected: 'BTC',
  msgCount: 0,
  pollTimer: null,
};

const $ = (id) => document.getElementById(id);
const tbody = () => document.querySelector('#cryptoTable tbody');

/* ---------- utils ---------- */
function fmtPrice(v) {
  if (v == null || isNaN(v)) return '-';
  if (v < 1) return Number(v).toFixed(4);
  return new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(v);
}
function fmtMcap(v) {
  if (v == null || isNaN(v)) return '-';
  if (v >= 1e12) return (v / 1e12).toFixed(2) + 'T';
  if (v >= 1e9) return (v / 1e9).toFixed(2) + 'B';
  if (v >= 1e6) return (v / 1e6).toFixed(2) + 'M';
  return new Intl.NumberFormat().format(Math.round(v));
}
function esc(s) { return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }

/** Canonical pair display: always BASE/USDT, never a broken substring. */
function pairDisplay(c) {
  const rawPair = (c.pair || '').toString().toUpperCase().replace(/[^A-Z]/g, '');
  if (rawPair.endsWith('USDT') && rawPair.length > 4) {
    return rawPair.slice(0, -4) + '/USDT';
  }
  const sym = (c.symbol || '').toString().toUpperCase().replace(/[^A-Z]/g, '');
  if (sym) return sym + '/USDT';
  return '—/USDT';
}
function baseSym(c) {
  const p = pairDisplay(c);
  return p.split('/')[0];
}
function iconFor(sym) {
  return ICONS[(sym || '').toUpperCase()] || null;
}

function log(msg, cls = '') {
  const el = $('sysLog');
  if (!el) return;
  const div = document.createElement('div');
  if (cls) div.className = cls;
  div.textContent = `[${new Date().toISOString().substr(11, 8)}] ${msg}`;
  el.prepend(div);
  while (el.children.length > 30) el.lastChild.remove();
}
function cmdPrint(html, cls = '') {
  const out = $('cmdOutput');
  const div = document.createElement('div');
  if (cls) div.className = cls;
  div.innerHTML = html;
  out.appendChild(div);
  out.scrollTop = out.scrollHeight;
}

/* ---------- UTC clock ---------- */
setInterval(() => {
  $('utcClock').textContent = new Date().toISOString().substr(11, 8);
}, 1000);

/* ---------- WebSocket (backend intact: ws://host:8081) ---------- */
function wsUrl() {
  const host = location.hostname || 'localhost';
  return `ws://${host}:8081`;
}
function connectWS() {
  const url = wsUrl();
  $('connDetail').textContent = `WS ${url}`;
  let ws;
  try { ws = new WebSocket(url); } catch (e) { fallbackPoll(); return; }
  state.ws = ws;
  ws.onopen = () => {
    state.wsOk = true;
    const el = $('wsStatus');
    el.textContent = 'WS STREAMING';
    el.className = 'badge badge-ws ok';
    log('WS connected ' + url, 'ok');
    cmdPrint(`&gt; connected <b>${esc(url)}</b>`, 'cmd-ok');
    if (state.pollTimer) { clearInterval(state.pollTimer); state.pollTimer = null; }
  };
  ws.onmessage = (ev) => {
    state.msgCount++;
    try {
      const arr = JSON.parse(ev.data);
      if (Array.isArray(arr)) onTick(arr);
    } catch (e) { /* ignore malformed */ }
  };
  ws.onclose = () => {
    state.wsOk = false;
    const el = $('wsStatus');
    el.textContent = 'WS RECONNECTING';
    el.className = 'badge badge-ws bad';
    log('WS closed, fallback poll + retry in 3s', 'warn');
    fallbackPoll();
    setTimeout(connectWS, 3000);
  };
  ws.onerror = () => { try { ws.close(); } catch (e) {} };
}

async function fallbackPoll() {
  if (state.pollTimer) return;
  log('REST fallback poll /api/crypto every 2s', 'warn');
  const pull = async () => {
    try {
      const t0 = performance.now();
      const r = await fetch('/api/crypto');
      if (!r.ok) throw new Error(r.status);
      const arr = await r.json();
      $('latency').textContent = Math.round(performance.now() - t0);
      if (Array.isArray(arr)) onTick(arr);
    } catch (e) { log('poll failed: ' + e.message, 'err'); }
  };
  await pull();
  state.pollTimer = setInterval(pull, 2000);
}

/* ---------- tick handling ---------- */
let lastRateTs = Date.now(), lastRateCount = 0;
setInterval(() => {
  const now = Date.now();
  const rate = ((state.msgCount - lastRateCount) / ((now - lastRateTs) / 1000)).toFixed(1);
  lastRateTs = now; lastRateCount = state.msgCount;
  $('msgRate').textContent = state.wsOk ? rate : 'poll';
}, 2000);

function onTick(arr) {
  if (state.paused) return;
  const t0 = performance.now();
  for (const c of arr) {
    const sym = (c.symbol || '').toString().toUpperCase();
    if (!sym) continue;
    const prev = state.assets.get(sym);
    state.assets.set(sym, c);
    let h = state.hist.get(sym);
    if (!h) { h = Array.isArray(c.history) ? [...c.history] : []; state.hist.set(sym, h); }
    const px = c.current_price ?? c.price;
    if (px != null && (h.length === 0 || h[h.length - 1] !== px)) {
      h.push(px);
      if (h.length > 60) h.shift();
    }
    if (!state.rows.has(sym)) buildRow(sym);
    updateRow(sym, c, prev);
  }
  applySortFilter();
  updateTape();
  updateSummary();
  updateDepthBook();
  $('timestamp').textContent = new Date().toLocaleTimeString('en-GB');
  $('latency').textContent = Math.round(performance.now() - t0);
  $('feedMode').textContent = state.wsOk ? 'LIVE-WS' : 'REST';
  $('modeLabel').textContent = state.paused ? 'PAUSED' : 'STREAM';
}

function buildRow(sym) {
  const tb = tbody();
  const tr = document.createElement('tr');
  tr.dataset.sym = sym;
  tr.innerHTML = `
    <td class="asset-cell">
      <div class="asset-wrap">
        <img class="coin-icon" alt="" loading="lazy" />
        <span class="coin-fallback" style="display:none"></span>
        <div><span class="asset-name"></span><span class="asset-pair"></span></div>
      </div>
    </td>
    <td class="price-cell"></td>
    <td class="chg-cell"></td>
    <td class="hilo-cell"></td>
    <td class="mcap-cell"></td>
    <td><canvas class="spark" width="132" height="30"></canvas></td>`;
  tr.addEventListener('click', () => selectSym(sym));
  tb.appendChild(tr);
  const img = tr.querySelector('img.coin-icon');
  const fb = tr.querySelector('.coin-fallback');
  img.addEventListener('error', () => { img.style.display = 'none'; fb.style.display = 'inline-flex'; });
  state.rows.set(sym, {
    tr, img, fb,
    nameEl: tr.querySelector('.asset-name'),
    pairEl: tr.querySelector('.asset-pair'),
    priceTd: tr.children[1],
    chgTd: tr.children[2],
    hiLoTd: tr.children[3],
    mcapTd: tr.children[4],
    canvas: tr.querySelector('canvas.spark'),
  });
}

function updateRow(sym, c, prev) {
  const r = state.rows.get(sym);
  if (!r) return;
  const price = c.current_price ?? c.price ?? 0;
  const chg = c.price_change_percentage_24h ?? c.change24h ?? 0;
  const oldPrice = prev ? (prev.current_price ?? prev.price ?? price) : price;

  // [ICON] Name (BASE/USDT)
  const src = iconFor(sym);
  if (src && r.img.getAttribute('src') !== src) {
    r.fb.style.display = 'none'; r.img.style.display = '';
    r.img.src = src;
  } else if (!src) {
    r.img.style.display = 'none'; r.fb.style.display = 'inline-flex';
  }
  r.fb.textContent = sym.slice(0, 1);
  r.img.alt = sym;
  r.nameEl.textContent = c.name || sym;
  r.pairEl.textContent = `${sym} / USDT`;

  r.tr.classList.toggle('selected', sym === state.selected);

  if (price !== oldPrice) {
    r.priceTd.textContent = '$' + fmtPrice(price);
    r.priceTd.classList.remove('flash-up', 'flash-down');
    void r.priceTd.offsetWidth;
    r.priceTd.classList.add(price > oldPrice ? 'flash-up' : 'flash-down');
  } else {
    r.priceTd.textContent = '$' + fmtPrice(price);
  }
  r.chgTd.textContent = `${chg >= 0 ? '+' : ''}${Number(chg).toFixed(2)}%`;
  r.chgTd.className = 'chg-cell ' + (chg >= 0 ? 'positive' : 'negative');
  r.hiLoTd.textContent = `${fmtPrice(c.high_24h)} / ${fmtPrice(c.low_24h)}`;
  r.mcapTd.textContent = fmtMcap(c.market_cap);

  drawSparkSmooth(r.canvas, state.hist.get(sym) || [], chg >= 0);
}

/* ---------- smoothed sparklines (anti-aliased bezier + gradient) ---------- */
function drawSparkSmooth(canvas, data, up) {
  const dpr = window.devicePixelRatio || 1;
  const W = 132, H = 30;
  if (canvas.width !== W * dpr) { canvas.width = W * dpr; canvas.height = H * dpr; }
  canvas.style.width = W + 'px'; canvas.style.height = H + 'px';
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, W, H);
  if (!data || data.length < 2) return;
  const min = Math.min(...data), max = Math.max(...data);
  const rng = (max - min) || 1;
  const pts = data.map((v, i) => ({
    x: (i / (data.length - 1)) * (W - 4) + 2,
    y: H - 4 - ((v - min) / rng) * (H - 8),
  }));
  const color = up ? '#0ecb81' : '#f6465d';
  // gradient fill
  const grad = ctx.createLinearGradient(0, 0, 0, H);
  grad.addColorStop(0, up ? 'rgba(14,203,129,0.35)' : 'rgba(246,70,93,0.35)');
  grad.addColorStop(1, 'rgba(0,0,0,0)');
  ctx.beginPath();
  ctx.moveTo(pts[0].x, pts[0].y);
  for (let i = 0; i < pts.length - 1; i++) {
    const p0 = pts[Math.max(0, i - 1)], p1 = pts[i], p2 = pts[i + 1], p3 = pts[Math.min(pts.length - 1, i + 2)];
    const c1x = p1.x + (p2.x - p0.x) / 6, c1y = p1.y + (p2.y - p0.y) / 6;
    const c2x = p2.x - (p3.x - p1.x) / 6, c2y = p2.y - (p3.y - p1.y) / 6;
    ctx.bezierCurveTo(c1x, c1y, c2x, c2y, p2.x, p2.y);
  }
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.6;
  ctx.lineJoin = 'round'; ctx.lineCap = 'round';
  ctx.shadowColor = color; ctx.shadowBlur = 5;
  ctx.stroke();
  ctx.shadowBlur = 0;
  // fill under
  ctx.lineTo(pts[pts.length - 1].x, H);
  ctx.lineTo(pts[0].x, H);
  ctx.closePath();
  ctx.fillStyle = grad;
  ctx.fill();
  // end dot
  const last = pts[pts.length - 1];
  ctx.beginPath();
  ctx.arc(last.x, last.y, 2, 0, Math.PI * 2);
  ctx.fillStyle = color;
  ctx.fill();
}

/* ---------- tape (fixed pairs + icons, seamless -50% loop) ---------- */
function updateTape() {
  const coins = [...state.assets.values()];
  if (!coins.length) return;
  const item = (c) => {
    const sym = (c.symbol || '').toString().toUpperCase();
    const pair = pairDisplay(c); // guaranteed BASE/USDT
    const chg = Number(c.price_change_percentage_24h ?? c.change24h ?? 0);
    const cls = chg >= 0 ? 'up' : 'dn';
    const src = iconFor(sym);
    const img = src
      ? `<img src="${src}" alt="${esc(sym)}" loading="lazy" onerror="this.style.display='none'" />`
      : '';
    return `<span class="tape-item">${img}<span class="pair">${esc(pair)}</span><span class="px">$${esc(fmtPrice(c.current_price ?? c.price))}</span><b class="${cls}">${chg >= 0 ? '+' : ''}${chg.toFixed(2)}%</b></span>`;
  };
  const half = coins.map(item).join('');
  $('tickerTape').innerHTML = half + half; // exactly 2 copies for translateX(-50%)
}

function updateSummary() {
  const arr = [...state.assets.values()];
  if (!arr.length) return;
  const tot = arr.reduce((s, c) => s + (Number(c.market_cap) || 0), 0);
  $('totalMarketCap').textContent = '$' + fmtMcap(tot);
  $('assetCount').textContent = arr.length;
  let g = arr[0], l = arr[0];
  const ch = (c) => Number(c.price_change_percentage_24h ?? c.change24h ?? 0);
  for (const c of arr) { if (ch(c) > ch(g)) g = c; if (ch(c) < ch(l)) l = c; }
  $('topGainer').textContent = `${g.symbol.toUpperCase()}/USDT ${ch(g) >= 0 ? '+' : ''}${ch(g).toFixed(2)}%`;
  $('topLoser').textContent = `${l.symbol.toUpperCase()}/USDT ${ch(l) >= 0 ? '+' : ''}${ch(l).toFixed(2)}%`;
}

/* ---------- depth + order book (proportionally scaled) ---------- */
function updateDepthBook() {
  const sym = state.selected;
  const c = state.assets.get(sym);
  if (!c) return;
  const price = c.current_price ?? c.price ?? 100;
  const label = pairDisplay(c);
  $('depthSym').textContent = label;
  $('bookSym').textContent = label;

  const bids = [], asks = [];
  for (let i = 5; i >= 1; i--) {
    const bp = price * (1 - i * 0.0004);
    const ap = price * (1 + i * 0.0004);
    const bs = Math.abs(Math.sin(price * i + Date.now() / 9000) * 4 + Math.random() * 2 + 0.5);
    const as = Math.abs(Math.cos(price * i + Date.now() / 7000) * 4 + Math.random() * 2 + 0.5);
    bids.push({ p: bp, s: bs }); asks.push({ p: ap, s: as });
  }
  const maxV = Math.max(...bids.map((b) => b.s), ...asks.map((a) => a.s), 0.001);

  const depthEl = $('depthBars');
  depthEl.innerHTML = '';
  for (let i = 0; i < 5; i++) {
    const row = document.createElement('div');
    row.className = 'depth-row';
    const bw = (bids[i].s / maxV * 100).toFixed(1);
    const aw = (asks[i].s / maxV * 100).toFixed(1);
    row.innerHTML = `
      <div class="depth-cell left"><div class="vol-bar" style="width:${bw}%"></div><span>${bids[i].s.toFixed(2)}</span></div>
      <div class="depth-mid">${fmtPrice((bids[i].p + asks[i].p) / 2)}</div>
      <div class="depth-cell right"><div class="vol-bar" style="width:${aw}%"></div><span>${asks[i].s.toFixed(2)}</span></div>`;
    depthEl.appendChild(row);
  }
  const bidsEl = $('bids'), asksEl = $('asks');
  bidsEl.innerHTML = ''; asksEl.innerHTML = '';
  const bMax = Math.max(...bids.map((b) => b.s));
  const aMax = Math.max(...asks.map((a) => a.s));
  bids.forEach((b) => {
    const d = document.createElement('div');
    d.className = 'book-row bid';
    d.innerHTML = `<span>$${fmtPrice(b.p)}</span><span>${b.s.toFixed(2)}</span><div class="bar" style="width:${(b.s / bMax * 100).toFixed(1)}%"></div>`;
    bidsEl.appendChild(d);
  });
  asks.forEach((a) => {
    const d = document.createElement('div');
    d.className = 'book-row ask';
    d.innerHTML = `<span>$${fmtPrice(a.p)}</span><span>${a.s.toFixed(2)}</span><div class="bar" style="width:${(a.s / aMax * 100).toFixed(1)}%"></div>`;
    asksEl.appendChild(d);
  });
  $('spread').textContent = `SPREAD ${(asks[0].p - bids[0].p).toFixed(price < 10 ? 4 : 2)}`;
}

/* ---------- sort / filter ---------- */
function coinVal(c, key) {
  if (key === 'price') return c.current_price ?? c.price ?? 0;
  if (key === 'change') return c.price_change_percentage_24h ?? c.change24h ?? 0;
  if (key === 'mcap') return c.market_cap ?? 0;
  return (c.name || '');
}
function applySortFilter() {
  const tb = tbody();
  const rows = [...state.rows.values()];
  rows.sort((a, b) => {
    const ca = state.assets.get(a.tr.dataset.sym), cb = state.assets.get(b.tr.dataset.sym);
    if (!ca || !cb) return 0;
    const va = coinVal(ca, state.sortKey), vb = coinVal(cb, state.sortKey);
    if (typeof va === 'string') return va.localeCompare(vb) * state.sortDir;
    return (va - vb) * state.sortDir;
  });
  for (const r of rows) {
    const c = state.assets.get(r.tr.dataset.sym);
    const hide = state.filter && !((c.name || '').toLowerCase().includes(state.filter) || (c.symbol || '').toLowerCase().includes(state.filter) || pairDisplay(c).toLowerCase().includes(state.filter));
    r.tr.style.display = hide ? 'none' : '';
    tb.appendChild(r.tr);
  }
}

document.querySelectorAll('#cryptoTable th[data-sort]').forEach((th) => {
  th.addEventListener('click', () => {
    const raw = th.dataset.sort;
    const k = raw === 'name' ? 'name' : raw === 'price' ? 'price' : raw === 'change' ? 'change' : 'mcap';
    if (state.sortKey === k) state.sortDir *= -1;
    else { state.sortKey = k; state.sortDir = k === 'name' ? 1 : -1; }
    $('sortLabel').textContent = k === 'mcap' ? 'MKT CAP' : k.toUpperCase();
    applySortFilter();
  });
});

function selectSym(sym) {
  state.selected = sym.toUpperCase();
  for (const [, r] of state.rows) r.tr.classList.toggle('selected', r.tr.dataset.sym === state.selected);
  updateDepthBook();
  cmdPrint(`&gt; selected <b>${esc(state.selected)}/USDT</b> — depth/book now tracking`, 'cmd-ok');
}

/* ---------- COMMAND prompt ---------- */
function handleCmd(raw) {
  const line = raw.trim();
  if (!line) return;
  cmdPrint(`<span class="cmd-echo">COMMAND &gt; ${esc(line)}</span>`);
  const [cmd, ...rest] = line.split(/\s+/);
  const arg = rest.join(' ').toLowerCase();
  switch (cmd.toLowerCase()) {
    case 'help':
      cmdPrint(`cmds: <b>sort [price|change|mcap|name]</b> · <b>filter &lt;text|clear&gt;</b> · <b>select &lt;SYM&gt;</b> · <b>pause|resume</b> · <b>snapshot</b> · <b>clear</b>`);
      break;
    case 'pause': state.paused = true; $('modeLabel').textContent = 'PAUSED'; cmdPrint('stream paused', 'cmd-ok'); break;
    case 'resume': state.paused = false; $('modeLabel').textContent = 'STREAM'; cmdPrint('stream resumed', 'cmd-ok'); break;
    case 'clear': $('cmdOutput').innerHTML = ''; break;
    case 'sort': {
      const m = { price: 'price', change: 'change', mcap: 'mcap', name: 'name' }[arg];
      if (!m) { cmdPrint(`usage: sort [price|change|mcap|name]`, 'cmd-err'); break; }
      state.sortKey = m; $('sortLabel').textContent = m === 'mcap' ? 'MKT CAP' : m.toUpperCase(); applySortFilter();
      cmdPrint(`sorted by ${m}`, 'cmd-ok'); break;
    }
    case 'filter':
      if (!arg || arg === 'clear') { state.filter = ''; cmdPrint('filter cleared', 'cmd-ok'); }
      else { state.filter = arg; cmdPrint(`filter = "${esc(arg)}"`, 'cmd-ok'); }
      applySortFilter(); break;
    case 'select': {
      const s = (rest[0] || '').toUpperCase();
      if (!state.assets.has(s)) { cmdPrint(`unknown symbol ${esc(s)} — try ${[...state.assets.keys()].join(', ')}`, 'cmd-err'); break; }
      selectSym(s); break;
    }
    case 'snapshot': {
      const arr = [...state.assets.values()].map((c) => `${baseSym(c)}/USDT $${fmtPrice(c.current_price ?? c.price)}`);
      cmdPrint(arr.join(' | ')); break;
    }
    case 'ping':
      cmdPrint(`pong — ws:${state.wsOk ? 'UP' : 'DOWN'} msgs:${state.msgCount}`, 'cmd-ok'); break;
    default:
      cmdPrint(`unknown cmd "${esc(cmd)}" — type <b>help</b>`, 'cmd-err');
  }
}

$('cmdInput').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') { handleCmd(e.target.value); e.target.value = ''; }
});

/* ---------- boot ---------- */
log('dashboard boot…', 'ok');
connectWS();
cmdPrint(`CRYPTO DASHBOARD — type <b>help</b> to begin`, 'cmd-ok');
