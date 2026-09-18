// ============================================================================
// AGGIE'S NEW TELEPHONE — Voice Gateway v1.17 (Twilio ConversationRelay <-> Anthropic)
// v1.22: per-turn clock in /health (caller-done → first word, interrupts, silent turns).
// v1.21: the first-turn holding line is neutral unless the pack says the caller is a known customer.
// v1.20: one booking per call - recaps never re-book; recap answers are silent.
// v1.19: acts inherit the call's extracted lead (name/address/email/service) when the act left them blank.
// v1.18: + rescheduleJob on the customer line (voice can now move a visit the caller asked to move).
// v1.17: THE CUSTOMER LINE GETS HANDS. Her JSON on a customer call may carry `act`
//        (bookJob / cancelJob / noteJob / confirmJob); it rides to GAS hook=voicebook
//        WHILE THE CALLER IS STILL ON THE LINE, and she speaks the `say` line GAS hands
//        back — never an improvised confirmation. THE LAW OF THE SAME TURN: if her words
//        promise a date, a cancel, or a note and the act is missing, the act is built from
//        her lead + the caller's last words, and a fallback line is spoken on ok:false.
//        Pairs with APS 2.0 v38.517+ (hook=voicebook) / v38.518 (the brain knows the act).
// v1.12: /health confesses the real version again (the constant had sat at 1.2 through v1.8–v1.11).
// v1.2: outbound rescues ride this road too — customer number derived from
// direction, call rows labeled correctly, and /health confesses its version.
// Apex Pest Solutions · pairs with APS 2.0 v34.11+ (hook=brainpack / hook=relay / hook=voiceact since v1.9)
//
// WHY THIS EXISTS: Google Apps Script's front door degrades under daytime load
// (verified: same code, 4 AM pass / 11 AM 502, GAS error log empty — the code
// never ran). So the LIVE CALL leaves Google entirely. Twilio streams caller
// speech here over a websocket; this service runs Aggie's brain against the
// Anthropic API and streams her words straight back into the caller's ear.
// GAS remains book of record and brain source — reached only OFF the call.
//
// DESIGN LAWS (carried over from APS): nothing fails silently (every error is
// in the ring buffer at /health), one rulebook (the brain is compiled BY GAS,
// pulled here — zero prompt drift), lossless pipeline (results POST retries,
// and the 30-min Twilio reconciliation sweep in GAS is the final backstop).
//
// FAILURE MODE BY CONSTRUCTION: the Twilio TwiML that starts the call has
// <Dial>Chris's cell</Dial> AFTER the <Connect>. If this service is down, the
// websocket never opens and the caller lands on Chris — pre-Aggie behavior,
// automatically. When Aggie finishes a call ("done"), we hang the call up via
// Twilio REST so it never falls through to that Dial.
// ============================================================================
'use strict';

const http = require('http');
const { WebSocketServer } = require('ws');

// ---- config (all via environment; render.yaml wires these) -----------------
const GW_VERSION = '1.22';
const PORT       = process.env.PORT || 10000;
const ANTHROPIC  = process.env.ANTHROPIC_API_KEY || '';
const GAS_URL    = (process.env.GAS_EXEC_URL || '').replace(/\/+$/, ''); // full /exec URL, no query
const WKEY       = process.env.WEBHOOK_KEY || '';
const RELAY_TOKEN= process.env.RELAY_TOKEN || '';          // shared secret in the wss URL (?t=...)
const TW_SID     = process.env.TWILIO_ACCOUNT_SID || '';
const TW_TOKEN   = process.env.TWILIO_AUTH_TOKEN || '';
const CHRIS_CELL = process.env.CHRIS_CELL || '';           // transfer target, E.164
const MODEL      = process.env.AGGIE_MODEL || 'claude-sonnet-4-6';
const MAX_TOKENS = Number(process.env.AGGIE_MAX_TOKENS || 500);

// ---- nothing fails silently: ring buffer surfaced at /health ---------------
const errs = [];
// v1.7: the API reports cache reads on the opening event of each turn. Keeping
// the last numbers means caching can be PROVEN on /health, not just believed.
let lastUsage = null;
// v1.17: the last few voicebook round-trips, so /health can prove her hands work.
const acts = [];
function logErr(where, e) {
  const line = new Date().toISOString() + ' ' + where + ': ' + String((e && e.message) || e);
  console.error(line);
  errs.push(line);
  while (errs.length > 30) errs.shift();
}
function logInfo(msg) { console.log(new Date().toISOString() + ' ' + msg); }

// ---- brainpack cache --------------------------------------------------------
// Generic brain refreshed every 4 minutes (mirrors GAS-side cache TTL). A
// caller-specific pack (dossier included) is raced at ring time with a hard
// timeout — if GAS is slow, the generic brain answers and the call NEVER waits.
let genericPack = null;          // { sys, greeting, vm, model, v, at }
let genericAt   = 0;

async function fetchPack(phone, timeoutMs, mid) {
  const ctl = new AbortController();
  const tm = setTimeout(() => ctl.abort(), timeoutMs);
  try {
    const u = GAS_URL + '?hook=brainpack&k=' + encodeURIComponent(WKEY) +
              (mid ? '&mid=' + encodeURIComponent(mid) : '') +
              (phone ? '&phone=' + encodeURIComponent(phone) : '');
    const r = await fetch(u, { signal: ctl.signal, redirect: 'follow' });
    const j = await r.json();
    if (j && j.ok && j.sys) return j;
    throw new Error('bad pack: ' + JSON.stringify(j).slice(0, 120));
  } finally { clearTimeout(tm); }
}
async function refreshGeneric() {
  try {
    genericPack = await fetchPack('', 25000);
    genericAt = Date.now();
    logInfo('brainpack refreshed (v' + genericPack.v + ', ' + genericPack.sys.length + ' chars)');
  } catch (e) { logErr('brainpack.refresh', e); }
}
refreshGeneric();
setInterval(refreshGeneric, 4 * 60 * 1000);

// ---- Anthropic streaming with live "reply" extraction -----------------------
// Aggie answers in strict JSON: {"reply":"...","done":...,"lead":{...}}. To get
// sub-second first-word latency we do NOT wait for the whole JSON — a tiny
// state machine watches the token stream for  "reply":"  and forwards the reply
// text to Twilio TTS character-for-character as it is generated, handling JSON
// escapes on the fly. The full raw text is kept and parsed at the end for the
// control fields (done / transfer / flagOwner / lead / sched).
function replyExtractor(emit) {
  let mode = 0;            // 0 = hunting for "reply":" · 1 = inside reply · 2 = done
  let hunt = '';
  let esc = false, uni = '';
  return function feed(chunk) {
    for (const ch of chunk) {
      if (mode === 0) {
        hunt += ch;
        if (hunt.length > 400) hunt = hunt.slice(-40);
        if (/"reply"\s*:\s*"$/.test(hunt)) mode = 1;
      } else if (mode === 1) {
        if (uni) { uni += ch; if (uni.length === 5) { emit(String.fromCharCode(parseInt(uni.slice(1), 16) || 32)); uni = ''; } continue; }
        if (esc) {
          esc = false;
          if (ch === 'n' || ch === 't') emit(' ');
          else if (ch === 'u') uni = 'u';
          else emit(ch);                       // \" \\ \/ etc
        }
        else if (ch === '\\') esc = true;
        else if (ch === '"') mode = 2;         // unescaped close quote — reply over
        else emit(ch);
      }
    }
  };
}

async function aiTurn(sys, convo, onReplyText, signal) {
  const r = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    signal,
    headers: {
      'content-type': 'application/json',
      'x-api-key': ANTHROPIC,
      'anthropic-version': '2023-06-01'
    },
    // v1.7 THE BRAIN WAS RE-SENT ON EVERY SINGLE TURN. Aggie's system prompt is
    // the whole canon, the dossier, the availability — tens of thousands of
    // tokens — and a ten-turn call paid for it ten times over. 87.7M tokens in a
    // week is what pushed the account into its spend cap mid-morning and took
    // her brain offline. Marking it cacheable means the first turn of a call
    // pays full price and every turn after reads from cache at a tenth the
    // cost. Same prompt, same behaviour, same voice — only the bill changes.
    body: JSON.stringify({
      model: MODEL, max_tokens: MAX_TOKENS,
      system: [{ type: 'text', text: sys, cache_control: { type: 'ephemeral' } }],
      messages: convo, stream: true
    })
  });
  if (!r.ok) throw new Error('anthropic ' + r.status + ': ' + (await r.text()).slice(0, 200));
  const feed = replyExtractor(onReplyText);
  let raw = '';
  const reader = r.body.getReader();
  const dec = new TextDecoder();
  let buf = '';
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    let ix;
    while ((ix = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, ix).trim(); buf = buf.slice(ix + 1);
      if (!line.startsWith('data:')) continue;
      const data = line.slice(5).trim();
      if (data === '[DONE]') continue;
      try {
        const ev = JSON.parse(data);
        try {
          const u = ev && ev.message && ev.message.usage;
          if (u) lastUsage = {
            at: new Date().toISOString(),
            input: u.input_tokens || 0,
            cacheWrite: u.cache_creation_input_tokens || 0,
            cacheRead: u.cache_read_input_tokens || 0
          };
        } catch (eU) {}
        const t = ev && ev.delta && ev.delta.text;
        if (t) { raw += t; feed(t); }
      } catch (e) { /* partial SSE line — ignored */ }
    }
  }
  return raw;
}

// Same tolerant parse GAS uses (vrParse_ spirit): full JSON first, regex rescue
// for truncated output, raw-speech fallback last.
function parseTurn(raw) {
  const t = String(raw || '').trim();
  try {
    const m = t.match(/\{[\s\S]*\}/);
    if (m) { const j = JSON.parse(m[0]); if (j && j.reply) return j; }   // v1.9: `act` (owner line) rides through untouched
  } catch (e) {}
  const m2 = /"reply"\s*:\s*"((?:[^"\\]|\\.)*)/.exec(t);
  if (m2) {
    const lead = {};
    for (const k of ['name','address','phone','email','pest','service','day','window','notes']) {
      const mm = new RegExp('"' + k + '"\\s*:\\s*"((?:[^"\\\\]|\\\\.)*)"').exec(t);
      if (mm) lead[k] = mm[1].replace(/\\"/g, '"');
    }
    return {
      reply: m2[1].replace(/\\"/g, '"').replace(/\\n/g, ' ').replace(/\\u([0-9a-fA-F]{4})/g, (_, h) => String.fromCharCode(parseInt(h, 16))).replace(/\\\\/g, '\\'),
      done: /"done"\s*:\s*true/.test(t), flagOwner: /"flagOwner"\s*:\s*true/.test(t),
      commercial: /"commercial"\s*:\s*true/.test(t), transfer: /"transfer"\s*:\s*true/.test(t),
      lead
    };
  }
  if (t && !t.startsWith('{')) return { reply: t.slice(0, 400) };
  return null;
}

// v1.15 ONE WOMAN, EVERY LINE (owner: "when a customer asks to speak with
// me her voice changes" - the transfer's plain <Say> lines fell to Polly
// while the conversation rode ElevenLabs). Fixed phrases now <Play> from the
// memory server's /say voice cache; unset MEM_SAY_BASE falls back to <Say>.
// MEM_SAY_BASE example: https://aggie-memory.onrender.com/say?key=THEKEY
const MEM_SAY_BASE = process.env.MEM_SAY_BASE || '';
function sayLine(text) {
  if (MEM_SAY_BASE) return '<Play>' + xesc(MEM_SAY_BASE + '&text=' + encodeURIComponent(text)) + '</Play>';
  return '<Say>' + xesc(text) + '</Say>';
}
// ---- Twilio REST helpers (transfer + graceful hangup, no GAS in the path) ---
async function twilioUpdateCall(callSid, twiml) {
  const u = 'https://api.twilio.com/2010-04-01/Accounts/' + TW_SID + '/Calls/' + callSid + '.json';
  const r = await fetch(u, {
    method: 'POST',
    headers: {
      'authorization': 'Basic ' + Buffer.from(TW_SID + ':' + TW_TOKEN).toString('base64'),
      'content-type': 'application/x-www-form-urlencoded'
    },
    body: 'Twiml=' + encodeURIComponent(twiml)
  });
  if (!r.ok) throw new Error('twilio update ' + r.status + ': ' + (await r.text()).slice(0, 200));
}
async function startRecording(s) {
  // v1.1: mirror of the v23.5 law — every receptionist call is recorded. The
  // recording callback rides to the SAME GAS hook (hook=rec) the old telephone
  // used, so the Calls sheet row and playback land exactly like before.
  if (s.recStarted || !s.callSid || !TW_SID) return;
  s.recStarted = true;
  try {
    const cb = GAS_URL + '?hook=rec&k=' + encodeURIComponent(WKEY);
    const u = 'https://api.twilio.com/2010-04-01/Accounts/' + TW_SID + '/Calls/' + s.callSid + '/Recordings.json';
    const r = await fetch(u, {
      method: 'POST',
      headers: {
        'authorization': 'Basic ' + Buffer.from(TW_SID + ':' + TW_TOKEN).toString('base64'),
        'content-type': 'application/x-www-form-urlencoded'
      },
      body: 'RecordingStatusCallback=' + encodeURIComponent(cb) + '&RecordingStatusCallbackEvent=completed'
    });
    if (!r.ok) throw new Error('rec ' + r.status + ': ' + (await r.text()).slice(0, 140));
    logInfo('recording started for ' + s.callSid);
  } catch (e) { logErr('recording', e); }
}
const xesc = s => String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');

// ---- results ramp: POST the finished call to GAS, with patient retries ------
async function postResults(payload) {
  const body = JSON.stringify(payload);
  const u = GAS_URL + '?hook=relay&k=' + encodeURIComponent(WKEY);
  const waits = [0, 3000, 12000, 40000];
  for (let i = 0; i < waits.length; i++) {
    if (waits[i]) await new Promise(res => setTimeout(res, waits[i]));
    try {
      const r = await fetch(u, { method: 'POST', headers: { 'content-type': 'application/json' }, body, redirect: 'follow' });
      const txt = await r.text();
      if (r.ok && txt.trim() === 'ok') { logInfo('results landed for ' + payload.sid); return; }
      throw new Error('gas said: ' + txt.slice(0, 120));
    } catch (e) { logErr('results.try' + (i + 1), e); }
  }
  logErr('results.FINAL', 'all retries failed for ' + payload.sid + ' — Twilio reconciliation sweep will backfill');
}

// v1.9: owner-line action ramp (one try, 20s — the owner is waiting on the line)
async function postVoiceAct(s, act) {
  const u = GAS_URL + '?hook=voiceact&k=' + encodeURIComponent(WKEY);
  const ctl = new AbortController(); const tm = setTimeout(() => ctl.abort(), 20000);
  try {
    const r = await fetch(u, { method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ sid: s.callSid, from: s.from, act }), redirect: 'follow', signal: ctl.signal });
    const txt = await r.text();
    try { return JSON.parse(txt); } catch (e) { return { ok: false, error: 'unreadable: ' + txt.slice(0, 80) }; }
  } finally { clearTimeout(tm); }
}

// v1.17 THE CUSTOMER LINE'S HANDS (Aggie's own #1, Sept 16 — Pedro Tito: a time
// promised on a live call with no work order behind it). One try, 8s: the caller
// is on the line. GAS answers with a `say` line either way; on timeout the
// fallback is spoken and the post-call sweep + the red row catch the rest.
const CUSTOMER_ACTS = { bookJob: 1, cancelJob: 1, noteJob: 1, confirmJob: 1, rescheduleJob: 1 };   // v1.18: + reschedule
const FALLBACK_SAY = 'I will have Chris confirm that with you shortly.';
async function postVoiceBook(s, act) {
  const u = GAS_URL + '?hook=voicebook&k=' + encodeURIComponent(WKEY);
  const ctl = new AbortController(); const tm = setTimeout(() => ctl.abort(), 8000);
  const t0 = Date.now();
  try {
    const r = await fetch(u, { method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ callSid: s.callSid, from: s.from, act }), redirect: 'follow', signal: ctl.signal });
    const txt = await r.text();
    let j; try { j = JSON.parse(txt); } catch (e) { j = { ok: false, error: 'unreadable: ' + txt.slice(0, 80), say: FALLBACK_SAY }; }
    acts.push({ at: new Date().toISOString(), sid: s.callSid, action: act.action, ok: !!j.ok, ms: Date.now() - t0, woId: j.woId || '', error: j.error || '' });
    while (acts.length > 12) acts.shift();
    return j;
  } catch (e) {
    acts.push({ at: new Date().toISOString(), sid: s.callSid, action: act.action, ok: false, ms: Date.now() - t0, error: String((e && e.message) || e).slice(0, 80) });
    while (acts.length > 12) acts.shift();
    return { ok: false, error: String((e && e.message) || e).slice(0, 120), say: FALLBACK_SAY };
  } finally { clearTimeout(tm); }
}

// v1.17 THE LAW OF THE SAME TURN (voice edition of GAS v38.512/514). If her
// spoken reply commits to a date / a cancel / a note and the JSON carried no
// act, build the act from what she already extracted (lead) plus the caller's
// last words as the agreement quote. GAS still applies the evidence law — a
// synthesized bookJob without a real "yes" in callerSaid books nothing and
// hands back the honest line.
const RX_SAID_BOOKED = /\b(you(?:'|’)?re (?:all )?set|on the books|got you (?:down|in) for|i(?:'|’)?ve got you (?:down|in)|(?:we(?:'|’)?ll|i(?:'|’)?ll) (?:be|see you) (?:there|out|then)|booked you|scheduled for|see you (?:on |then|at )?)\b/i;
const RX_SAID_CANCEL = /\b((?:it(?:'|’)?s|that(?:'|’)?s|that one(?:'|’)?s|you(?:'|’)?re) (?:all )?(?:canceled|cancelled)|(?:i(?:'|’)?ve )?(?:canceled|cancelled) (?:it|that|your|the)|taken (?:it|that|you) off (?:the|our) (?:schedule|books|calendar)|off the schedule)\b/i;
const RX_SAID_MOVED  = /\b(moved (?:you|it|that|your visit) to|you(?:'|’)?re (?:now )?(?:moved|rescheduled) (?:to|for)|rescheduled (?:you|it|that|your visit) (?:to|for)|new (?:day|date) is)\b/i;
const RX_SAID_NOTED  = /\b(i(?:'|’)?ve noted|i noted|noted (?:on|for)|on (?:the|your) work ?order|tech (?:comes|will come|will be) prepared|i(?:'|’)?ll (?:make a note|note that)|added (?:it|that) to (?:this|your|the) visit)\b/i;
function synthesizeAct(s, d) {
  const reply = String((d && d.reply) || '');
  const lastUser = (function () { for (let i = s.convo.length - 1; i >= 0; i--) { if (s.convo[i].role === 'user' && !/^\[/.test(String(s.convo[i].content || ''))) return String(s.convo[i].content || ''); } return ''; })();
  const lead = Object.assign({}, s.lead || {}, (d && d.lead) || {});
  if (RX_SAID_CANCEL.test(reply)) return { action: 'cancelJob', data: { name: lead.name || '', phone: s.from, promised: reply.slice(0, 240), callerSaid: lastUser.slice(0, 200) } };
  if (RX_SAID_MOVED.test(reply) && (lead.day || lead.window)) return { action: 'rescheduleJob', data: { name: lead.name || '', phone: s.from, day: lead.day || '', window: lead.window || '', promised: reply.slice(0, 240), callerSaid: lastUser.slice(0, 200) } };
  if (RX_SAID_NOTED.test(reply)) return { action: 'noteJob', data: { name: lead.name || '', phone: s.from, note: (reply.slice(0, 200) + (lastUser ? (' — caller said: “' + lastUser.slice(0, 120) + '”') : '')), promised: reply.slice(0, 240), callerSaid: lastUser.slice(0, 200) } };
  if (RX_SAID_BOOKED.test(reply) && (lead.day || lead.window)) return { action: 'bookJob', data: { name: lead.name || '', phone: s.from, address: lead.address || '', service: lead.service || lead.pest || '', day: lead.day || '', window: lead.window || '', price: lead.price || '', promised: reply.slice(0, 240), callerSaid: lastUser.slice(0, 200) } };
  return null;
}

// ---- per-call session -------------------------------------------------------
const sessions = new Map();   // ws -> session
let callsHandled = 0;

function newSession(ws) {
  return {
    ws, callSid: '', from: '', to: '', dir: 'in', startedAt: Date.now(),
    convo: [],                 // [{role,content}] — assistant turns store REPLY TEXT, same as GAS
    lead: {},                  // monotonic merge across turns — a fact once given is never lost
    flag: false, commercial: false, tierOffered: false, tierTaken: '',
    sched: null, done: false, finalized: false,
    packPromise: null, pack: null, callerPack: null, recStarted: false, mid: '', endWhy: '', needsCallback: false,
    ctl: null,                 // AbortController of the in-flight AI turn
    actsRan: []                // v1.17: what her hands did on this call (rides to GAS in the results)
  };
}

function mergeLead(into, from) {
  if (!from) return;
  for (const k of Object.keys(from)) {
    const v = String(from[k] || '').trim();
    if (v) into[k] = v;
  }
}

// v1.22 THE TURN CLOCK (Aggie's sticky note, Sept 17 5:25 PM: 'tangled in the echo/delay', 'the double Hello? means they
// couldn't hear me'). Twilio owns the ears and the voice; the gateway owns the gap between the caller finishing and her
// first word. Every turn records: caller-done → first token (ms), → last token (ms), tokens sent, and interrupts
// (caller talked over her). /health shows the last 30. First-token > 2000ms is the lag she feels; a turn with 0 tokens
// is a silent turn; a call full of interrupts is a caller hearing her late.
const turns = [];
function sendText(ws, token, last) {
  try {
    if (ws && ws._turnT0) { const now = Date.now(); if (!ws._turnFirst) ws._turnFirst = now - ws._turnT0; ws._turnTokens = (ws._turnTokens || 0) + 1;
      if (last) { turns.push({ at: new Date().toISOString(), call: ws._callSid || '', firstMs: ws._turnFirst, doneMs: now - ws._turnT0, tokens: ws._turnTokens, interrupts: ws._turnInterrupts || 0, words: String(token || '').split(/\s+/).length }); if (turns.length > 30) turns.shift(); ws._turnT0 = 0; ws._turnFirst = 0; ws._turnTokens = 0; ws._turnInterrupts = 0; } }
    ws.send(JSON.stringify({ type: 'text', token, last: !!last }));
  } catch (e) { logErr('ws.send', e); }
}

// v1.16 MAX'S EARS (SHADOW). He listens for an address in the caller's words;
// on the first hit of the call he asks GAS's read-only slot door and - with
// MAX_LIVE unset - LOGS what he would have handed her. MAX_LIVE=1 (the
// introduction, owner-flipped only) makes the card land in her context as
// [DISPATCH - Max]. He books nothing, speaks to no customer, ever.
const MAX_LIVE = process.env.MAX_LIVE === '1';
const MAX_ADDR_RE = /\b\d{1,5}\s+[A-Za-z][A-Za-z.\- ]{2,28}\s(?:st|street|rd|road|dr|drive|ln|lane|ave|avenue|hwy|highway|way|ct|court|cir|circle|ter|terrace|pl|place)\b/i;
function maxListen(s, text) {
  try {
    if (s.maxDone || !text) return;
    const m = MAX_ADDR_RE.exec(String(text));
    if (!m) return;
    s.maxDone = true;
    const addr = m[0];
    const u = GAS_URL + '?hook=maxslot&k=' + encodeURIComponent(WKEY);
    const ctl = new AbortController(); const tm = setTimeout(() => ctl.abort(), 8000);
    fetch(u, { method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ address: addr }), signal: ctl.signal })
      .then(r => r.text()).then(txt => {
        clearTimeout(tm);
        let j = {}; try { j = JSON.parse(txt); } catch (e) {}
        if (!j.ok || !j.slots || !j.slots.length) { logInfo('max: no slots for "' + addr + '"'); return; }
        const card = '[DISPATCH — Max] Slots for ' + addr + ': ' + j.slots.join(' · ');
        if (!MAX_LIVE) { logInfo('MAX-SHADOW would hand: ' + card); return; }
        if (!s.ws || s.ws.readyState !== 1) return;   // call ended - stale card dies
        s.convo.push({ role: 'user', content: card });
        logInfo('max: card handed on ' + s.callSid);
      }).catch(() => { clearTimeout(tm); });
  } catch (e) { logErr('maxListen', e); }
}
async function handlePrompt(s, voicePrompt) {
  // a new utterance always cancels a stale in-flight turn (barge-in via speech)
  if (s.ctl) { try { s.ctl.abort(); } catch (e) {} }
  s.convo.push({ role: 'user', content: String(voicePrompt).slice(0, 500) });
  maxListen(s, voicePrompt);   // v1.16 shadow ears - logs only unless MAX_LIVE=1

  // v1.1 brain choice, EVERY turn: the caller-specific pack (their dossier
  // inside) wins the moment it lands — even mid-call. First turn races it
  // briefly, then falls to the generic brain rather than keep a human waiting.
  // v1.8 THE 1.2-SECOND RACE LOST EVERY TIME (Arlena Taylor, 8/25). Apps
  // Script answers a brainpack in 2-8s; the first turn waited 1.2s, then ran
  // the GENERIC receptionist on a customer we know (asked a Tiers customer
  // 'what pest are you dealing with?') and, on a MISSION callback, opened
  // with 'how can I help you today?' to a person we had just dialed. Now:
  //   mission  - the generic brain is never an option. Wait for the mission
  //              pack (filler after 2s), and if it truly cannot come, say so
  //              honestly, flag the owner, and end — never impersonate the
  //              front desk on an outbound call.
  //   inbound  - wait 3s silently, then one filler line, then up to 6s more
  //              for the caller's dossier before falling to generic. A known
  //              customer is worth four seconds; a stranger's pack lands in
  //              the same window anyway.
  if (!s.callerPack) {
    const first = !s._packWaited;
    s._packWaited = true;
    if (s.mid) {
      try { await Promise.race([ s.packPromise, new Promise(res => setTimeout(res, 2000)) ]); } catch (e) {}
      if (!s.callerPack) {
        sendText(s.ws, 'One moment.', true);
        try { await Promise.race([ s.packPromise, new Promise(res => setTimeout(res, 12000)) ]); } catch (e) {}
      }
      if (!s.callerPack) {
        logErr('pack.mission', 'no mission pack for ' + s.callSid + ' (mid ' + s.mid + ') — refusing to run the receptionist on an outbound call');
        s.flag = true; s.needsCallback = true; s.endWhy = 'mission brain missing';
        s.convo.push({ role: 'assistant', content: '[MISSION ABORTED — assignment brain never arrived; owner flagged to call back personally]' });
        sendText(s.ws, 'Sorry — I am having a technical moment on my end. Chris will give you a call back shortly. Thanks for picking up.', true);
        setTimeout(async () => { try { await twilioUpdateCall(s.callSid, '<Response><Hangup/></Response>'); } catch (e) { logErr('hangup', e); } }, 7000);
        return;
      }
    } else if (genericPack && (function(){
    // v1.14 OWNER_LINES (the Kylie embarrassment): recognition checks the
    // ALLOWLIST the pack now carries, not a single number. Any house line in,
    // full owner mode. Falls back to the lone owner field on old packs.
    if (!s.from) return false;
    var d10 = String(s.from).replace(/\D/g, '').slice(-10);
    var list = (genericPack.ownerLines && genericPack.ownerLines.length) ? genericPack.ownerLines
             : (genericPack.owner ? [String(genericPack.owner).replace(/\D/g, '').slice(-10)] : []);
    return list.indexOf(d10) >= 0;
  })()) {
      // v1.11 THE OWNER'S CALL IS NEVER THE RECEPTIONIST'S. The generic pack
      // now carries the owner's number. His pack is cache-served (seconds),
      // but if it still has not landed we wait — and if it truly cannot come we
      // say so and hang up, rather than let the front desk tell the owner
      // 'I can't see my own backend from here' (8/26).
      try { await Promise.race([ s.packPromise, new Promise(res => setTimeout(res, 2500)) ]); } catch (e) {}
      if (!s.callerPack) {
        sendText(s.ws, 'One second, pulling everything up.', true);   // owner line: he always has an account
        try { await Promise.race([ s.packPromise, new Promise(res => setTimeout(res, 14000)) ]); } catch (e) {}
      }
      if (!s.callerPack) {
        logErr('pack.owner', 'owner pack never landed for ' + s.callSid + ' — refusing to run the receptionist on the owner');
        s.endWhy = 'owner brain missing';
        sendText(s.ws, 'Chris, my assistant brain did not load on this call. Hang up and call me right back — it will be warm.', true);
        setTimeout(async () => { try { await twilioUpdateCall(s.callSid, '<Response><Hangup/></Response>'); } catch (e) { logErr('hangup', e); } }, 6000);
        return;
      }
    } else {
      try { await Promise.race([ s.packPromise, new Promise(res => setTimeout(res, first ? 3000 : 800)) ]); } catch (e) {}
      if (!s.callerPack && first) {
        // v1.21 THE REFLEX MUST NOT ASSUME AN ACCOUNT (Aggie's journal, Sept 17: said 'pull up your account' to a
        // salesperson who never had one). The pack is not here yet at this point, so the line is neutral unless it is.
        sendText(s.ws, (s.callerPack && s.callerPack.known) ? 'One second while I pull up your account.' : 'One second — bear with me.', true);
        try { await Promise.race([ s.packPromise, new Promise(res => setTimeout(res, 6000)) ]); } catch (e) {}
        if (!s.callerPack) logErr('pack.late', 'caller pack still not landed after 9s for ' + s.callSid + ' — first turn runs generic');
      }
    }
  }
  let pack = s.callerPack || genericPack;
  if (!pack) {
    // v1.1 COLD-START PATIENCE: a fresh boot compiles the generic brain in the
    // background. Say something human ONCE, then wait up to 8 more seconds
    // before ever giving up. Transfer is the last resort, not the first reflex.
    sendText(s.ws, 'One second while I pull that up for you.', true);
    for (let i = 0; i < 16 && !genericPack && !s.callerPack; i++) {
      await new Promise(res => setTimeout(res, 500));
    }
    pack = s.callerPack || genericPack;
    if (!pack) {
      sendText(s.ws, 'Let me get you straight to the team.', true);
      return doTransfer(s, 'no brain after patience window');
    }
  }
  const sys = String(pack.sys).replace(/\{\{CALLER_ID\}\}/g, s.from || 'unknown');

  const ctl = new AbortController();
  s.ctl = ctl;
  let spoke = false;
  let raw = '';
  try {
    raw = await aiTurn(sys, s.convo, tok => { spoke = true; sendText(s.ws, tok, false); }, ctl.signal);
  } catch (e) {
    if (ctl.signal.aborted) return;    // superseded by a newer utterance — say nothing
    logErr('aiTurn', e);
    sendText(s.ws, 'Sorry, I hit a snag on my end — one moment while I get Chris for you.', true);
    return doTransfer(s, 'AI turn failed');
  } finally { if (s.ctl === ctl) s.ctl = null; }

  const d = parseTurn(raw);
  if (!d || !d.reply) {
    logErr('parse', 'unparseable: ' + raw.slice(0, 160));
    if (!spoke) sendText(s.ws, 'Sorry, say that one more time for me?', true);
    else sendText(s.ws, '', true);
    return;
  }
  if (!spoke) sendText(s.ws, d.reply, true);   // extractor missed (odd formatting) — speak the parsed reply
  else sendText(s.ws, '', true);               // close the utterance

  s.convo.push({ role: 'assistant', content: String(d.reply).slice(0, 500) });
  mergeLead(s.lead, d.lead);   // v1.17: merge BEFORE the hands, so a synthesized act sees this turn's extraction
  const isOwner = !!(s.callerPack && s.callerPack.owner === true);
  // v1.9 THE OWNER LINE. When the pack said owner:true, the brain is AGI and a
  // turn may carry `act` — a chat-bubble action the owner just approved out
  // loud. Post it to GAS (hook=voiceact, owner-number checked there too),
  // speak the result, and remember it in the convo so the next turn knows.
  if (d.act && d.act.action && isOwner) {
    try {
      const r = await postVoiceAct(s, d.act);
      // v1.10 A LOOKUP IS A THOUGHT, NOT A LINE. The record goes back into the
      // conversation as a note and she takes another turn with it in hand —
      // the owner hears the answer, never the raw dump.
      if (d.act.action === 'lookup') {
        const note = '[LOOKUP RESULT] ' + String((r && r.result) || (r && r.error) || 'no answer').slice(0, 900);
        s.convo.push({ role: 'user', content: note });
        const sys2 = sys;
        let raw2 = '';
        try { raw2 = await aiTurn(sys2, s.convo, tok => sendText(s.ws, tok, false), null); } catch (e) { logErr('lookupTurn', e); }
        const d2 = parseTurn(raw2);
        if (d2 && d2.reply) { sendText(s.ws, '', true); s.convo.push({ role: 'assistant', content: String(d2.reply).slice(0, 500) }); }
        else sendText(s.ws, 'Here is what I have: ' + note.replace(/^\[LOOKUP RESULT\] /, '').slice(0, 240), true);
        return;
      }
      const said = r && r.ok ? String(r.result || 'Done.').replace(/[\u2714\u2716\u2717\u23f3\ud83d\udcc5\u260e]/g, '').trim().slice(0, 240) : ('That did not go through: ' + String((r && r.error) || 'no answer from the office').slice(0, 120));
      sendText(s.ws, said, true);
      s.convo.push({ role: 'assistant', content: '[ran ' + d.act.action + ': ' + said.slice(0, 200) + ']' });
    } catch (e) { logErr('voiceact', e); sendText(s.ws, 'That action did not go through on my end.', true); }
  } else if (!isOwner && !s.mid) {
    // v1.17 THE CUSTOMER LINE'S HANDS. Her act, or the one her words imply,
    // rides to GAS while the caller is still on the line. Whatever GAS hands
    // back as `say` is what she says next — a real confirmation on ok, the
    // honest fallback on anything else. Missions keep their own script.
    let act = (d.act && d.act.action && CUSTOMER_ACTS[d.act.action]) ? d.act : null;
    let synthesized = false;
    if (!act) { act = synthesizeAct(s, d); synthesized = !!act; }
    // v1.20 ONE BOOKING PER CALL: once a bookJob landed, her recaps do not re-book (the second act failed the
    // evidence gate on 'thanks' and spoke 'Chris will confirm' after a perfectly good booking).
    if (act && synthesized && act.action === 'bookJob' && s.actsRan.some(a => a.action === 'bookJob' && a.ok)) act = null;
    if (d.act && d.act.action && !CUSTOMER_ACTS[d.act.action]) logErr('voicebook.refused', 'non-customer act ' + d.act.action + ' on ' + s.callSid + ' — ignored');
    if (act) {
      act.data = act.data || {};
      act.data.phone = act.data.phone || s.from;
      // v1.19 THE ACT INHERITS THE LEAD (owner's test call booked a job with no name -> no customer, no lead):
      // whatever she already extracted across the call rides with the act when the act itself left it blank.
      try { const L = s.lead || {}; ['name','address','email','service','pest','day','window'].forEach(k => { if (!act.data[k] && L[k]) act.data[k] = L[k]; }); if (!act.data.service && act.data.pest) act.data.service = act.data.pest; } catch (e) {}
      act.data.promised = act.data.promised || String(d.reply).slice(0, 240);
      if (!act.data.callerSaid) { for (let i = s.convo.length - 1; i >= 0; i--) { if (s.convo[i].role === 'user' && !/^\[/.test(String(s.convo[i].content || ''))) { act.data.callerSaid = String(s.convo[i].content || '').slice(0, 200); break; } } }
      try {
        const r = await postVoiceBook(s, act);
        const line = String((r && r.say) || FALLBACK_SAY).slice(0, 240);
        const ok = !!(r && r.ok);
        s.actsRan.push({ action: act.action, ok, woId: (r && r.woId) || '', synthesized, error: (r && r.error) || '' });
        // ok: only speak the office's line when her own reply did not already say it (avoid "You're set. You're on the books.")
        // not ok: ALWAYS speak the fallback — her words promised something the record could not hold.
        if ((!ok || synthesized) && line && !(r && r.recap)) sendText(s.ws, line, true);   // v1.20: a recap answer carries no line to speak
        s.convo.push({ role: 'assistant', content: '[' + (ok ? 'ran ' : 'FAILED ') + act.action + (synthesized ? ' (from my own words)' : '') + ': ' + (ok ? line : String((r && r.error) || 'no answer')).slice(0, 200) + ']' });
        logInfo('voicebook ' + act.action + (synthesized ? ' (synth)' : '') + ' ' + (ok ? 'ok ' + ((r && r.woId) || '') : 'FAIL ' + ((r && r.error) || '')) + ' on ' + s.callSid);
      } catch (e) { logErr('voicebook', e); sendText(s.ws, FALLBACK_SAY, true); }
    }
  }
  if (d.flagOwner) s.flag = true;
  if (d.commercial) s.commercial = true;
  if (d.tierOffered) s.tierOffered = true;
  if (d.tierTaken) s.tierTaken = String(d.tierTaken);
  if (d.sched && d.sched.action) s.sched = d.sched;

  if (d.transfer) return doTransfer(s, 'caller asked');
  // v1.12 HER MOUTH BINDS HER PLUMBING (owner, Sept 12: caller asked for a
  // person, she said "One moment while I try Chris for you" — and the
  // transfer flag never came, so the socket stayed open, the caller sat in
  // silence and hung up; zero calls reached his phone, a $350 quote walked).
  // If she TELLS the caller she is getting Chris, that IS the transfer,
  // JSON flag or not — the words she speaks to a customer are commitments.
  if (/\b(one moment|hold on|hang on|just a (second|moment|sec))?[^.]*\b(try|get|grab|connect you (?:to|with)|transfer(?:ring)? you to|put you through to)\s+(chris|him)\b/i.test(String(raw||''))) {
    return doTransfer(s, 'spoken-intent');
  }
  if (d.done) {
    s.done = true;
    s.endWhy = 'completed';
    // let TTS finish the closing recap, then hang up so the call never falls
    // through to the safety-net <Dial> and rings Chris after a booked call.
    const secs = Math.min(20, Math.max(4, Math.round(String(d.reply).split(/\s+/).length / 2.4) + 2));
    setTimeout(async () => {
      try { await twilioUpdateCall(s.callSid, '<Response><Hangup/></Response>'); } catch (e) { logErr('hangup', e); }
    }, secs * 1000);
  }
}

async function doTransfer(s, why) {
  // v1.4: SHE DID NOT KNOW SHE TRANSFERRED. Her websocket closes the instant
  // the call moves to Chris, so her record ended mid-sentence and looked to
  // everyone — including her — like the call dropped. Now the handoff is
  // written into the conversation itself, so the transcript, the thread, and
  // her memory of the customer all say plainly what happened.
  logInfo('transfer (' + why + ') ' + s.callSid);
  s.flag = true;
  s.endWhy = 'transferred:' + why;
  s.needsCallback = true;   // v1.5: they asked for a human — never let this go quiet
  s.convo.push({ role: 'assistant', content: '[TRANSFERRED TO CHRIS — ' + why + '. The rest of this conversation happened between the caller and Chris; the full call recording has it.]' });
  // v1.5 A TRANSFER NOBODY ANSWERS USED TO END THE LEAD. The old TwiML rang
  // Chris for 25 seconds, promised a callback, and hung up: no voicemail, no
  // flag, and no rescue (the call reads 'completed', so the missed-call sweep
  // never looks at it). A caller who ASKED for a human is the hottest lead of
  // the day and it evaporated. Three fixes here:
  //   answerOnBridge — the caller hears real ringing, not silence, and the
  //     call is not marked answered until Chris actually picks up
  //   timeout 20  — beats a Verizon voicemail pickup, so the caller lands on
  //     OUR recorder instead of Chris's personal greeting, where APS can see it
  //   Record      — the promise is kept: a message is taken, transcribed, and
  //     lands in the customer's thread through the same hook as every voicemail
  const recCb = GAS_URL + '?hook=rec&k=' + encodeURIComponent(WKEY) + '&vm=1';
  try {
    await twilioUpdateCall(s.callSid,
      '<Response>' + sayLine('One moment while I connect you.') +
      // v1.7 GUARDED BACKUP (owner: 'guard it'): the bridged human leg records
      // too. GAS treats leg=xfer as SECONDARY — it can never overwrite the
      // call-level recording; it only steps in if that one never arrived.
      '<Dial timeout="20" callerId="+18028999491" answerOnBridge="true" record="record-from-answer" recordingStatusCallback="' + xesc(GAS_URL + '?hook=rec&k=' + encodeURIComponent(WKEY) + '&leg=xfer') + '">' + xesc(CHRIS_CELL) + '</Dial>' +
      sayLine('Sorry, he could not grab the phone. Leave your name, number, and what you are seeing after the tone, and we will call you right back.') +
      '<Record maxLength="120" playBeep="true" recordingStatusCallback="' + xesc(recCb) + '"/>' +
      '<Say>Thanks. We will be in touch shortly.</Say><Hangup/></Response>');
  } catch (e) { logErr('transfer', e); }
}

function finalize(s) {
  if (s.finalized || !s.callSid) return;
  s.finalized = true;
  const secs = Math.max(1, Math.round((Date.now() - s.startedAt) / 1000));
  const hasLead = s.lead && (s.lead.name || s.lead.address || s.lead.pest);
  postResults({
    sid: s.callSid, from: s.from, to: s.to, dir: s.dir, mid: s.mid || '',
    endWhy: s.endWhy || (s.done ? 'completed' : 'caller hung up'),   // v1.4: never guess again why a call ended
    ts: new Date(s.startedAt).toISOString(),
    // v1.6 THE FIRST HALF OF THE CALL WAS BEING THROWN AWAY. A 24-turn cap
    // meant any conversation longer than a dozen exchanges arrived in APS with
    // its opening missing — the pest, the town, the address, all the parts that
    // matter most — and the thread appeared to start in the middle of nowhere.
    convo: s.convo.slice(-80),
    lead: hasLead ? s.lead : null,
    flag: s.flag || s.needsCallback, commercial: s.commercial, needsCallback: !!s.needsCallback,
    tierOffered: s.tierOffered, tierTaken: s.tierTaken,
    sched: s.sched, secs,
    needsSlot: !!(s.done && hasLead && !s.lead.window),
    actsRan: s.actsRan,   // v1.17: what her hands did on this call — the post-call sweep can skip what is already written
    done: s.done
  }).catch(e => logErr('finalize', e));
}

// ---- HTTP (health + keep-warm target) ---------------------------------------
const server = http.createServer((req, res) => {
  if (req.url && req.url.startsWith('/health')) {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({
      ok: true, gateway: GW_VERSION, up: Math.round(process.uptime()),
      brainAgeSec: genericPack ? Math.round((Date.now() - genericAt) / 1000) : null,
      brainVersion: genericPack ? genericPack.v : null,
      model: MODEL, callsHandled, liveCalls: sessions.size,
      promptCache: lastUsage || 'no turns yet since restart',
      hands: acts.slice(-8),   // v1.17: the last voicebook round-trips — proof her hands work, or exactly why not
      turns: turns.slice(-30), turnLag: (function(){ const f = turns.map(t => t.firstMs).filter(x => x > 0); if (!f.length) return null; f.sort((a,b)=>a-b); return { n: f.length, p50: f[Math.floor(f.length/2)], p90: f[Math.floor(f.length*0.9)], max: f[f.length-1], interrupts: turns.reduce((a,t)=>a+(t.interrupts||0),0), silentTurns: turns.filter(t=>!t.tokens).length }; })(),   // v1.22: the lag she feels, in numbers
      recentErrors: errs.slice(-8)
    }, null, 2));
    return;
  }
  res.writeHead(200, { 'content-type': 'text/plain' });
  res.end('Aggie gateway. Nothing to see here — the telephone is at /relay (websocket).');
});

// ---- WebSocket: the Twilio ConversationRelay protocol -----------------------
const wss = new WebSocketServer({ noServer: true });
server.on('upgrade', (req, socket, head) => {
  const u = new URL(req.url, 'http://x');
  if (u.pathname !== '/relay' || (RELAY_TOKEN && u.searchParams.get('t') !== RELAY_TOKEN)) {
    socket.destroy(); return;
  }
  const mid = u.searchParams.get('mid') || '';
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, mid));
});

wss.on('connection', (ws, mid) => {
  const s = newSession(ws);
  s.mid = String(mid || '');   // v1.3: present = this is a MISSION call
  sessions.set(ws, s);
  ws.on('message', msg => {
    let m = null;
    try { m = JSON.parse(msg); } catch (e) { return; }
    if (m.type === 'setup') {
      s.callSid = String(m.callSid || '');
      // v1.2: on an OUTBOUND rescue Twilio's from = our line and to = the
      // customer. The dossier, the lead, and the Inbox thread all belong to
      // the CUSTOMER — so s.from is always the customer's number, whichever
      // direction the call travels, and s.dir remembers the truth for the row.
      const outbound = /outbound/i.test(String(m.direction || ''));
      s.dir = outbound ? 'out' : 'in';
      s.from = String(outbound ? m.to : m.from) || '';
      s.to = String(outbound ? m.from : m.to) || '';
      callsHandled++;
      logInfo('call ' + s.callSid + ' from ' + s.from);
      // race the caller-specific brain (dossier inside) against the clock
      // v1.3 MISSIONS: an assignment brain, fetched by mid — never the
      // receptionist booking script. Same race, same patience, same rails.
      s.packPromise = (s.mid ? fetchPack('', 25000, s.mid) : fetchPack(s.from, 25000))
        .then(p => { if (p) { s.callerPack = p; logInfo('caller pack landed for ' + s.callSid); } return p; })
        .catch(e => { logErr('pack.caller', e); return null; });
      startRecording(s);   // v1.1: every live call is recorded, like v23.5 days
    }
    else if (m.type === 'prompt' && m.voicePrompt) {
      try { s.ws._turnT0 = Date.now(); s.ws._turnFirst = 0; s.ws._turnTokens = 0; s.ws._callSid = s.callSid || ''; } catch (e) {}   // v1.22 turn clock starts when the caller finishes
      handlePrompt(s, m.voicePrompt).catch(e => logErr('handlePrompt', e));
    }
    else if (m.type === 'interrupt') {
      try { s.ws._turnInterrupts = (s.ws._turnInterrupts || 0) + 1; } catch (e) {}   // v1.22
      if (s.ctl) { try { s.ctl.abort(); } catch (e) {} }
    }
    else if (m.type === 'error') {
      logErr('relay.error', m.description || JSON.stringify(m).slice(0, 200));
    }
  });
  ws.on('close', () => { sessions.delete(ws); finalize(s); });
  ws.on('error', e => { logErr('ws', e); });
});

server.listen(PORT, () => logInfo('Aggie gateway listening on :' + PORT + ' (model ' + MODEL + ')'));
