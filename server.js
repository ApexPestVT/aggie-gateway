// ============================================================================
// AGGIE'S NEW TELEPHONE — Voice Gateway v1.2 (Twilio ConversationRelay <-> Anthropic)
// v1.2: outbound rescues ride this road too — customer number derived from
// direction, call rows labeled correctly, and /health confesses its version.
// Apex Pest Solutions · pairs with APS 2.0 v34.11 (hook=brainpack / hook=relay)
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
const GW_VERSION = '1.7';
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
    // the whole canon, the dossier, the availability \u2014 tens of thousands of
    // tokens \u2014 and a ten-turn call paid for it ten times over. 87.7M tokens in a
    // week is what pushed the account into its spend cap mid-morning and took
    // her brain offline. Marking it cacheable means the first turn of a call
    // pays full price and every turn after reads from cache at a tenth the
    // cost. Same prompt, same behaviour, same voice \u2014 only the bill changes.
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
    if (m) { const j = JSON.parse(m[0]); if (j && j.reply) return j; }
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
    ctl: null                  // AbortController of the in-flight AI turn
  };
}

function mergeLead(into, from) {
  if (!from) return;
  for (const k of Object.keys(from)) {
    const v = String(from[k] || '').trim();
    if (v) into[k] = v;
  }
}

function sendText(ws, token, last) {
  try { ws.send(JSON.stringify({ type: 'text', token, last: !!last })); } catch (e) { logErr('ws.send', e); }
}

async function handlePrompt(s, voicePrompt) {
  // a new utterance always cancels a stale in-flight turn (barge-in via speech)
  if (s.ctl) { try { s.ctl.abort(); } catch (e) {} }
  s.convo.push({ role: 'user', content: String(voicePrompt).slice(0, 500) });

  // v1.1 brain choice, EVERY turn: the caller-specific pack (their dossier
  // inside) wins the moment it lands — even mid-call. First turn races it
  // briefly, then falls to the generic brain rather than keep a human waiting.
  if (!s.callerPack) {
    try { await Promise.race([ s.packPromise, new Promise(res => setTimeout(res, 1200)) ]); } catch (e) {}
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
  mergeLead(s.lead, d.lead);
  if (d.flagOwner) s.flag = true;
  if (d.commercial) s.commercial = true;
  if (d.tierOffered) s.tierOffered = true;
  if (d.tierTaken) s.tierTaken = String(d.tierTaken);
  if (d.sched && d.sched.action) s.sched = d.sched;

  if (d.transfer) return doTransfer(s, 'caller asked');
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
      '<Response><Say>One moment while I connect you.</Say>' +
      // v1.7 GUARDED BACKUP (owner: 'guard it'): the bridged human leg records
      // too. GAS treats leg=xfer as SECONDARY \u2014 it can never overwrite the
      // call-level recording; it only steps in if that one never arrived.
      '<Dial timeout="20" answerOnBridge="true" record="record-from-answer" recordingStatusCallback="' + xesc(GAS_URL + '?hook=rec&k=' + encodeURIComponent(WKEY) + '&leg=xfer') + '">' + xesc(CHRIS_CELL) + '</Dial>' +
      '<Say>Sorry, he could not grab the phone. Leave your name, number, and what you are seeing after the tone, and we will call you right back.</Say>' +
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
    // its opening missing \u2014 the pest, the town, the address, all the parts that
    // matter most \u2014 and the thread appeared to start in the middle of nowhere.
    convo: s.convo.slice(-80),
    lead: hasLead ? s.lead : null,
    flag: s.flag || s.needsCallback, commercial: s.commercial, needsCallback: !!s.needsCallback,
    tierOffered: s.tierOffered, tierTaken: s.tierTaken,
    sched: s.sched, secs,
    needsSlot: !!(s.done && hasLead && !s.lead.window),
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
      handlePrompt(s, m.voicePrompt).catch(e => logErr('handlePrompt', e));
    }
    else if (m.type === 'interrupt') {
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
