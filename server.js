// ============================================================================
// AGGIE'S NEW TELEPHONE — Voice Gateway v1.17 (Twilio ConversationRelay <-> Anthropic)
// v1.27: SAY IT, DO IT on the live call — the same commitment vocabulary as the kit's text/email lanes (v38.639): 'passed this
//        to the tech', 'the tech will be out', 'someone will be there this afternoon', 'on the board/schedule for' now bind a
//        booking the same as 'you're all set', so a visit she describes on a call cannot leave the call without a work order
//        (or the honest fallback line). Owner request Sept 21: 'on any communication with a client, she says it, she does it.'
// v1.26: LIVE PICTURE ON A CALL (owner: 'hey meta call aggie, with live picture sending'). Mid-call, when Chris
//        references a photo he just sent ('what is this', 'look at this', 'I sent you'), the gateway pulls the
//        pending MMS from GAS (hook=glasspending), fetches it from Twilio, and shows it to her in that turn. Owner only.
// v1.25: the temple tap sends the frame from the instant before the tap (auto:true) - she uses it only when the
//        question is about something he can see; a plain question with a tap photo is answered as a plain question.
// v1.24: THE GLASSES DOOR (owner, Sept 21: 'same voice as phone' + 'about 1 minute to answer the simplest questions').
//        POST /glass?k=WEBHOOK_KEY {q, img?, mt?} -> {ok, say, audio (base64 mp3 in her phone voice)}. The Aggie Eyes app
//        on his Ray-Ban Metas now rides this warm server like his phone calls do: GAS compiles the glasses brain
//        (hook=brainpack&glass=1), held here and refreshed in the background while the glasses are in use; photo +
//        question go to Claude in ONE streamed call; reads chain once like the phone; record changes ride hook=voiceact
//        with glass:true + his words, and GAS refuses them without the code. Voice: ElevenLabs with the phone line's
//        own voice id (needs ELEVENLABS_API_KEY here) -> else the memory server's /say voice -> else none (app speaks).
//        GET /glass/warm (app start) · GET /glass/say?text= (short fixed lines) · /health shows glass timings.
// v1.23: EVERY READ IS A THOUGHT, NOT A LINE (owner, Sept 18: 'if I ask how many rat jobs I want a number' / 'I especially
//        don't like it when she repeats verbatim records on the phone'). Search/lookup/memory results go back to her as a
//        private note (up to 6,000 chars, was 900 for lookup and a 240-char READ-ALOUD for every other search) and she
//        ANSWERS from it; one chained read allowed; the note shrinks once answered so later turns stay fast.
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
const GW_VERSION = '1.53';
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
let lastOwnerPack = null;        // v1.46 { pack, at } - the last owner pack that landed, good for 45 min when a fresh one is slow
let genericAt   = 0;
// v1.49 caller-file timing for /health: land = ring to file in hand; build = the office's own build time (0-ish when pre-warmed)
const packTimes = [];
function packStat(landMs, buildMs) { packTimes.push({ at: new Date().toISOString(), land: landMs, build: buildMs == null ? null : Number(buildMs) }); while (packTimes.length > 30) packTimes.shift(); }

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
// v1.48 THE BRAIN THAT IS ALWAYS THERE (Sept 28 6:31 PM: the office was busy with a 3-minute editor run, the generic pack never
// arrived, and the first caller heard 'Let me get you straight to the team'; Sept 28 7:54 PM the owner heard 'my brain did not
// load'). Two nets under the live pack: the last good generic pack is saved to disk on every refresh and reloaded at boot (a
// restart on the same instance keeps yesterday's brain), and under that a copy of the receptionist brain shipped with this file
// (fallback-brain.txt) so a cold instance with a dead office still answers like Aggie. A fallback pack is marked so the log says
// which brain took the call.
const PACK_DISK = require('path').join(require('os').tmpdir(), 'aggie-generic-pack.json');
function fallbackPack() {
  try {
    const sys = require('fs').readFileSync(require('path').join(__dirname, 'fallback-brain.txt'), 'utf8');
    return { ok: true, v: 'fallback', sys, greeting: 'Apex Pest Solutions, this is Aggie. How can I help you today?', vm: '', model: '', at: new Date().toISOString(), fallback: true, ownerLines: [] };
  } catch (e) { logErr('pack.fallback', e); return null; }
}
async function refreshGeneric() {
  try {
    genericPack = await fetchPack('', 25000);
    genericAt = Date.now();
    logInfo('brainpack refreshed (v' + genericPack.v + ', ' + genericPack.sys.length + ' chars)');
    try { require('fs').writeFileSync(PACK_DISK, JSON.stringify(genericPack)); } catch (e2) { logErr('brainpack.disk', e2); }
  } catch (e) {
    logErr('brainpack.refresh', e);
    if (!genericPack) {
      try { const j = JSON.parse(require('fs').readFileSync(PACK_DISK, 'utf8')); if (j && j.sys) { genericPack = j; genericPack.stale = true; logErr('brainpack.disk', 'using the pack saved ' + Math.round((Date.now() - Date.parse(j.at || 0)) / 60000) + ' min ago'); } } catch (e3) {}
      if (!genericPack) { genericPack = fallbackPack(); if (genericPack) logErr('brainpack.fallback', 'no office pack at all - the built-in receptionist brain is answering'); }
    }
  }
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

async function aiTurn(sys, convo, onReplyText, signal, image) {
  let messages = convo;
  // v1.28 THE CONVERSATION ENDS WITH THE CALLER (Sept 24 8:10 AM, owner line: 'anthropic 400: This model does not support
  // assistant message prefill' -> transfer (AI turn failed) -> she dialed Chris while he was on with her -> voicemail).
  // After an action ran, or a mission aborted, her own bracketed note was the LAST message; Opus 5 read that as a prefill
  // and carried on, Sonnet 4.6 refuses it. A trailing assistant turn now gets a user nudge so every model can answer.
  if (messages.length && messages[messages.length - 1].role === 'assistant') {
    messages = messages.slice();
    messages.push({ role: 'user', content: '[system: the result above is in. Say your next line to the caller now - spoken words only, same JSON shape as always.]' });
  }
  if (image && image.b64) {
    // v1.26: fold the photo into the LAST user turn so she sees what he sent, this turn only
    messages = convo.slice();
    for (let i = messages.length - 1; i >= 0; i--) {
      if (messages[i].role === 'user') {
        const t = typeof messages[i].content === 'string' ? messages[i].content : '(look at this)';
        messages[i] = { role: 'user', content: [
          { type: 'image', source: { type: 'base64', media_type: image.mt || 'image/jpeg', data: image.b64 } },
          { type: 'text', text: t + '\n(Chris just sent you this photo on the call — it is what he is looking at. Answer about it in one or two spoken sentences.)' }
        ] };
        break;
      }
    }
  }
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
    body: wellFormedJson(JSON.stringify(deepWellFormed({
      model: MODEL, max_tokens: MAX_TOKENS,
      system: [{ type: 'text', text: sys, cache_control: { type: 'ephemeral' } }],
      messages, stream: true
    })))   // v1.45 cleaned before serializing, netted after
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
// v1.38 THE OFFICE IS SLOW, NOT BROKEN (owner line, Sept 27 10:20 AM: every move that SAVED took 20+ s in the kit - Jim Burrows'
// first move finished one second AFTER the re-fire that found him already moved. The gateway quit at 20 s, said 'That action did not
// go through on my end', she re-fired, the office said 'already there', and she told the owner the system had moved them by itself.)
// A write now gets 20 s, then a spoken 'still saving' line, then one more 25 s try of the SAME act - a reschedule is idempotent, so the
// second answer ('already there' or the move itself) is the truth either way. Reads and refusals answer fast and never reach the retry.
// Only acts that are safe to run twice get the second try. A text, a charge, an invoice, a new record or a sign-up is NOT retried:
// if the first one landed after the cut, a retry would send or charge twice - she is told it may have gone through and to look first.
const IDEMPOTENT_ACTS = /^(rescheduleJob|moveDay|voidJob|updateLead|updateClient|updateWorkOrderStatus|setPrice|noteJob|terminateService|sendConfirmations|checklistDone|obligationClose|dutyDone|cancelCall|cancelText|forgetMemory|inboxArchive)$/;   // v1.42 + setPrice / status / note / terminate / sendConfirmations (Sept 28 6:51 AM: Sandy Ferguson's $0 landed twice and she told him it hadn't)   // v1.40 + moveDay (a second run finds the day empty and says so)
const LONG_ACTS = { moveDay: 1, sendConfirmations: 1, terminateService: 1 };   // v1.42   // v1.40 a whole day is several saves; give it 90 s, then one 60 s retry
const ONESHOT_ACTS = /^(createWorkOrder|createLead|sendInvoice|chargeCard|tierSignup|smsReply|scheduleText|scheduleCall|checklistAdd|routeBuild|emailReply|rememberThis)$/;
// v1.39 A FRESH WIRE EVERY TIME (Sept 27 11:59 AM: Blake Gintof's move reached the office in 9 s; Jim Burrows' three posts over the
// next three minutes NEVER arrived - no ledger row, no error row, nothing, while the gateway waited 45 s each and gave up. Same shape
// as Charlie Thomas' three at 10:20 AM. Leading read, unverified without the Render log: a kept-alive socket to Google that went dead
// on their side and swallowed the next request. Every office post now closes its connection, and the log times each phase so the
// next stall is visible: 'voiceact phases: headers 1204 ms, body 1310 ms'.)
async function postVoiceActOnce(s, act, ms) {
  const u = GAS_URL + '?hook=voiceact&k=' + encodeURIComponent(WKEY);
  const ctl = new AbortController(); const tm = setTimeout(() => ctl.abort(), ms);
  if (LONG_ACTS[String(act && act.action)]) s.actCtl = ctl;   // v1.46 the owner's 'stop' can cut the wait (not the office - it keeps working)
  const t0 = Date.now(); let tH = 0;
  try {
    const r = await fetch(u, { method: 'POST', headers: { 'content-type': 'application/json', 'connection': 'close' },
      body: wellFormed(JSON.stringify(s.glass ? { sid: s.callSid, from: s.from, act, glass: true, said: String(s.said || '') } : { sid: s.callSid, from: s.from, act })), redirect: 'follow', signal: ctl.signal });
    tH = Date.now() - t0;
    const txt = await r.text();
    logInfo('voiceact phases ' + String(act && act.action) + ': headers ' + tH + ' ms, body ' + (Date.now() - t0) + ' ms, http ' + r.status + ' on ' + s.callSid);
    try { return JSON.parse(txt); } catch (e) { return { ok: false, error: 'unreadable: ' + txt.slice(0, 80) }; }
  } catch (e) {
    logErr('voiceact.phase', String(act && act.action) + ' died at ' + (Date.now() - t0) + ' ms (' + (tH ? 'after headers' : 'NO HEADERS - the request never came back') + '): ' + String((e && e.message) || e));
    throw e;
  } finally { clearTimeout(tm); if (s.actCtl === ctl) s.actCtl = null; }
}
async function postVoiceAct(s, act) {
  const an0 = String((act && act.action) || '');
  if (LONG_ACTS[an0]) {
    const hb = [];   // v1.40 a word every 30 s so the line is never dead while the office works through the day
    if (s.ws && !s.glass) { hb.push(setTimeout(() => { try { sendText(s.ws, 'Still working through them.', true); } catch (e) {} }, 30000)); hb.push(setTimeout(() => { try { sendText(s.ws, 'Almost there.', true); } catch (e) {} }, 60000)); }
    // v1.46 NO SECOND RUN OF A DAY-LONG ACT (owner call Sept 28 8:07 PM: the afternoon moveDay wrote Cage Martin and Janet Coon, then
    // nothing; at 90 s the gateway fired it AGAIN while the office was still on the first one, then a third time with the override - two
    // more executions fighting the first for the script lock, 'the office did not answer' three times, and his 'No. Stop.' queued behind
    // the whole thing. A long act runs once. If the office is still working at 90 s, she says so and tells him how to check; his
    // 'stop' cuts the wait at once.)
    try { return await postVoiceActOnce(s, act, 90000); }
    catch (e) {
      if (s.stopSaid) { s.stopSaid = false; throw new Error('STOPPED AT HIS WORD - the office may still be working on it; look the day up before touching it again'); }
      logErr('voiceact.slow', String((e && e.message) || e) + ' on ' + an0 + ' - not re-run');
      throw new Error('STILL RUNNING - the office had not answered at 90 s and this act is never run twice; some jobs may have moved. Look the day up (searchJobs) before doing anything else with it');
    }
    finally { hb.forEach(clearTimeout); }
  }
  try { return await postVoiceActOnce(s, act, 20000); }
  catch (e) {
    const aborted = e && (e.name === 'AbortError' || /abort/i.test(String(e && e.message)));
    const an = String((act && act.action) || '');
    if (aborted && ONESHOT_ACTS.test(an)) throw new Error('the office was still working at 20 s - it MAY HAVE COMPLETED');
    if (!IDEMPOTENT_ACTS.test(an)) throw e;
    logErr('voiceact.slow', (aborted ? 'aborted at 20 s' : String((e && e.message) || e)) + ' on ' + an + ' - one more try');
    if (s.ws && !s.glass) { try { sendText(s.ws, 'The office is still saving that. One second.', true); } catch (e2) {} }
    return await postVoiceActOnce(s, act, 25000);
  }
}

// v1.17 THE CUSTOMER LINE'S HANDS (Aggie's own #1, Sept 16 — Pedro Tito: a time
// promised on a live call with no work order behind it). One try, 8s: the caller
// is on the line. GAS answers with a `say` line either way; on timeout the
// fallback is spoken and the post-call sweep + the red row catch the rest.
// v1.23 the reads: their results come back to HER, never read to the owner raw
const READ_ACTS = { lookup:1, searchMemory:1, searchClients:1, searchJobs:1, searchInbox:1, readThread:1, lookupClient:1, lookupJob:1,
  mindReport:1, recallMemory:1, explainMemory:1, intentions:1, leadsRecent:1, salesBacklog:1, auditLeads:1, auditWon:1, previewPurge:1, cacheReport:1, callQueue:1,
  books:1, bankRecent:1, searchCalls:1 };   // v1.52 the call recordings come back to her like every read (kit 742)   // v1.50 (owner call Sept 30 4:23 PM: 'pulling the P and L... that one didn't come back') - the books come back to HER, like every read
const CUSTOMER_ACTS = { bookJob: 1, cancelJob: 1, noteJob: 1, confirmJob: 1, rescheduleJob: 1, cardLink: 1, updateContact: 1, sendQuote: 1, tierSignup: 1, dncAdd: 1, contactChange: 1 };   // v1.53: + contactChange (a new contact for an account - parked for Chris's tap)   // v1.31: + dncAdd   // v1.30: + tierSignup (she signs them up)   // v1.18: + reschedule · v1.29: + cardLink (tier sign-up -> Square link on the call)
// v1.36 no more 'Chris will confirm' (owner: no hand-offs). If the office is slow she says she is saving it; the promise net + office retry finish the job.
const FALLBACK_SAY = 'One moment while I check on that.';
// v1.36 THE LONE SURROGATE (Sept 26 14:20 aiTurn 400 'invalid high surrogate in string'): a pack/thread string cut mid-emoji poisoned the whole
// request and the turn died. Every request body is made well-formed before it leaves.
function wellFormed(str) {
  const s = String(str || '');
  if (typeof s.toWellFormed === 'function') return s.toWellFormed();
  return s.replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])/g, '\uFFFD').replace(/(^|[^\uD800-\uDBFF])([\uDC00-\uDFFF])/g, '$1\uFFFD');
}
// v1.45 THE HALF-EMOJI THAT KILLED TWO CALLS (aggie-sim, Sept 28 3:26 PM: Bonnie Hipko and Arlena Taylor - every brain call after the
// caller file landed came back 'anthropic 400: invalid high surrogate in string' and she said 'let me get Chris' to everything).
// 1.36's wellFormed() ran on the JSON TEXT - but JSON.stringify had already turned the broken character into the six plain
// characters \ud83d, which are perfectly well-formed text, so nothing was repaired and the API refused the escape. The strings
// are cleaned BEFORE they are serialized now (system text, every message, every text block), and the serialized text gets a
// second net for any escape that slipped through.
function deepWellFormed(v) {
  if (typeof v === 'string') return wellFormed(v);
  if (Array.isArray(v)) return v.map(deepWellFormed);
  if (v && typeof v === 'object') { const o = {}; for (const k of Object.keys(v)) o[k] = deepWellFormed(v[k]); return o; }
  return v;
}
function wellFormedJson(text) {
  return String(text || '')
    .replace(/\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})/g, '\\ufffd')       // a high escape with no low escape after it
    .replace(/(^|[^0-9a-fA-F]|(?<!\\u[dD][89abAB][0-9a-fA-F]{2}))(\\u[dD][c-fC-F][0-9a-fA-F]{2})/g, (m, pre, esc, off, str) => (/\\u[dD][89abAB][0-9a-fA-F]{2}$/.test(str.slice(Math.max(0, off - 6), off + pre.length)) ? m : pre + '\\ufffd'));   // a low escape with no high escape before it
}
async function postVoiceBook(s, act, _retry) {
  // v1.36 (health log Sept 25: seven bookJob 'This operation was aborted' at 8000 ms). The office's write chain can outrun 8 s on a
  // busy sheet; the kit keeps running after the cut, so the job often DID save. One retry after an abort: the kit answers an
  // existing same-day job as a recap (ok:true), so she tells the truth instead of 'I will have Chris confirm'.
  const u = GAS_URL + '?hook=voicebook&k=' + encodeURIComponent(WKEY);
  // v1.44 THE OFFICE NEEDS 20 SECONDS TO BOOK (health log Sept 28: Patty 8 s + 10 s aborted, Bonnie Hipko 8 s + 10 s aborted TWICE
  // while the office appended two work orders). The 8 s cut was set when the line had no voice during the wait; 1.43 speaks at 3 s
  // and 10 s, so the wait can be honest instead of short. 20 s first, 12 s retry; the kit's reservation (v38.701) makes the retry safe.
  const ctl = new AbortController(); const tm = setTimeout(() => ctl.abort(), _retry ? 12000 : 20000);
  const t0 = Date.now();
  try {
    const r = await fetch(u, { method: 'POST', headers: { 'content-type': 'application/json', 'connection': 'close' },   // v1.39 fresh wire
      body: JSON.stringify({ callSid: s.callSid, from: s.from, act }), redirect: 'follow', signal: ctl.signal });
    const txt = await r.text();
    let j; try { j = JSON.parse(txt); } catch (e) { j = { ok: false, error: 'unreadable: ' + txt.slice(0, 80), say: FALLBACK_SAY }; }
    acts.push({ at: new Date().toISOString(), sid: s.callSid, action: act.action, ok: !!j.ok, ms: Date.now() - t0, woId: j.woId || '', error: j.error || '' });
    while (acts.length > 12) acts.shift();
    return j;
  } catch (e) {
    acts.push({ at: new Date().toISOString(), sid: s.callSid, action: act.action, ok: false, ms: Date.now() - t0, error: String((e && e.message) || e).slice(0, 80) + (_retry ? ' (retry)' : '') });
    while (acts.length > 12) acts.shift();
    clearTimeout(tm);
    if (!_retry && /abort/i.test(String((e && e.message) || e))) return postVoiceBook(s, act, true);   // v1.36 one more try, then the honest line
    return { ok: false, error: String((e && e.message) || e).slice(0, 120), say: FALLBACK_SAY };
  } finally { clearTimeout(tm); }
}

// v1.17 THE LAW OF THE SAME TURN (voice edition of GAS v38.512/514). If her
// spoken reply commits to a date / a cancel / a note and the JSON carried no
// act, build the act from what she already extracted (lead) plus the caller's
// last words as the agreement quote. GAS still applies the evidence law — a
// synthesized bookJob without a real "yes" in callerSaid books nothing and
// hands back the honest line.
const RX_SAID_BOOKED = /\b((?:you(?:'|’)?re|you are) (?:all )?set|on the (?:books|board|schedule)(?: for)?|got you (?:down|in) for|i(?:'|’)?ve got you (?:down|in)|(?:we(?:'|’)?ll|i(?:'|’)?ll) (?:be|see you) (?:there|out|then)|booked you|scheduled for|see you (?:on |then|at )?|passed (?:this|it|that)?\s*(?:straight |along )?to (?:the )?tech|the tech will (?:be (?:out|there)|come|stop by|handle)|(?:someone|a tech) will be (?:out|there)|put you (?:down|on the (?:books|board|schedule)))\b/i;   // v1.27: + the kit's shared commitment vocabulary
const RX_SAID_CANCEL = /\b((?:it(?:'|’)?s|that(?:'|’)?s|that one(?:'|’)?s|you(?:'|’)?re) (?:all )?(?:canceled|cancelled)|(?:i(?:'|’)?ve )?(?:canceled|cancelled) (?:it|that|your|the)|taken (?:it|that|you) off (?:the|our) (?:schedule|books|calendar)|off the schedule)\b/i;
const RX_SAID_MOVED  = /\b(moved (?:you|it|that|your visit) to|you(?:'|’)?re (?:now )?(?:moved|rescheduled) (?:to|for)|rescheduled (?:you|it|that|your visit) (?:to|for)|new (?:day|date) is)\b/i;
const RX_SAID_NOTED  = /\b(i(?:'|’)?ve noted|i noted|noted (?:on|for)|on (?:the|your) work ?order|tech (?:comes|will come|will be) prepared|i(?:'|’)?ll (?:make a note|note that)|added (?:it|that) to (?:this|your|the) visit)\b/i;
// v1.37 the identity of an act inside one call: what + who + which day
function actKey(a) {
  const d = (a && a.data) || {};
  return String(a.action || '') + '|' + String(d.name || '').toLowerCase().replace(/\s+/g, ' ').trim() + '|' + String(d.day || d.newDate || d.date || '').toLowerCase().trim();
}
function synthesizeAct(s, d) {
  const reply = String((d && d.reply) || '');
  const lastUser = (function () { for (let i = s.convo.length - 1; i >= 0; i--) { if (s.convo[i].role === 'user' && !/^\[/.test(String(s.convo[i].content || ''))) return String(s.convo[i].content || ''); } return ''; })();
  const lead = Object.assign({}, s.lead || {}, (d && d.lead) || {});
  if (RX_SAID_CANCEL.test(reply)) return { action: 'cancelJob', data: { name: lead.name || '', phone: s.from, promised: reply.slice(0, 240), callerSaid: lastUser.slice(0, 200) } };
  if (RX_SAID_MOVED.test(reply) && (lead.day || lead.window)) return { action: 'rescheduleJob', data: { name: lead.name || '', phone: s.from, day: lead.day || '', window: lead.window || '', promised: reply.slice(0, 240), callerSaid: lastUser.slice(0, 200) } };
  // v1.29 a spoken 'you're confirmed' with no act -> confirmJob (GAS stamps the waiting job from the caller's own yes; the post-call backstop catches the rest)
  if (RX_SAID_CONFIRMED.test(reply)) return { action: 'confirmJob', data: { name: lead.name || '', phone: s.from, promised: reply.slice(0, 240), callerSaid: lastUser.slice(0, 200) } };
  if (/\b(you(?:'|’)?re (?:all )?signed up|signed you up|welcome to the (?:standard|alpha|omega))\b/i.test(reply) && (s.tierTaken || (d && d.tierTaken))) return { action: 'tierSignup', data: { name: lead.name || '', phone: s.from, tier: String(s.tierTaken || d.tierTaken || ''), address: lead.address || '', email: lead.email || '', promised: reply.slice(0, 240), callerSaid: lastUser.slice(0, 200) } };   // v1.30
  if (/\b((?:removed|taken|took) you (?:off|from) (?:our|the) list|you(?:'|’)?re off (?:our|the) list|won(?:'|’)?t (?:hear from|be contacted by) us)\b/i.test(reply)) return { action: 'dncAdd', data: { name: lead.name || '', phone: s.from, reason: lastUser.slice(0, 160), promised: reply.slice(0, 240), callerSaid: lastUser.slice(0, 200) } };   // v1.31
  if (RX_SAID_CONTACT.test(reply) && (lead.address || lead.email)) return { action: 'updateContact', data: { name: lead.name || '', phone: s.from, address: lead.address || '', email: lead.email || '', promised: reply.slice(0, 240), callerSaid: lastUser.slice(0, 200) } };
  if (RX_SAID_SENT.test(reply) && (lead.service || lead.pest)) return { action: 'sendQuote', data: { name: lead.name || '', phone: s.from, service: lead.service || lead.pest || '', promised: reply.slice(0, 240), callerSaid: lastUser.slice(0, 200) } };
  if (RX_SAID_BOOKED.test(reply) && (lead.day || lead.window)) return { action: 'bookJob', data: { name: lead.name || '', phone: s.from, address: lead.address || '', service: lead.service || lead.pest || '', day: lead.day || '', window: lead.window || '', price: lead.price || '', promised: reply.slice(0, 240), callerSaid: lastUser.slice(0, 200) } };   // v1.45 a booking claim outranks a note (sim: 'I've got you down for Friday morning ... so the tech comes prepared' synthesized a noteJob, the note ran, and the booking claim rode out on it - no job was ever made)
  if (RX_SAID_NOTED.test(reply)) return { action: 'noteJob', data: { name: lead.name || '', phone: s.from, note: (reply.slice(0, 200) + (lastUser ? (' — caller said: “' + lastUser.slice(0, 120) + '”') : '')), promised: reply.slice(0, 240), callerSaid: lastUser.slice(0, 200) } };
  if (RX_SAID_BOOKED.test(reply) && (lead.day || lead.window)) return { action: 'bookJob', data: { name: lead.name || '', phone: s.from, address: lead.address || '', service: lead.service || lead.pest || '', day: lead.day || '', window: lead.window || '', price: lead.price || '', promised: reply.slice(0, 240), callerSaid: lastUser.slice(0, 200) } };
  return null;
}

// v1.29 THE MOUTH WAITS FOR THE HANDS (owner, Sept 24: "if she says it, it happens ... hard rules, no grey area").
// Until now her reply streamed to the caller's ear token by token WHILE the model was still writing it, and the act ran
// afterwards — so "you're all set for Friday the 26th" was spoken before the office could refuse it (Jacob Dalton: the 26th
// was a Saturday; nothing was booked; he was told he was set). On every non-mission turn the reply is now BUFFERED: the
// office writes first, then she speaks. If the office refuses, every sentence that claims a record changed is cut from her
// words and the office's honest line takes its place. Cost: the caller hears her a second or two later than before.
const RX_SAID_CONFIRMED = /\b(you(?:'|’)?re confirmed|confirmed (?:you |it |that )?for|i(?:'|’)?ve confirmed)\b/i;
const RX_SAID_CARD = /\b((?:i(?:'|’)?ve |i |just )?(?:sent|texted|emailed) (?:you |over )?(?:the |a |your )?(?:card|payment|signup|sign-up|square) link|link (?:is )?(?:on its way|coming)|check your (?:texts?|phone) for the link)\b/i;
const RX_SAID_SENT = /\b((?:i(?:'|’)?ve |i(?:'|’)?ll |i |just )(?:sent|send|emailed|email|texted|text|shot|shoot)(?:ing)? (?:you |that |it |over )?(?:over |along )?(?:the |a |your )?(?:quote|estimate|pricing|proposal|agreement|contract|paperwork|receipt|details|info(?:rmation)?)\b)/i;
const RX_SAID_CONTACT = /\b((?:i(?:'|’)?ve |i |just )?(?:updated|changed|corrected|fixed|put|saved) (?:your |the )?(?:new )?(?:address|phone|number|email|contact (?:info|information))(?: on file)?)\b/i;
const RX_SAID_ANY = [RX_SAID_BOOKED, RX_SAID_CANCEL, RX_SAID_MOVED, RX_SAID_NOTED, RX_SAID_CONFIRMED, RX_SAID_CARD, RX_SAID_SENT, RX_SAID_CONTACT];   // v1.29 1b: + sent / contact
const RX_DESCRIBES = /\b(already|currently|still)\b/i;   // v1.45 'you're already on the books for Wednesday' describes the file; it promises nothing
function saidClaims(reply) { return String(reply || '').split(/(?<=[.!?])\s+/).some(x => !RX_DESCRIBES.test(x) && RX_SAID_ANY.some(rx => rx.test(x))); }
// v1.45 a claim is BACKED when an act of that kind already succeeded on this call - a recap of the truth is not a promise
const CLAIM_BACKERS = [[RX_SAID_BOOKED, /^(bookJob|rescheduleJob|confirmJob)$/], [RX_SAID_CANCEL, /^cancelJob$/], [RX_SAID_MOVED, /^rescheduleJob$/], [RX_SAID_NOTED, /^(noteJob|bookJob)$/], [RX_SAID_CONFIRMED, /^(confirmJob|bookJob)$/], [RX_SAID_CARD, /^(cardLink|tierSignup)$/], [RX_SAID_SENT, /^sendQuote$/], [RX_SAID_CONTACT, /^updateContact$/]];
function stripUnbacked(s, reply) {   // v1.45 keep every sentence except a claim no act on this call backs
  return String(reply || '').split(/(?<=[.!?])\s+/).filter(x => RX_DESCRIBES.test(x) || !RX_SAID_ANY.some(rx => rx.test(x)) || CLAIM_BACKERS.some(([rx, acts]) => rx.test(x) && s.actsRan.some(a => a.ok && acts.test(a.action)))).join(' ').replace(/\s+/g, ' ').trim();
}
function claimsBacked(s, reply) {
  const sents = String(reply || '').split(/(?<=[.!?])\s+/).filter(x => !RX_DESCRIBES.test(x) && RX_SAID_ANY.some(rx => rx.test(x)));
  if (!sents.length) return true;
  return sents.every(x => CLAIM_BACKERS.some(([rx, acts]) => rx.test(x) && s.actsRan.some(a => a.ok && acts.test(a.action))));
}
function stripClaims(reply) {
  const sents = String(reply || '').split(/(?<=[.!?])\s+/);
  const kept = sents.filter(x => RX_DESCRIBES.test(x) || !RX_SAID_ANY.some(rx => rx.test(x)));   // v1.45 a description of the file stays
  return kept.join(' ').replace(/\s+/g, ' ').trim();
}
// what she says once the office has answered: her own words when they came true, the office's line when they did not
// v1.31 (kit 673 parity): a callback promise carries no clock; a window job never gets a clock time in her mouth
const RX_CALLBACK = /\b(?:(?:chris|he|someone|we|i)(?:'|’)?(?:ll| will) (?:call|ring|get back to|reach out to|text|follow up with|be in touch with) you(?: back)?|(?:have|ask|get) chris (?:call|ring|get back to|reach out to|text|follow up with) you(?: back)?)\b/i;
const RX_CALLBACK_TIME = /\s*(?:,\s*)?\b(within (?:the |an? )?(?:hour|half hour|\d+ ?(?:minutes|mins|hours|hrs))|in (?:a few|a couple(?: of)?|\d+) ?(?:minutes|mins|hours|hrs)|by (?:noon|midday|\d{1,2}(?::\d{2})?\s*(?:am|pm|o(?:'|’)?clock)?|end of (?:the )?day|eod|tonight|this (?:morning|afternoon|evening)|close of business)|(?:first thing )?(?:tomorrow|today|tonight)(?: morning| afternoon| evening)?|right away|in a (?:bit|moment|minute)|momentarily|asap|as soon as possible|before (?:noon|\d{1,2}(?::\d{2})?\s*(?:am|pm)?|end of day|tonight|he leaves))\b/i;
const RX_CLOCK = /\b(?:at|around|about|by|before|after)\s+(\d{1,2})(?::(\d{2}))?(?:\s*(am|pm|a\.m\.|p\.m\.|o(?:'|’)?clock))?\b(?!\s*(?:-|to|–)\s*\d)(?!\s+[A-Z][a-z])(?!\s+(?:street|st|road|rd|drive|dr|lane|ln|ave|avenue|way|court|ct|circle|terrace|place|highway|hwy|route|rte)\b)/i;   // v1.46 'at 19 Oswald Street' is an address, not a clock (sim run 6: 'between 12 and 5 Oswald Street')
function tidyPromises(reply, win) {
  let out = String(reply || '');
  out = out.split(/(?<=[.!?])\s+/).map(sent => {
    let x = sent;
    if (RX_CALLBACK.test(x)) { for (let i = 0; i < 3 && RX_CALLBACK_TIME.test(x); i++) x = x.replace(RX_CALLBACK_TIME, ''); x = x.replace(/\s{2,}/g, ' ').replace(/\s+([.!?,])/g, '$1'); }
    if (win && RX_CLOCK.test(x)) { const ww = win === 'morning' ? 'between 8 and 12' : win === 'afternoon' ? 'between 12 and 5' : 'anytime that day'; x = x.replace(RX_CLOCK, ww).replace(/\s{2,}/g, ' '); }
    return x;
  }).join(' ');
  return out;
}
// v1.33 THE OWNER LINE (Sept 25 10:39, Sandy $0): she said 'Done. Sandy's job is set to 0.' before the office refused three times;
// then 'One moment while I try Chris for you' - to Chris, on his own line. Owner-claim sentences are cut when the office says no,
// and a transfer line on the owner line is never spoken and never dialed.
const RX_OWNER_CLAIM = /(^|\s)(?:done|all set|on it|consider it done)\b|\b(?:i(?:'|’)?ve |i |just )?(?:set|updated|changed|moved|marked|zeroed|voided|rescheduled|cancel+ed|noted|sent|texted|emailed|booked|charged|invoiced)\b[^.!?]*\b(?:to|as|for|on)\b/i;
function stripOwnerClaims(reply) {
  return String(reply || '').split(/(?<=[.!?])\s+/).filter(x => !RX_OWNER_CLAIM.test(x) && !/^\s*done\.?\s*$/i.test(x)).join(' ').trim();
}
const RX_TRANSFER_LINE = /[^.!?]*\b(?:one moment|hold on|hang on|just a (?:second|moment|sec)|let me)\b[^.!?]*\b(?:try|get|grab|connect you|transfer(?:ring)? you|put you through|reach)\b[^.!?]*\b(?:chris|him)\b[^.!?]*[.!?]?/i;
function stripTransferLine(reply) { return String(reply || '').replace(RX_TRANSFER_LINE, '').replace(/\s{2,}/g, ' ').trim(); }
// v1.35 THE HAND-OFF RULE ON THE PHONE (owner, Sept 25: 'I do not want to talk to a client unless strictly necessary'). A customer
// sentence that hands them to Chris is removed unless the call is commercial or the sentence is about money; a scheduling hand-off
// becomes the booking question. Missions and the owner line are untouched.
const RX_HANDOFF = /\b(?:chris|he)(?:'|’)?(?:ll| will) (?:call|ring|get back to|reach out(?: to)?|follow up(?: with)?|confirm|text|send|be in touch|get in touch|contact|take care of|go over|look at|walk you through)\b|\bi(?:'|’)?(?:ll| will) have chris\b|\bhave chris (?:call|confirm|reach|get back|follow|text|send|look|contact)\b|\b(?:let me |i(?:'|’)?(?:ll| will) )?(?:pass|send|flag)(?:ing)? (?:this|that|it|along)[^.!?]{0,40}?\b(?:to|for) chris\b|\bmake sure chris knows\b|\bnoted? for chris to\b|\bfor chris to (?:call|text|reach|confirm|sort)\b|\bget chris for you\b|\bflag (?:this|that|it)[^.!?]{0,30}\b(?:for|to) the (?:owner|boss)\b|\bthe (?:owner|boss) (?:will|can|(?:'|’)ll) (?:call|reach|look|sort|locate|find|get back)\b/i;   // v1.47 'the owner' is Chris too (sim run 9: 'I'll flag this for the owner right now so he can locate Scott's account')   // v1.45 + 'flag this for Chris to sort out', 'pass along your request for Chris to call you', 'make sure Chris knows', 'noted for Chris to call' (sim, Sept 28)
const RX_HANDOFF_OK = /\b(refund|charged twice|double.?charg|overcharg|bill(?:ing|ed|s)?\b|invoice|commercial|business|restaurant|property manag|building|termite|\bbats?\b|contract|estimate|dispute|credit)/i;   // v1.45 'pricing', 'quote', 'card', 'payment' dropped: she quotes the Canon and sends the card link herself (sim: 'flag this for Chris ... so you get the right pricing' rode out on the word pricing)
const ASK_DAY = 'Just so I lock it in right — which day works best for you, morning or afternoon?';
function stripHandoff(reply, haveDay) {
  let asked = false, cut = 0;
  const out = String(reply || '').split(/(?<=[.!?])\s+/).map(x => {
    if (!RX_HANDOFF.test(x) || RX_HANDOFF_OK.test(x)) return x;
    cut++;
    if (!haveDay && /\b(time|day|date|schedul|appointment|visit|book|window|come out|when)\b/i.test(x) && !asked) { asked = true; return ASK_DAY; }   // v1.45 never re-ask the day once it is known
    return '';
  }).filter(Boolean).join(' ').trim();
  return { text: out || (cut ? 'Is there anything else I can take care of for you right now?' : String(reply || '')), cut };
}
function gateSpeak(reply, ok, line) {
  if (ok) return String(reply || '').trim();
  const rest = stripClaims(reply);
  const L = String(line || FALLBACK_SAY).trim();
  return (rest ? (rest + ' ') : '') + L;
}

// v1.38 THE CLOCK ON THE WALL (owner line, Sept 27 10:20 AM: 'move Saturday's jobs to Sunday' -> she called Sunday 9/27's jobs
// 'Saturday' and tried to move them to 9/28. The pack's 7-day schedule starts today, so yesterday never appears and she guessed
// the weekday.) Every call opens with today, yesterday and tomorrow spelled out, from the gateway's own clock, never a cache.
const TZ = 'America/New_York';
function dayName(d) { return d.toLocaleDateString('en-US', { timeZone: TZ, weekday: 'long' }); }
function mdy(d) { return d.toLocaleDateString('en-US', { timeZone: TZ, month: 'numeric', day: 'numeric' }); }
function clockLine() {
  const now = new Date();
  const y = new Date(now.getTime() - 86400000), t = new Date(now.getTime() + 86400000);
  const tail = []; for (let i = 2; i <= 6; i++) { const d = new Date(now.getTime() + i * 86400000); tail.push(dayName(d).slice(0, 3) + ' ' + mdy(d)); }
  return 'RIGHT NOW: ' + now.toLocaleDateString('en-US', { timeZone: TZ, weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' }) + ', '
    + now.toLocaleTimeString('en-US', { timeZone: TZ, hour: 'numeric', minute: '2-digit' }) + ' Eastern. Yesterday was ' + dayName(y) + ' ' + mdy(y)
    + '. Tomorrow is ' + dayName(t) + ' ' + mdy(t) + '. Then ' + tail.join(', ') + '. A weekday name with no date means the nearest one: '
    + '"Saturday" today means ' + (dayName(y) === 'Saturday' ? 'yesterday, ' + mdy(y) : 'the coming one') + '. Never guess a weekday from a date; read it here. '
    + 'Before you SAY a weekday with a date, or put a date in an act, find that weekday in this line and copy its date - "this Wednesday" is the Wednesday listed here, nothing else.';   // v1.46 (Sept 28 8:07 PM: 'Wednesday, October 1st' - Wednesday was the 30th; the list was right, she did not read it)
}
// v1.38 SPOKEN, NOT PRINTED: the office answers in record form ('2026-09-27 12:00 PM to 5:00 PM (ownerOverride:true if you mean it.)') and
// the voice read it as 'twenty twenty-six zero nine twenty-seven'. Dates become 'Sunday the 27th', windows become '12 to 5', the override
// hint becomes a question.
const MONTHS = ['January','February','March','April','May','June','July','August','September','October','November','December'];
function ordinal(n) { n = Number(n); const s = ['th','st','nd','rd'], v = n % 100; return n + (s[(v - 20) % 10] || s[v] || s[0]); }
function spokenDates(t) {
  return String(t || '')
    .replace(/\b(\d{4})-(\d{2})-(\d{2})\b/g, (m, y, mo, d) => { const dt = new Date(Number(y), Number(mo) - 1, Number(d), 12); return dayName(dt) + ' ' + MONTHS[Number(mo) - 1] + ' ' + ordinal(d); })
    .replace(/\b(\d{1,2}):00 ([AP]M) to (\d{1,2}):00 ([AP]M)\b/gi, (m, a, ap, b, bp) => a + (ap.toUpperCase() === bp.toUpperCase() ? '' : ' ' + ap) + ' to ' + b + ' ' + bp)
    .replace(/\b0?(\d{1,2}):00\s*[-\u2013]\s*0?(\d{1,2}):00\b/g, (m, a, b) => { const A = Number(a), B = Number(b); const h = x => (x === 12 ? '12' : (x > 12 ? String(x - 12) : String(x))); return h(A) + ' to ' + h(B) + (B >= 12 ? ' PM' : ' AM'); })   // v1.39 '08:00-12:00' -> '8 to 12 PM'
    .replace(/\s*\(ownerOverride:true if you mean it\.?\)/i, ' Say the word and I will override it.')
    .replace(/\s*\|\s*/g, '. ').replace(/(\d+) held\b/g, '$1 held')
    .replace(/\s*\u2014\s*/g, ', ').replace(/\s{2,}/g, ' ').trim();
}
// v1.38 ONE MOVE PER JOB ON THE OWNER LINE TOO: 'Mari Jo Handbury' came back as 'Marijo Handberry' and the plain key missed it.
// Names compare by letter bigrams; 0.6 or better is the same person on the same call.
function nameSim(a, b) {
  const g = x => { x = String(x || '').toLowerCase().replace(/[^a-z]/g, ''); const o = {}; for (let i = 0; i < x.length - 1; i++) o[x.slice(i, i + 2)] = (o[x.slice(i, i + 2)] || 0) + 1; return o; };
  const A = g(a), B = g(b); let hit = 0, na = 0, nb = 0;
  for (const k in A) { na += A[k]; if (B[k]) hit += Math.min(A[k], B[k]); } for (const k in B) nb += B[k];
  return (na + nb) ? (2 * hit) / (na + nb) : 1;   // v1.45 two blank names are the same (blank) name - a day-level act (sendConfirmations) carries no customer and was never deduped (sim, Sept 28 6:33 PM: texts sent twice)
}
const RX_ALREADY = /already (?:there|on)\b|nothing to move/i;

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
    actsRan: [],               // v1.17: what her hands did on this call (rides to GAS in the results)
    ownerActs: [], ownerOverride: false,   // v1.38 owner-line ledger + his standing 'I mean it' for this call
    turnSeq: 0, chain: Promise.resolve(),   // v1.45 one turn at a time per call (see handlePrompt)
    actCtl: null, stopSaid: false   // v1.46 the day-long act in the office, and the owner's 'stop' against it
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
// v1.50 NO ACT WORDS IN HER MOUTH (owner call Sept 30: she said 'Flash action. Search memory data. Q Charlie Thomas.' - an act written
// into the spoken reply and read aloud). Anything that looks like an act - a JSON object, a key:value pair naming an action, a code fence -
// is cut from what the voice speaks. Her words stay; the plumbing never reaches the caller's ear.
function speakable(t) {
  let x = String(t == null ? '' : t);
  if (!/[{}`"]|action|\bq\s*:/i.test(x)) return x;
  x = x.replace(/```[\s\S]*?(```|$)/g, ' ')
       .replace(/\{[^{}]*("action"|"data"|"q"\s*:)[^{}]*\}+/gi, ' ')
       .replace(/"?\baction"?\s*:\s*"?[a-z]+[A-Z]?\w*"?,?/g, ' ')
       .replace(/"?\bdata"?\s*:\s*\{?/g, ' ')
       .replace(/"q"\s*:\s*"[^"]*"/g, ' ')
       .replace(/"?\baction"?\s*:/g, ' ')
       .replace(/"[a-z]+[A-Z]\w*"\s*,?/g, ' ')
       .replace(/[{}]/g, ' ')
       .replace(/\s{2,}/g, ' ');
  return x;
}
function sendText(ws, token, last) {
  try { token = speakable(token); } catch (eSp) {}
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
// ---- v1.26 live picture on a call ------------------------------------------
const IMG_REF = /\b(look(ing)? at|see (this|that|it)|what(?:'|’)?s (this|that|it)|what is (this|that|it)|what am i (looking at|seeing)|i (just )?(sent|texted|sending|shot)|check (this|that|it) out|this (photo|picture|pic|bug|thing|one)|the (photo|picture|pic) i|show(ing)? (you|ya))\b/i;
async function pendingPhotoUrl(phone) {
  try {
    const u = GAS_URL + '?hook=glasspending&k=' + encodeURIComponent(WKEY) + '&phone=' + encodeURIComponent(phone || '');
    const ctl = new AbortController(); const tm = setTimeout(() => ctl.abort(), 8000);
    try {
      const r = await fetch(u, { method: 'POST', redirect: 'follow', signal: ctl.signal });
      const j = await r.json();
      return (j && j.has && j.url) ? String(j.url) : '';
    } finally { clearTimeout(tm); }
  } catch (e) { logErr('glass.pending', e); return ''; }
}
async function fetchTwilioMedia(url) {
  if (!TW_SID || !TW_TOKEN) return null;
  const ctl = new AbortController(); const tm = setTimeout(() => ctl.abort(), 12000);
  try {
    const r = await fetch(url, { headers: { authorization: 'Basic ' + Buffer.from(TW_SID + ':' + TW_TOKEN).toString('base64') }, redirect: 'follow', signal: ctl.signal });
    if (!r.ok) { logErr('glass.media', 'twilio media ' + r.status); return null; }
    const mt = (r.headers.get('content-type') || 'image/jpeg').split(';')[0];
    if (!/^image\//.test(mt)) return null;
    const buf = Buffer.from(await r.arrayBuffer());
    if (buf.length > 4800000) { logErr('glass.media', 'photo too big ' + buf.length); return null; }
    return { b64: buf.toString('base64'), mt };
  } catch (e) { logErr('glass.media', e); return null; }
  finally { clearTimeout(tm); }
}
async function maybeGlassPhoto(s, text) {
  try {
    if (!(s.callerPack && s.callerPack.owner === true)) return null;   // owner line only
    if (!IMG_REF.test(String(text || ''))) return null;
    let url = await pendingPhotoUrl(s.from);
    if (!url) { await new Promise(r => setTimeout(r, 3500)); url = await pendingPhotoUrl(s.from); }   // MMS can lag a few seconds
    if (!url) return null;
    const img = await fetchTwilioMedia(url);
    if (img) logInfo('glass photo pulled into call ' + s.callSid + ' (' + img.b64.length + ' b64)');
    return img;
  } catch (e) { logErr('glass.maybePhoto', e); return null; }
}
// v1.45 ONE TURN AT A TIME (aggie-sim, Sept 28 2:44 PM run: the owner's PIN turn was still waiting for his pack when
// 'move every work order on Wednesday to Thursday' arrived; both turns woke on the pack, both asked the brain, both ran
// moveDay - 'On it. On it. Moved 4 of 4... Moved 4 of 4...' - a DOUBLE WRITE. The same overlap on a customer line is
// 'Aggie? Hello?' while the office is still booking: a second brain turn with no record of the first.) Turns now run in
// sequence per call. An utterance that arrives while an older turn is still waiting for the pack makes that older turn
// stale - the newer one carries both sentences in the conversation. An older turn already past the brain (its act in the
// office) finishes and speaks first; the newer utterance is answered right after, with the result on record.
async function handlePrompt(s, voicePrompt) {
  // a new utterance always cancels a stale in-flight turn (barge-in via speech)
  if (s.ctl) { try { s.ctl.abort(); } catch (e) {} }
  if (s.actCtl && /\b(stop|hold on|hang on|wait|cancel|never mind|hold up|listen)\b/i.test(String(voicePrompt || ''))) {   // v1.46 'No. Stop.' during a day-long act: the wait ends now, she answers now
    s.stopSaid = true; try { s.actCtl.abort(); } catch (e) {} s.actCtl = null;
    try { sendText(s.ws, 'Stopping. I\'m listening.', true); } catch (e) {}
    logInfo('owner said stop during a long act on ' + s.callSid);
  }
  s.convo.push({ role: 'user', content: String(voicePrompt).slice(0, 500) });
  maxListen(s, voicePrompt);   // v1.16 shadow ears - logs only unless MAX_LIVE=1
  const seq = ++s.turnSeq;
  const run = () => runTurn(s, voicePrompt, seq).catch(e => logErr('runTurn', e));
  s.chain = s.chain.then(run, run);
  return s.chain;
}
async function runTurn(s, voicePrompt, seq) {
  if (seq !== s.turnSeq) { logInfo('turn ' + seq + ' skipped on ' + s.callSid + ' - a newer utterance carries it'); return; }   // v1.45 superseded while queued

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
      if (!s.callerPack && lastOwnerPack && (Date.now() - lastOwnerPack.at) < 45 * 60000) {   // v1.46 (Sept 28 7:54 PM: 'my assistant brain did not load - hang up and call me back' - the office took >16 s to build it; the one from minutes earlier was fine)
        s.callerPack = lastOwnerPack.pack; logErr('pack.owner.stale', 'owner pack not here in 16 s for ' + s.callSid + ' - using the one from ' + Math.round((Date.now() - lastOwnerPack.at) / 60000) + ' min ago');
        sendText(s.ws, 'Got you. Go ahead.', true);
      }
      if (!s.callerPack) {
        logErr('pack.owner', 'owner pack never landed for ' + s.callSid + ' — refusing to run the receptionist on the owner');
        s.endWhy = 'owner brain missing';
        sendText(s.ws, 'Chris, my assistant brain did not load on this call. Hang up and call me right back — it will be warm.', true);
        setTimeout(async () => { try { await twilioUpdateCall(s.callSid, '<Response><Hangup/></Response>'); } catch (e) { logErr('hangup', e); } }, 6000);
        return;
      }
    } else {
      // v1.32 NO DEAD AIR (Dawna Cao, Sept 25 7:30 AM: the office took >25s to hand over her file; she heard the greeting,
      // 'one second', then silence, and hung up at 21s). The first turn waits 3s for the caller file, then runs on the
      // generic brain right away; the file is swapped in on the next turn when it lands. No second wait, no filler line.
      // v1.49 THE HELLO DOES NOT WAIT (sim Sept 30: all 12 calls answered their first turn at 5.0 s - 3 s waiting for the caller
      // file that never came in time, then ~2 s of brain, and the 5 s filler 'Sure - let me take a look.' glued to the greeting).
      // When the caller's first words are just a hello ('Hello?', 'Hi, is anyone there?'), nothing in their file changes the
      // answer - the reply is the greeting - so the generic brain answers at once and the file swaps in for turn two. A first
      // turn that carries a real request ('this is Patty, I need to move my visit') still waits up to 3 s for the file.
      const bareHello = first && /^\W*(hello|hi|hey|yeah|yes|good (morning|afternoon|evening))\b[\w\s,'?!.-]{0,40}$/i.test(String(voicePrompt || '').trim()) && String(voicePrompt || '').trim().split(/\s+/).length <= 7 && !/this is|my name|calling|it'?s /i.test(String(voicePrompt || ''));
      const packWaitMs = first ? (bareHello ? 150 : 3000) : 800;
      try { await Promise.race([ s.packPromise, new Promise(res => setTimeout(res, packWaitMs)) ]); } catch (e) {}
      if (!s.callerPack && first && !bareHello) logErr('pack.late', 'caller pack not here after 3s for ' + s.callSid + ' — first turn runs generic, file swaps in when it lands');
    }
  }
  if (seq !== s.turnSeq) { logInfo('turn ' + seq + ' skipped after the pack wait on ' + s.callSid + ' - a newer utterance carries it'); return; }   // v1.45
  let pack = s.callerPack || genericPack;
  if (!pack) {
    // v1.1 COLD-START PATIENCE: a fresh boot compiles the generic brain in the
    // background. Say something human ONCE, then wait up to 8 more seconds
    // before ever giving up. Transfer is the last resort, not the first reflex.
    sendText(s.ws, 'One second while I pull that up for you.', true);
    for (let i = 0; i < 16 && !genericPack && !s.callerPack; i++) {
      await new Promise(res => setTimeout(res, 500));
    }
    pack = s.callerPack || genericPack || fallbackPack();   // v1.48 the built-in brain before any transfer
    if (!pack) {
      sendText(s.ws, 'Let me get you straight to the team.', true);
      return doTransfer(s, 'no brain after patience window');
    }
    if (pack.fallback) logErr('pack.fallback', 'call ' + s.callSid + ' is running on the built-in brain');
  }
  const ownerLaw = (s.callerPack && s.callerPack.owner === true)
    ? '\nOWNER BATCH LAW (v1.42): "move everything on <day> to <day>" is ONE act: moveDay {fromDate, newDate, window?, ownerOverride?}; "send confirmations to the rest / to the unconfirmed" is ONE act: sendConfirmations {date, names?, skip?}; "mark X confirmed" is updateWorkOrderStatus {client, date?, status:"Confirmed"} - you CAN change a status; "terminate service / close the account" is terminateService {client}. Reply "On it." and emit the act in the SAME turn; never do a batch one item at a time, never ask which jobs or whether to start, never say a thing must be done in the app when one of these acts does it. When he says yes to any batch, that yes covers every item - no per-item asking, no re-announcing a finished item, one short line per result. If the office reports nothing on a day he can see jobs on, your schedule is stale: say so and look it up (searchJobs) before contradicting him. "Will you / can you send confirmations to the unconfirmed?" is the order itself - run it, do not ask "want me to?" first.\n'
    : '';
  const sys = clockLine() + ownerLaw + '\n\n' + String(pack.sys).replace(/\{\{CALLER_ID\}\}/g, s.from || 'unknown');   // v1.38 the clock first · v1.39 the batch law

  const glassImg = await maybeGlassPhoto(s, voicePrompt);   // v1.26: did he just send a photo he's now asking about?
  const ctl = new AbortController();
  s.ctl = ctl;
  let spoke = false;
  let raw = '';
  const buffered = true;   // v1.29: every turn waits for the hands; 1b: missions too — they have no hands, so a claim on a mission call is cut, never spoken
  // v1.45 NO SILENT FIRST TURN (sim: 6-9 s from the caller's first sentence to her first word on every call - the brain's first
  // pass over a 35k-character prompt). A customer line that has heard nothing 4 s in gets one short human line; her answer follows.
  const isOwnerLine = !!(s.callerPack && s.callerPack.owner === true);
  const sinceHeard = Date.now() - Number((s.ws && s.ws._turnT0) || Date.now());   // the pack wait on a first turn already spent some of the caller's patience
  const lastHeard = String(voicePrompt || '').trim();
  const goodbye = lastHeard.split(/\s+/).length <= 12 && /\b(bye|goodbye|see you|take care)\b/i.test(lastHeard);   // v1.48 'Great, thank you so much Aggie, I appreciate it. Bye!' is nine words   // v1.45 no 'One sec.' before a goodbye · v1.46 only a real goodbye - 'Okay.' before a booking left 9 s of dead air (sim run 6)
  const filler = (!s.mid && !goodbye) ? setTimeout(() => { try { sendText(s.ws, isOwnerLine ? 'One second.' : (s.convo.length <= 1 ? 'Sure — let me take a look.' : 'One sec.'), true); } catch (e) {} }, Math.max(1200, 5000 - sinceHeard)) : null;   // v1.46 the owner line too (8 s of silence after the move order, sim run 6)   // 5 s from when the caller stopped talking: a normal cached turn answers in 2-4 s
  try {
    try {
      raw = await aiTurn(sys, s.convo, tok => { if (buffered) return; spoke = true; sendText(s.ws, tok, false); }, ctl.signal, glassImg);
    } catch (e1) {
      if (ctl.signal.aborted) return;
      // v1.45 ONE RETRY BEFORE ANY TRANSFER (sim: two of six calls went to 'I hit a snag on my end - one moment while I get Chris'
      // on a single failed brain call, and every turn after it repeated the line). A failed call is logged in full and tried once more.
      logErr('aiTurn', e1);
      await new Promise(r => setTimeout(r, 1200));
      raw = await aiTurn(sys, s.convo, tok => { if (buffered) return; spoke = true; sendText(s.ws, tok, false); }, ctl.signal, glassImg);
    }
  } catch (e) {
    if (ctl.signal.aborted) return;    // superseded by a newer utterance — say nothing
    logErr('aiTurn.retry', e);
    if (filler) clearTimeout(filler);
    if (isOwnerLine) { sendText(s.ws, 'Chris, my brain call failed twice just now - ' + String((e && e.message) || e).replace(/[{}"]/g, '').slice(0, 120) + '. Say that again in a second.', true); return; }
    sendText(s.ws, 'Let me get Chris on the line for you.', true);
    return doTransfer(s, 'AI turn failed twice: ' + String((e && e.message) || e).slice(0, 160));
  } finally { if (filler) clearTimeout(filler); if (s.ctl === ctl) s.ctl = null; }

  const d = parseTurn(raw);
  if (d && !d.reply && d.done) {   // v1.45 the brain says the call is over and has nothing left to say (sim: 'Sorry, say that one more time?' eight times after 'bye')
    s.done = true; s.endWhy = 'completed';
    sendText(s.ws, s.actsRan.some(a => a.ok) ? 'Thanks for calling Apex — bye now!' : '', true);
    setTimeout(async () => { try { await twilioUpdateCall(s.callSid, '<Response><Hangup/></Response>'); } catch (e) { logErr('hangup', e); } }, 4000);
    return;
  }
  if (!d || !d.reply) {
    logErr('parse', 'unparseable: ' + raw.slice(0, 160));
    if (!spoke) sendText(s.ws, 'Sorry, say that one more time for me?', true);
    else sendText(s.ws, '', true);
    return;
  }
  if (!buffered) {
    if (!spoke) sendText(s.ws, d.reply, true);   // extractor missed (odd formatting) — speak the parsed reply
    else sendText(s.ws, '', true);               // close the utterance
  }

  s.convo.push({ role: 'assistant', content: String(d.reply).slice(0, 500) });
  mergeLead(s.lead, d.lead);   // v1.17: merge BEFORE the hands, so a synthesized act sees this turn's extraction
  const isOwner = !!(s.callerPack && s.callerPack.owner === true);
  if (isOwner && (d.transfer || RX_TRANSFER_LINE.test(String(d.reply || '')))) {   // v1.33 the owner is never transferred to himself
    logInfo('owner-line transfer suppressed on ' + s.callSid + ': "' + String(d.reply).slice(0, 100) + '"');
    d.transfer = false; d.reply = stripTransferLine(d.reply) || 'Go ahead, sir — I\'m listening.';
    raw = String(raw || '').replace(RX_TRANSFER_LINE, '');
  }
  let spokenThisTurn = false;   // v1.29: buffered turns speak exactly once, below, after the office has answered
  if (!isOwner && !s.mid && d.commercial && !s.commercial) logInfo('handoff strip: turn marked commercial by the brain on ' + s.callSid + ' — ignored, the pack did not say so');   // v1.43
  if (!isOwner && !s.mid && !s.commercial) {   // v1.35 the hand-off rule · v1.43 the brain's own per-turn 'commercial' flag no longer switches it off (Patty, 10:15 AM: 'I'll have Chris confirm the time with you' went out)
    const h = stripHandoff(d.reply, !!((s.lead && s.lead.day) || s.actsRan.some(a => a.ok && /^(bookJob|rescheduleJob|confirmJob)$/.test(a.action))));   // v1.45
    if (h.cut) { logInfo('handoff removed on ' + s.callSid + ': "' + String(d.reply).slice(0, 120) + '" -> "' + h.text.slice(0, 80) + '"'); d.reply = h.text; s.convo[s.convo.length - 1].content = String(d.reply).slice(0, 500); }
  }
  // v1.9 THE OWNER LINE. When the pack said owner:true, the brain is AGI and a
  // turn may carry `act` — a chat-bubble action the owner just approved out
  // loud. Post it to GAS (hook=voiceact, owner-number checked there too),
  // speak the result, and remember it in the convo so the next turn knows.
  if (d.act && d.act.action && isOwner) {
    // v1.38 ONE MOVE PER JOB, HIS OVERRIDE STANDS FOR THE CALL
    if (d.act.data && typeof d.act.data === 'object') {
      const nm = String(d.act.data.client || d.act.data.name || d.act.data.clientName || (/^(moveDay|sendConfirmations)$/.test(d.act.action) ? ('day ' + String(d.act.data.fromDate || d.act.data.from || d.act.data.date || 'today')) : ''));   // v1.45 sendConfirmations keyed by its day too
      const target = String(d.act.data.newDate || d.act.data.date || '').slice(0, 10) + (d.act.action === 'setPrice' ? '|$' + String(d.act.data.price || '') : (d.act.action === 'updateWorkOrderStatus' ? '|' + String(d.act.data.status || '') : ''));   // v1.42 a new price or status is a new act
      const prior = s.ownerActs.find(x => x.ok && x.action === d.act.action && (!target || x.target === target) && nameSim(x.name, nm) >= 0.6 && !(d.act.data.ownerOverride && !x.override));   // v1.41 an override re-fire is a new act
      if (prior && /^(rescheduleJob|moveDay|voidJob|sendInvoice|chargeCard|tierSignup|createWorkOrder|setPrice|updateWorkOrderStatus|terminateService|sendConfirmations)$/.test(d.act.action)) {
        const line = nm.split(' ')[0] + ' is already done - ' + spokenDates(String(prior.said).split(/(?<=\.)\s/)[0]).replace(/^Moved\s+\S+\s+\S+\s*(\([^)]*\))?\s*/i, 'moved ').slice(0, 140) + ' Next?';   // v1.39 one short line, not her whole recap again
        logInfo('voiceact ' + d.act.action + ' skipped: already done for ' + nm + ' on ' + s.callSid);
        s.convo.push({ role: 'assistant', content: '[already done this call: ' + d.act.action + ' for ' + nm.slice(0, 40) + ' - ' + String(prior.said).slice(0, 120) + '. Do not repeat it; say so and move to the next one.]' });
        sendText(s.ws, line, true);   // v1.39 her re-narration is dropped entirely; the one line is the answer
        spokenThisTurn = true;
        d.act = null;
      } else if (d.act.data.ownerOverride === undefined && /^(rescheduleJob|createWorkOrder)$/.test(d.act.action)) {
        const lastUser = [...s.convo].reverse().find(m => m.role === 'user' && !/^\[/.test(String(m.content || '')));
        const yes = /\b(yes|yeah|yep|do it|mean it|override|go ahead|always mean it|i said so|just do it)\b/i.test(String((lastUser && lastUser.content) || ''));
        if (s.ownerOverride || (s.overrideAsked && yes)) { d.act.data.ownerOverride = true; s.ownerOverride = true; }
      }
    }
  }
  if (d.act && d.act.action && isOwner) {
    try {
      if (buffered && READ_ACTS[d.act.action] && d.reply) { sendText(s.ws, String(d.reply), true); spokenThisTurn = true; }   // v1.29 a read: her lead-in line, then the answer turn below
      let ledIn = false;   // v1.40 a long act: 'On it, sir.' goes out NOW, the office line follows when it lands
      if (buffered && LONG_ACTS[d.act.action]) { const lead = stripOwnerClaims(stripClaims(d.reply)) || 'On it.'; sendText(s.ws, lead, true); spokenThisTurn = true; ledIn = true; }
      const r = await postVoiceAct(s, d.act);
      // v1.10 A LOOKUP IS A THOUGHT, NOT A LINE. v1.23: so is EVERY read. The result goes back into the
      // conversation as a note; she takes another turn with it in hand and ANSWERS — the owner hears the
      // answer, never the rows. One chained read (search -> lookup) is allowed.
      if (READ_ACTS[d.act.action]) {
        let rr = r, act = d.act, depth = 0, d2 = null, spoke2 = false;
        for (;;) {
          const body = String((rr && (rr.result || rr.error)) || 'no answer');
          const note = '[RESULT of ' + act.action + ' — FOR YOU, NOT TO READ ALOUD. Answer his question from it in one or two spoken sentences: '
            + 'a count is a number ("fourteen"), a yes/no is yes or no plus the one fact that proves it, a who/when is the name or the day. '
            + 'Never read rows, lists, ids, phone numbers, timestamps or quoted text unless he asks you to read it. If it does not answer him, say so plainly.]\n'
            + body.slice(0, 6000);
          s.convo.push({ role: 'user', content: note });
          let raw2 = '';
          spoke2 = false;
          try { raw2 = await aiTurn(sys, s.convo, tok => { spoke2 = true; sendText(s.ws, tok, false); }, null); } catch (e) { logErr('readTurn', e); }
          d2 = parseTurn(raw2);
          s.convo[s.convo.length - 1].content = note.slice(0, 1500);   /* answered: keep the gist, not 6k of rows, for later turns */
          if (d2 && d2.act && d2.act.action && READ_ACTS[d2.act.action] && depth < 1) {
            depth++; act = d2.act;
            if (d2.reply) { sendText(s.ws, '', true); s.convo.push({ role: 'assistant', content: String(d2.reply).slice(0, 500) }); }
            try { rr = await postVoiceAct(s, act); } catch (e) { logErr('voiceact.chain', e); rr = { ok: false, error: 'the office did not answer' }; }
            continue;
          }
          break;
        }
        if (d2 && d2.reply) { sendText(s.ws, spoke2 ? '' : String(d2.reply), true); s.convo.push({ role: 'assistant', content: String(d2.reply).slice(0, 500) }); }   /* extractor missed (plain text) - speak the parsed reply, as the main path does */
        else sendText(s.ws, 'I pulled it up but lost my train of thought — ask me that once more?', true);
        return;
      }
      const rawRes = String((r && (r.result || r.error)) || '').replace(/[\u2714\u2716\u2717\u23f3\ud83d\udcc5\u260e]/g, '').trim();
      // v1.38 'No X job on <day>. Their next job is <target>, already there; nothing to move' IS the job done - by us, earlier this call.
      const refused = /^\s*[\u2716\u2717]/.test(String((r && r.result) || ''));   // v1.41 the office's own ✖/✗ is a refusal even when its ok flag says true (Sept 27 9:16 PM: 'Moved none of 10' spoken as done)
      const alreadyThere = !!(r && (!r.ok || refused) && RX_ALREADY.test(rawRes) && /^(rescheduleJob|voidJob)$/.test(d.act.action));
      const ok = (!!(r && r.ok) && !refused) || alreadyThere;
      let said = ok ? spokenDates(rawRes || 'Done.').slice(0, 240) : (refused ? spokenDates(rawRes).slice(0, 300) : ('That did not go through: ' + spokenDates(rawRes || 'no answer from the office').slice(0, 160)));
      // v1.51 AN ERROR PAGE IS NOT A 'NO' (owner call Oct 1 8:12 AM: two moveDay retries came back as a Google HTML page and she said
      // 'That did not go through. Unreadable.' - while three of the six jobs HAD moved at 8:11). An HTML answer means the office's reply
      // was lost, not that nothing ran: she says so in plain words and the conversation note says UNKNOWN, look it up first.
      const pageBack = !ok && /^unreadable/i.test(String((r && r.error) || ''));
      if (pageBack) said = 'The office sent back an error page instead of an answer, so I can\'t tell you yet whether that went through. Ask me what\'s on the day and I\'ll read it to you before I touch it again.';
      if (alreadyThere) { const mm = /Their next job is ([^()]+?)\s*(?:\(|\u2014|,|$)/.exec(rawRes); const who = String((d.act.data && (d.act.data.client || d.act.data.name || d.act.data.clientName)) || 'That job'); said = who + ' is already on ' + spokenDates(mm ? mm[1].trim() : 'the new day') + '. Done.'; }
      if (/ownerOverride/i.test(rawRes)) s.overrideAsked = true;   // v1.38 the office asked; his next yes is the override, for the rest of the call
      const nm0 = String((d.act.data && (d.act.data.client || d.act.data.name || d.act.data.clientName || (/^(moveDay|sendConfirmations)$/.test(d.act.action) ? ('day ' + String(d.act.data.fromDate || d.act.data.from || d.act.data.date || 'today')) : ''))) || '');   // v1.45
      s.ownerActs.push({ action: d.act.action, name: nm0, target: String((d.act.data && (d.act.data.newDate || d.act.data.date)) || '').slice(0, 10) + (d.act.action === 'setPrice' ? '|$' + String((d.act.data && d.act.data.price) || '') : (d.act.action === 'updateWorkOrderStatus' ? '|' + String((d.act.data && d.act.data.status) || '') : '')), ok, said: said.slice(0, 200), override: !!(d.act.data && d.act.data.ownerOverride) });
      if (ok && d.act.data && d.act.data.ownerOverride) s.ownerOverride = true;
      // v1.29: on a buffered turn her own words come first with any 'done' claim cut out, then the office's verdict
      if (ledIn) sendText(s.ws, said, true);   // v1.40 she already said 'On it'; now just the result
      else if (buffered) { const lead = ok ? stripClaims(d.reply) : stripOwnerClaims(stripClaims(d.reply)); sendText(s.ws, (lead ? (lead + ' ') : '') + said, true); spokenThisTurn = true; }   // v1.33 no 'Done' over a refusal
      else sendText(s.ws, said, true);
      s.convo.push({ role: 'assistant', content: '[' + (ok ? 'ran ' : (pageBack ? 'UNKNOWN (error page back - it may have run; look the day or job up before running it again) ' : 'FAILED ')) + d.act.action + ': ' + (pageBack ? 'no readable answer' : rawRes.slice(0, 200)) + (alreadyThere ? ' (that is the move we made earlier this call - done, move on)' : '') + ']' });
      logInfo('voiceact ' + d.act.action + ' ' + (ok ? 'ok' : 'FAIL') + ' for ' + nm0 + ' on ' + s.callSid + ': ' + rawRes.slice(0, 120));
    } catch (e) {
      // v1.38 the failure is spoken ONCE, with her own 'doing it now' words cut, and written into the conversation so she knows.
      // (Before: the fallback line, then her full 'Starting with Charlie Thomas...' reply on top of it, and no note - so she
      // stood by 'waiting on the system' for three turns.)
      logErr('voiceact', e);
      const why = String((e && e.message) || e).slice(0, 120);
      const maybe = /MAY HAVE COMPLETED|STILL RUNNING|STOPPED AT HIS WORD/.test(why);   // v1.46 a long act that outran the clock is unknown, never 'did not run'
      s.convo.push({ role: 'assistant', content: maybe
        ? '[UNKNOWN ' + d.act.action + ': the office was still working when the line timed out - it may have gone through. Do NOT run it again blind; look it up first (lookup / searchJobs / searchInbox) and tell him what you find.]'
        : '[FAILED ' + d.act.action + ': the office did not answer (' + why + '). It did NOT run as far as you know. Tell him plainly; offer to try once more.]' });
      const line = /STOPPED AT HIS WORD/.test(why) ? 'The office may have moved some of them already - say the day and I will read you what is on it.'
        : (/STILL RUNNING/.test(why) ? 'The office is still working through that day. Give it a minute, then ask me what is on the new day and I will read it back.'
        : (maybe ? 'The office was still working on that when it timed out. Let me check whether it went through before I touch it again.' : 'The office did not answer on that one. Want me to try it again?'));
      if (buffered) sendText(s.ws, gateSpeak(stripOwnerClaims(d.reply), false, line), true); else sendText(s.ws, line, true);
      spokenThisTurn = true;
    }
  } else if (!isOwner) {   // v1.30 MISSIONS HAVE HANDS (owner, Sept 25 5:02 AM: 'Fix both those') — an outbound call books/moves/cancels/signs up through the same office door as an inbound one
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
    // v1.37 ONE ACT PER JOB PER CALL (owner line, Sept 26 3:55 PM: 'move every job to Sunday'. Each move landed, then her own
    // narration 'Moved Charlie Thomas ... to Sunday' matched RX_SAID_MOVED and SYNTHESIZED the same rescheduleJob again -> the office
    // said 'No Charlie Thomas job on 9/26' (already moved) -> she believed it failed, told the owner 2 of 8, and stopped. Three jobs
    // had moved; she never touched the other four.) A synthesized act never repeats a success; a real re-emit of the same
    // move/cancel/confirm/book for the same name and day is answered from the earlier success instead of running again.
    if (act) {
      const _k = actKey(act);
      const prior = s.actsRan.find(a => a.ok && a.key === _k);
      if (prior && (synthesized || /^(rescheduleJob|cancelJob|confirmJob|bookJob)$/.test(act.action))) {
        s.convo.push({ role: 'assistant', content: '[already done this call: ' + act.action + ' for ' + String((act.data && act.data.name) || '').slice(0, 40) + ' succeeded earlier' + (prior.woId ? ' (' + prior.woId + ')' : '') + ' — do not repeat it; move on to the next one]' });
        logInfo('voicebook ' + act.action + (synthesized ? ' (synth)' : '') + ' skipped: already done for ' + _k + ' on ' + s.callSid);
        act = null;
      }
    }
    if (act && act.action === 'tierSignup' && s.actsRan.some(a => a.action === 'tierSignup' && a.ok)) act = null;   // v1.30 one sign-up per call
    if (d.act && d.act.action && !CUSTOMER_ACTS[d.act.action]) logErr('voicebook.refused', 'non-customer act ' + d.act.action + ' on ' + s.callSid + ' — ignored');
    if (act) {
      act.data = act.data || {};
      act.data.phone = act.data.phone || s.from;
      // v1.19 THE ACT INHERITS THE LEAD (owner's test call booked a job with no name -> no customer, no lead):
      // whatever she already extracted across the call rides with the act when the act itself left it blank.
      try { const L = s.lead || {}; ['name','address','email','service','pest','day','window'].forEach(k => { if (!act.data[k] && L[k]) act.data[k] = L[k]; }); if (!act.data.service && act.data.pest) act.data.service = act.data.pest; } catch (e) {}
      act.data.promised = act.data.promised || String(d.reply).slice(0, 240);
      // v1.34 THE AGREEING WORDS, NOT THE LAST WORDS (Amanda Thibault, Sept 25: 'Friday the 2nd in the morning' four turns back; her
      // last words at act time were 'Nope' / 'Thank you' -> held twice). For bookJob / rescheduleJob / confirmJob the caller's
      // agreement is the most recent caller turn that carries a yes OR a day / window / date, looked back up to 8 turns.
      if (!act.data.callerSaid || /^(bookJob|rescheduleJob|confirmJob)$/.test(act.action)) {
        const RX_AGREE = /\b(yes|yeah|yep|yup|sure|ok(?:ay)?|that works|sounds good|perfect|book (?:me|it|that)|let(?:'|’)?s do (?:it|that)|go ahead|please do|sign me up|put me down|mon|tue|wed|thu|fri|sat|sun|monday|tuesday|wednesday|thursday|friday|saturday|sunday|morning|afternoon|anytime|tomorrow|the \d{1,2}(?:st|nd|rd|th))\b|\b\d{1,2}\/\d{1,2}\b/i;
        let picked = '', last = '', seen = 0;
        for (let i = s.convo.length - 1; i >= 0 && seen < 8; i--) {
          if (s.convo[i].role !== 'user' || /^\[/.test(String(s.convo[i].content || ''))) continue;
          const u = String(s.convo[i].content || ''); seen++;
          if (!last) last = u;
          if (RX_AGREE.test(u)) { picked = u; break; }
        }
        act.data.callerSaid = (picked || act.data.callerSaid || last).slice(0, 200);
      }
      try {
        // v1.43 NO DEAD AIR (Patty Claremont, Sept 28 10:06 AM: 'Shall I go ahead and move your—' then ~40 s of nothing while the office
        // worked; she said 'Hello? Aggie?' six times and hung up). A buffered turn holds her words until the office answers; the caller
        // hears the wait. Now the line speaks at 3 s and again at 10 s, and her answer follows when it lands.
        const hb = [];
        if (buffered) { hb.push(setTimeout(() => { try { sendText(s.ws, 'One moment while I check on that.', true); } catch (e) {} }, 3000)); hb.push(setTimeout(() => { try { sendText(s.ws, 'Still with you — just a few more seconds.', true); } catch (e) {} }, 10000)); }
        let r; try { r = await postVoiceBook(s, act); } finally { hb.forEach(clearTimeout); }
        let line = String((r && r.say) || FALLBACK_SAY).slice(0, 240);
        if (!s.commercial) { const hs = stripHandoff(line, true); if (hs.cut) { logInfo('handoff removed from office line on ' + s.callSid + ': "' + line.slice(0, 80) + '"'); line = hs.text || FALLBACK_SAY; } }   // v1.43 the office's own fallback table is not exempt
        const ok = !!(r && r.ok);
        s.actsRan.push({ action: act.action, ok, woId: (r && r.woId) || '', synthesized, error: (r && r.error) || '', key: actKey(act) });   // v1.37 key
        if (buffered) {
          // v1.29 THE GATE: the office answered BEFORE she spoke. ok -> her words stand (they came true; add the office line only when
          // she did not already say it herself). not ok -> every claim sentence is cut and the office's honest line is spoken instead.
          let say;
          if (ok) say = (r && r.recap) ? String(d.reply) : (synthesized ? String(d.reply) : (String(d.reply) + (line && !saidClaims(d.reply) ? (' ' + line) : '')));
          else say = gateSpeak(d.reply, false, line);
          if (ok && !claimsBacked(s, say)) { const kept = stripUnbacked(s, say); logInfo('claim cut after ' + act.action + ' on ' + s.callSid + ': "' + say.slice(0, 100) + '" -> "' + kept.slice(0, 80) + '"'); say = kept || line || FALLBACK_SAY; }   // v1.45 the act that ran backs ITS claim only; a booking claim on a note turn is still cut
          say = tidyPromises(say, ok ? String((r && r.window) || '').toLowerCase() : '');   // v1.31 no clock on a callback; the window, not a clock, on a window job
          sendText(s.ws, say.slice(0, 600), true); spokenThisTurn = true;
          if (!ok) logInfo('gate-held ' + act.action + ' on ' + s.callSid + ': "' + String(d.reply).slice(0, 100) + '" -> "' + line.slice(0, 80) + '"');
        } else {
          // ok: only speak the office's line when her own reply did not already say it (avoid "You're set. You're on the books.")
          // not ok: ALWAYS speak the fallback — her words promised something the record could not hold.
          if ((!ok || synthesized) && line && !(r && r.recap)) sendText(s.ws, line, true);   // v1.20: a recap answer carries no line to speak
        }
        s.convo.push({ role: 'assistant', content: '[' + (ok ? 'ran ' : 'FAILED ') + act.action + (synthesized ? ' (from my own words)' : '') + ': ' + (ok ? line : String((r && r.error) || 'no answer')).slice(0, 200) + ']' });
        logInfo('voicebook ' + act.action + (synthesized ? ' (synth)' : '') + ' ' + (ok ? 'ok ' + ((r && r.woId) || '') : 'FAIL ' + ((r && r.error) || '')) + ' on ' + s.callSid);
      } catch (e) { logErr('voicebook', e); sendText(s.ws, buffered ? gateSpeak(d.reply, false, FALLBACK_SAY) : FALLBACK_SAY, true); spokenThisTurn = true; }
    } else if (buffered && saidClaims(d.reply) && !claimsBacked(s, d.reply)) {
      // v1.29 no act could be built (no day, no window, nothing to move) yet her words claim a record changed: cut the claim, say the honest line
      // v1.45 unless the record already landed this call (sim: every 'you're all set for Tuesday' recap AFTER a good booking was cut and
      // replaced with 'One moment while I check on that.' - five times in one call; the goodbye came out as a filler)
      const say = tidyPromises(gateSpeak(d.reply, false, FALLBACK_SAY), '');
      sendText(s.ws, say.slice(0, 600), true); spokenThisTurn = true;
      logInfo('gate-held (no act) on ' + s.callSid + ': "' + String(d.reply).slice(0, 120) + '"');
      s.convo.push({ role: 'assistant', content: '[GATE: I claimed something was booked/moved/canceled/noted but no act could be built — the caller heard: ' + say.slice(0, 160) + ']' });
    }
  }
  if (buffered && !spokenThisTurn) sendText(s.ws, isOwner ? String(d.reply) : tidyPromises(d.reply, ''), true);   // v1.29 nothing to check on this turn — speak her words (v1.31: minus any callback clock)
  if (d.flagOwner) s.flag = true;
  if (d.commercial && (s.callerPack && s.callerPack.commercial)) s.commercial = true;   // v1.45 the 1.43 rule made whole: only a pack that says commercial latches it (this line was still latching the brain's own flag for every later turn)
  if (d.tierOffered) s.tierOffered = true;
  if (d.tierTaken) s.tierTaken = String(d.tierTaken);
  if (d.sched && d.sched.action) s.sched = d.sched;

  if (d.transfer && !isOwner) return doTransfer(s, 'caller asked');
  // v1.12 HER MOUTH BINDS HER PLUMBING (owner, Sept 12: caller asked for a
  // person, she said "One moment while I try Chris for you" — and the
  // transfer flag never came, so the socket stayed open, the caller sat in
  // silence and hung up; zero calls reached his phone, a $350 quote walked).
  // If she TELLS the caller she is getting Chris, that IS the transfer,
  // JSON flag or not — the words she speaks to a customer are commitments.
  if (!isOwner && /\b(one moment|hold on|hang on|just a (second|moment|sec))?[^.]*\b(try|get|grab|connect you (?:to|with)|transfer(?:ring)? you to|put you through to)\s+(chris|him)\b/i.test(String(raw||''))) {   // v1.33 never on the owner line
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
  // v1.28 NEVER TRANSFER A CALL TO THE PHONE THAT IS CALLING. The owner on his own cell can only ever land in his own
  // voicemail. Say so, stay on the line, and log it - whatever the reason for the transfer was.
  try {
    var _from10 = String(s.from || '').replace(/\D/g, '').slice(-10), _to10 = String(CHRIS_CELL || '').replace(/\D/g, '').slice(-10);
    if (_from10 && _from10 === _to10) {
      logErr('transfer.self', 'refused: caller IS the transfer target (' + why + ') ' + s.callSid);
      s.convo.push({ role: 'assistant', content: '[transfer refused: the caller is Chris himself - ' + why + ']' });
      sendText(s.ws, 'Chris, I am not going to transfer you to your own phone. Something tripped on my side - go ahead, I am still here.', true);
      return;
    }
  } catch (e) { logErr('transfer.selfcheck', e); }
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


// ---- v1.24 THE GLASSES DOOR ------------------------------------------------
const ELEVEN_KEY = process.env.ELEVENLABS_API_KEY || '';
let glassPack = null, glassPackAt = 0, glassUsedAt = 0, glassPackP = null;
const glass = { convo: [], at: 0, turns: 0, last: null };
function glassPackFetch() {
  if (glassPackP) return glassPackP;
  glassPackP = (async () => {
    const ctl = new AbortController(); const tm = setTimeout(() => ctl.abort(), 30000);
    try {
      const r = await fetch(GAS_URL + '?hook=brainpack&glass=1&k=' + encodeURIComponent(WKEY), { signal: ctl.signal, redirect: 'follow' });
      const j = await r.json();
      if (!j || !j.ok || !j.sys) throw new Error('bad glass pack: ' + JSON.stringify(j).slice(0, 120));
      glassPack = j; glassPackAt = Date.now();
      logInfo('glass pack refreshed (v' + j.v + ', ' + j.sys.length + ' chars)');
      return j;
    } catch (e) { logErr('glass.pack', e); return glassPack; }
    finally { clearTimeout(tm); glassPackP = null; }
  })();
  return glassPackP;
}
async function glassPackGet() {
  if (glassPack && Date.now() - glassPackAt < 4 * 60 * 1000) return glassPack;
  if (glassPack) { glassPackFetch(); return glassPack; }      // stale is fine - refresh behind him
  return await glassPackFetch();
}
// while the glasses are in use (last 30 min), keep the brain warm like the phone's
setInterval(() => { if (glassUsedAt && Date.now() - glassUsedAt < 30 * 60 * 1000) glassPackFetch(); }, 4 * 60 * 1000);

function glassVoiceMode() {
  const v = glassPack && glassPack.voice;
  if (ELEVEN_KEY && v && v.id) return 'phone-voice';
  if (MEM_SAY_BASE || (glassPack && glassPack.sayBase)) return 'memory-voice';
  return glassPack ? 'none' : 'unknown until first use';
}
const ELEVEN_MODELS = { turbo_v2_5: 'eleven_turbo_v2_5', flash_v2_5: 'eleven_flash_v2_5', turbo_v2: 'eleven_turbo_v2', flash_v2: 'eleven_flash_v2', multilingual_v2: 'eleven_multilingual_v2' };
async function glassVoice(text) {
  const t = String(text || '').replace(/\s+/g, ' ').trim().slice(0, 600);
  if (!t) return null;
  const v = glassPack && glassPack.voice;
  const ctl = new AbortController(); const tm = setTimeout(() => ctl.abort(), 12000);
  try {
    if (ELEVEN_KEY && v && v.id) {
      const vs = { stability: v.stability != null ? v.stability : 0.5, similarity_boost: v.similarity != null ? v.similarity : 0.75 };
      if (v.speed) vs.speed = v.speed;
      const r = await fetch('https://api.elevenlabs.io/v1/text-to-speech/' + encodeURIComponent(v.id) + '?output_format=mp3_44100_64', {
        method: 'POST', signal: ctl.signal,
        headers: { 'xi-api-key': ELEVEN_KEY, 'content-type': 'application/json', accept: 'audio/mpeg' },
        body: JSON.stringify({ text: t, model_id: ELEVEN_MODELS[v.model] || 'eleven_flash_v2_5', voice_settings: vs })
      });
      if (r.ok) return Buffer.from(await r.arrayBuffer());
      logErr('glass.voice', 'elevenlabs ' + r.status + ': ' + (await r.text()).slice(0, 160));
    }
    const base = MEM_SAY_BASE || (glassPack && glassPack.sayBase) || '';
    if (base) {
      const r = await fetch(base + (base.includes('?') ? '&' : '?') + 'text=' + encodeURIComponent(t), { signal: ctl.signal });
      if (r.ok && /audio/i.test(r.headers.get('content-type') || '')) return Buffer.from(await r.arrayBuffer());
      logErr('glass.voice', 'memory /say ' + r.status + ' ' + (r.headers.get('content-type') || ''));
    }
  } catch (e) { logErr('glass.voice', e); }
  finally { clearTimeout(tm); }
  return null;
}
async function postGlassAct(act, said) {
  const owner = (glassPack && glassPack.ownerPhone) || (genericPack && genericPack.owner) || CHRIS_CELL;
  return postVoiceAct({ callSid: 'glass', from: owner, glass: true, said }, act);
}
function postGlassLog(q, say, photo, ms) {
  const ctl = new AbortController(); const tm = setTimeout(() => ctl.abort(), 30000);
  fetch(GAS_URL + '?hook=glasslog&k=' + encodeURIComponent(WKEY), { method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ q, say, photo: !!photo, ms }), redirect: 'follow', signal: ctl.signal })
    .catch(e => logErr('glass.log', e)).finally(() => clearTimeout(tm));
}
async function glassTurn(q, img, mt, auto) {
  const t0 = Date.now();
  glassUsedAt = t0;
  if (t0 - glass.at > 15 * 60 * 1000) glass.convo = [];   // a new conversation after 15 quiet minutes
  glass.at = t0;
  const pack = await glassPackGet();
  if (!pack) return { ok: false, say: 'My brain did not load. Try me again in a few seconds.', brainMs: Date.now() - t0 };
  const sys = clockLine() + '\n\n' + String(pack.sys);   // v1.38 the clock first
  const text = String(q || '').trim().slice(0, 1500) || 'What am I looking at?';
  const first = img ? [{ type: 'image', source: { type: 'base64', media_type: mt || 'image/jpeg', data: img } },
                       { type: 'text', text: text + (auto ? '\n(A photo of what he is looking at came along automatically with his tap. Use it ONLY if his question is about something he can see; otherwise ignore it and never mention it.)' : '\n(The photo is what he is looking at right now.)') }] : text;
  const convo = glass.convo.slice(-12);
  while (convo.length && convo[0].role !== 'user') convo.shift();
  convo.push({ role: 'user', content: first });
  const keep = [{ role: 'user', content: text + (img ? ' [with a photo]' : '') }];
  let raw = await aiTurn(sys, convo, () => {}, null);
  let d = parseTurn(raw);
  let say = (d && d.reply) ? String(d.reply) : '';
  if (d && d.act && d.act.action) {
    let act = d.act;
    try {
      let r = await postGlassAct(act, text);
      if (READ_ACTS[act.action]) {
        for (let depth = 0; depth < 2; depth++) {
          const body = String((r && (r.result || r.error)) || 'no answer');
          const note = '[RESULT of ' + act.action + ' — FOR YOU, NOT TO READ ALOUD. Answer his question from it in one to three short spoken sentences: '
            + 'a count is a number, a yes/no is yes or no plus the one fact that proves it, a who/when is the name or the day. Never read rows, lists, ids, phone numbers or timestamps. If it does not answer him, say so plainly.]\n'
            + body.slice(0, 6000);
          convo.push({ role: 'assistant', content: String(say || 'Pulling that up.').slice(0, 500) });
          convo.push({ role: 'user', content: note });
          keep.push({ role: 'assistant', content: String(say || 'Pulling that up.').slice(0, 500) }, { role: 'user', content: note.slice(0, 1500) });
          const d2 = parseTurn(await aiTurn(sys, convo, () => {}, null));
          say = (d2 && d2.reply) ? String(d2.reply) : 'I pulled it up but lost my train of thought. Ask me that once more?';
          if (depth === 0 && d2 && d2.act && d2.act.action && READ_ACTS[d2.act.action]) { act = d2.act; r = await postGlassAct(act, text); continue; }
          break;
        }
      } else {
        const res = String((r && (r.result || r.say)) || '').replace(/[✔✖✗⏳📅☎]/g, '').trim();
        say = (r && r.ok) ? (res.slice(0, 240) || 'Done.') : (r && r.held) ? res : ('That did not go through: ' + String((r && r.error) || 'no answer from the office').slice(0, 120));
      }
    } catch (e) { logErr('glass.act', e); say = 'That did not go through on my end.'; }
  }
  if (!say) say = 'Say that one more time for me?';
  keep.push({ role: 'assistant', content: say.slice(0, 500) });
  glass.convo = glass.convo.concat(keep).slice(-16);
  return { ok: true, say, brainMs: Date.now() - t0 };
}
function glassReadBody(req, limit) {
  return new Promise((resolve, reject) => {
    const chunks = []; let n = 0;
    req.on('data', c => { n += c.length; if (n > limit) { reject(new Error('too big')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}
async function glassHttp(req, res, u) {
  const J = (code, obj) => { res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(obj)); };
  if (!WKEY || u.searchParams.get('k') !== WKEY) return J(403, { ok: false, say: 'The key in Aggie settings is wrong.' });
  if (u.pathname === '/glass/warm') { glassUsedAt = Date.now(); const p = glassPackGet(); if (!glassPack) await Promise.race([p, new Promise(r => setTimeout(r, 20000))]); return J(200, { ok: !!glassPack, gateway: GW_VERSION, voice: glassVoiceMode() }); }
  if (u.pathname === '/glass/say') {
    if (!glassPack) await glassPackGet();
    const buf = await glassVoice(u.searchParams.get('text') || '');
    if (!buf) return J(503, { ok: false });
    res.writeHead(200, { 'content-type': 'audio/mpeg', 'cache-control': 'private, max-age=86400' }); return res.end(buf);
  }
  if (u.pathname === '/glass' && req.method === 'POST') {
    const t0 = Date.now();
    let b = {};
    try { b = JSON.parse(await glassReadBody(req, 9 * 1024 * 1024) || '{}') || {}; } catch (e) { return J(400, { ok: false, say: 'That photo was too big to send.' }); }
    const img = typeof b.img === 'string' && b.img.length > 100 && b.img.length < 6500000 ? b.img : '';
    let out;
    try { out = await glassTurn(b.q, img, String(b.mt || 'image/jpeg'), b.auto === true); }
    catch (e) { logErr('glass.turn', e); out = { ok: false, say: 'I hit a snag on my end. Ask me again?', brainMs: Date.now() - t0 }; }
    const tv = Date.now();
    const audio = await glassVoice(out.say);
    const ms = { brainMs: out.brainMs, voiceMs: Date.now() - tv, totalMs: Date.now() - t0 };
    glass.turns++; glass.last = Object.assign({ at: new Date().toISOString(), photo: !!img }, ms);
    logInfo('glass turn ' + JSON.stringify(ms));
    J(200, { ok: out.ok, say: out.say, audio: audio ? audio.toString('base64') : '', ms });
    if (out.ok) postGlassLog(String(b.q || ''), out.say, !!img, ms);
    return;
  }
  return J(404, { ok: false });
}

// ---- HTTP (health + keep-warm target) ---------------------------------------
const server = http.createServer((req, res) => {
  if (req.url && req.url.startsWith('/glass')) {   // v1.24 the glasses door
    const gu = new URL(req.url, 'http://x');
    glassHttp(req, res, gu).catch(e => { logErr('glass.http', e); try { res.writeHead(500, { 'content-type': 'application/json' }); res.end('{"ok":false,"say":"I hit a snag on my end."}'); } catch (e2) {} });
    return;
  }
  if (req.url && req.url.startsWith('/health')) {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({
      ok: true, gateway: GW_VERSION, up: Math.round(process.uptime()),
      brainAgeSec: genericPack ? Math.round((Date.now() - genericAt) / 1000) : null,
      brainVersion: genericPack ? genericPack.v : null,
      model: MODEL, callsHandled, liveCalls: sessions.size,
      packs: (function () { const l = packTimes.map(x => x.land).sort((a, b) => a - b); const q = f => l.length ? l[Math.min(l.length - 1, Math.floor(l.length * f))] : null; return { n: l.length, landP50: q(0.5), landP90: q(0.9), under3s: l.filter(x => x < 3000).length, last: packTimes.slice(-6) }; })(),   // v1.49
      promptCache: lastUsage || 'no turns yet since restart',
      hands: acts.slice(-8),   // v1.17: the last voicebook round-trips — proof her hands work, or exactly why not
      turns: turns.slice(-30), turnLag: (function(){ const f = turns.map(t => t.firstMs).filter(x => x > 0); if (!f.length) return null; f.sort((a,b)=>a-b); return { n: f.length, p50: f[Math.floor(f.length/2)], p90: f[Math.floor(f.length*0.9)], max: f[f.length-1], interrupts: turns.reduce((a,t)=>a+(t.interrupts||0),0), silentTurns: turns.filter(t=>!t.tokens).length }; })(),   // v1.22: the lag she feels, in numbers
      glass: { voice: glassVoiceMode(), brainAgeSec: glassPack ? Math.round((Date.now() - glassPackAt) / 1000) : null, turns: glass.turns, last: glass.last },   // v1.24
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
      s._packT0 = Date.now();   // v1.49 how long the caller file takes to land
      s.packPromise = (s.mid ? fetchPack('', 25000, s.mid) : fetchPack(s.from, 25000))
        .catch(e => { logErr('pack.caller', e); return s.mid ? null : fetchPack(s.from, 25000).catch(e2 => { logErr('pack.caller.retry', e2); return null; }); })   // v1.32 one retry - the kit caches the file per phone now, so the second ask is usually instant
        .then(p => { if (p) { s.callerPack = p; try { packStat(Date.now() - s._packT0, p.buildMs); } catch (e) {} logInfo('caller pack landed for ' + s.callSid + ' in ' + (Date.now() - s._packT0) + ' ms (office build ' + (p.buildMs != null ? p.buildMs + ' ms' : '?') + ')'); if (p.owner === true) lastOwnerPack = { pack: p, at: Date.now() }; } return p; });   // v1.46 remember the owner's
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
  ws.on('close', (code, reason) => {
    sessions.delete(ws);
    // v1.50 THE OWNER NEVER RINGS HIMSELF (owner call Sept 30 4:23-4:38 PM: fifteen minutes in, the relay session ended, Twilio moved on to
    // the next verb in the answer script - 'One moment while I try Chris for you' + a Dial to Chris's cell - and he got his own voicemail).
    // Every close is logged with its code so we learn WHY a session ends; an owner call that closes without being finished is hung up by
    // REST at once, so the fall-through Dial never rings his phone.
    try {
      const own = !!(s.callerPack && s.callerPack.owner === true);
      if (!s.done) logErr('relay.closed', 'session closed code ' + code + (reason && String(reason) ? (' "' + String(reason).slice(0, 120) + '"') : '') + ' on ' + (s.callSid || '?') + (own ? ' (OWNER line)' : '') + ' after ' + Math.round((Date.now() - (s.startedAt || Date.now())) / 1000) + ' s');
      if (own && !s.done && s.callSid) twilioUpdateCall(s.callSid, '<Response><Say>Chris, the line dropped on my side. Call me right back.</Say><Hangup/></Response>').catch(e => logErr('owner.closeHangup', e));
    } catch (eC) { logErr('ws.close', eC); }
    finalize(s);
  });
  ws.on('error', e => { logErr('ws', e); });
});

server.listen(PORT, () => logInfo('Aggie gateway listening on :' + PORT + ' (model ' + MODEL + ')'));
