# Aggie's New Telephone — the Voice Gateway
**LABEL: LIVING** — this README is the setup and operations manual for the gateway.

The live phone call no longer touches Google. Twilio streams the caller's speech
here; this little service runs Aggie's brain and streams her voice back.
APS (Google) is still the book of record and the brain source — it's just never
in the answer path.

**If this service is ever down, callers ring Chris's cell.** That is built into
the phone number's TwiML, automatically, with no action needed.

---

## ONE-TIME SETUP (about 15 minutes, all buttons)

### ☐ 1. Put this folder on GitHub
1. Go to **github.com** → click the **+** (top right) → **New repository**
2. Name: `aggie-gateway` · set **Private** · click **Create repository**
3. On the new empty repo page click **uploading an existing file**
4. Drag ALL the files from this folder in (server.js, package.json, render.yaml, .gitignore, README.md, twilio-number-twiml.xml)
5. Click **Commit changes**

### ☐ 2. Deploy on Render (free)
1. Go to **render.com** → sign up with the **GitHub** button (one click)
2. Dashboard → **New +** → **Blueprint**
3. Pick the `aggie-gateway` repo → Render reads `render.yaml` and shows the service
4. It will ask for the secret values — paste these:
   - **ANTHROPIC_API_KEY** — same key APS uses (Script Properties)
   - **GAS_EXEC_URL** — the pinned /exec URL, no `?k=` part
   - **WEBHOOK_KEY** — same as APS Script Properties WEBHOOK_KEY
   - **TWILIO_ACCOUNT_SID** / **TWILIO_AUTH_TOKEN** — Twilio Console front page
   - **CHRIS_CELL** — your cell in +1XXXXXXXXXX form
   - **RELAY_TOKEN** — Render generates this one itself; after deploy, open the
     service → Environment → copy its value, you need it in step 4
5. Click **Apply** and wait for the green **Live** badge
6. Copy the service URL, e.g. `https://aggie-gateway.onrender.com`

### ☐ 3. Health check (proves it breathes)
Open `https://YOUR-SERVICE.onrender.com/health` in a browser.
You want: `"ok": true` and `"brainVersion": "34.11"`. If `brainVersion` is null,
GAS_EXEC_URL or WEBHOOK_KEY is wrong — recent errors are printed right there on
the same page. Nothing fails silently.

### ☐ 4. Point the phone number at the new telephone
1. Open `twilio-number-twiml.xml` (in this folder) — replace:
   - `YOUR-SERVICE.onrender.com` with your Render URL host
   - `PASTE_RELAY_TOKEN` with the RELAY_TOKEN from Render
   - `PASTE_CHRIS_CELL` with your cell (+1…)
2. Twilio Console → **TwiML Bins** → **Create new** → name it `aggie-relay` →
   paste the edited XML → **Save**
3. **Phone Numbers → your number → Voice Configuration**:
   - **A call comes in** → TwiML Bin → `aggie-relay`
   - Leave the **fallback** exactly as it is today (the smart vrfail net)
4. Save.

### ☐ 5. Tell APS where the gateway lives
APS → Settings → Script Properties (via the editor) → add:
`RELAY_GATEWAY_URL` = your Render URL.
The 10-minute heartbeat now pings the gateway around the clock so Render's free
tier never puts it to sleep.

### ☐ 6. Test ladder (from the handoff)
- ☐ Call in, say nothing after the greeting → she should re-prompt, not die
- ☐ Speak first, book a full residential job (all six fields)
- ☐ Interrupt her mid-sentence → she stops and listens (barge-in)
- ☐ Ask for Chris twice → live transfer to your cell
- ☐ After a call: lead + transcript + email land in APS within ~2 minutes
- ☐ Kill test: suspend the Render service, call in → your cell rings

---

## HOW IT WORKS (one paragraph)
Twilio ConversationRelay does the ears and the mouth (speech-to-text and
text-to-speech, streaming, with barge-in). This service is the brain stem: it
holds the conversation, calls the Anthropic API with the exact same brain APS
compiles (`hook=brainpack`, pulled every 4 minutes and cached — plus a
caller-specific version with their dossier, raced at ring time with a hard
timeout), and streams Aggie's words back the moment they're generated. When the
call ends, the transcript, lead, booking, and duration are POSTed to APS
(`hook=relay`) with patient retries — and the existing 30-minute Twilio
reconciliation sweep is the backstop of last resort. No call can vanish.

## OPERATIONS
- **Dashboard:** the `/health` page. Uptime, brain age, live calls, last 8 errors.
- **Change her greeting:** it lives in the TwiML Bin (`welcomeGreeting`) now —
  edit it there. The APS Settings greeting still governs rescue/callback calls.
- **Change the model:** Render → Environment → `AGGIE_MODEL`
  (`claude-haiku-4-5-20251001` for max speed).
- **Bench her instantly:** Twilio → phone number → point "A call comes in" back
  at the old GAS voice URL, or at a Bin with just `<Dial>` to your cell.
  One dropdown. No deploys.

## WHAT THIS SERVICE NEVER DOES
- Never reads or writes a Google Sheet.
- Never blocks a live call on APS being slow — 1.2s brain race, then generic.
- Never swallows an error — everything lands on `/health`.
- Never takes card numbers, never invents appointments — same brain, same laws.
