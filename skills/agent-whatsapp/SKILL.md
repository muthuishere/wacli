---
name: agent-whatsapp
description: >
  Global two-way WhatsApp bridge for ANY Claude Code / AI coding session, built on wacli.
  Send tagged status updates to ONE shared WhatsApp ops group and receive replies in
  near-real-time (routed by #tag) so a reply from the phone wakes the right session and it
  acts. Accepts ANY message in the ops group from any sender; sessions are multiplexed by
  per-session #tags with a tiny registry. Zero-dep Node CLI shelling out to wacli. Use when
  the user says "send updates to whatsapp", "ping me on whatsapp", "make this session
  reachable on whatsapp", "let me reply and you act", "register/tag this session",
  "connect the whatsapp bridge", or wires a long-running / autonomous run to phone
  reporting + control over WhatsApp.
---

# agent-whatsapp — global two-way bridge (wacli engine)

One WhatsApp group → one ops channel. Many sessions share it, kept apart by a **#tag**
per session. **Any message in the group is routed** (any sender/device — only the skill's
own sends are filtered via a sent-ID ledger). Untagged replies go to **all active
sessions**; tagged replies go to that tag only.

Engine: [`wacli`](https://github.com/openclaw/wacli) (whatsmeow). Config in
`~/.config/agent-whatsapp/config.json` (`{groupJid, groupName, port}`).

## Install

**Always install the wacli engine from the OFFICIAL channel — never build it from a
fork or vendored source.** Upstream (github.com/openclaw/wacli) is actively maintained
with proper releases/changelogs; tracking it via brew keeps you on fixed, signed builds:

```sh
brew install openclaw/tap/wacli        # engine — official tap (brew upgrade wacli to update)
# the bridge CLI itself:
# from this dir (or after publish: npm i -g @muthuishere/agent-whatsapp)
npm i -g .                             # exposes the `agent-whatsapp` binary
# or invoke directly: node <skill>/cli.js <cmd>
```

Engine docs: https://wacli.sh · releases: https://github.com/openclaw/wacli/releases

## Connect scenario (onboarding — once)

1. **Pair wacli** — the user runs it themselves (interactive QR):
   ```sh
   ! WACLI_SYNC_MAX_MESSAGES=5000 wacli auth
   ```
   Phone: WhatsApp → Settings → Linked devices → Link a device → scan.
2. **Create the ops group** on the phone (e.g. "Agent Ops" — can be just yourself).
3. **Connect** (non-interactive with `--group`, else prompts a picker):
   ```sh
   agent-whatsapp connect --group "Agent Ops"
   ```
   Verifies auth, finds the group JID, saves config. If the group is new, refresh first:
   `wacli sync --once --refresh-groups`.

## Wire a session (when a run begins)

```sh
agent-whatsapp register stockloop "daily live stock research loop"   # 1. tag + purpose
agent-whatsapp listen   # 2. singleton daemon — run via Bash run_in_background:true
# 3. real-time inbound: run under a Monitor (persistent:true):
agent-whatsapp watch stockloop   # each line printed = a reply = a notification → act
```

`listen` owns the store lock: it supervises `wacli sync --follow --webhook
http://127.0.0.1:<port> --webhook-allow-private` (restarts it if it dies), receives the
webhook on loopback, and demuxes into `sessions/<tag>/inbox.ndjson`. A catch-up poll of
the wacli store runs at boot + every 2 min because webhook delivery is best-effort.

## Send / receive

```sh
agent-whatsapp send stockloop "cycle 3: bought INDIGO x2, +0.4%"   # → "#stockloop\n<msg>"
```

User replies in the group — `#stockloop sell vedl` routes to that session;
a plain `sell vedl` (no tag) routes to **every active session**. `watch` emits it →
the session acts that turn. All untagged traffic is also mirrored to `unrouted.ndjson`.

## Reply-threading (#385 — opt-in, default OFF)

A native WhatsApp **reply** (quoting an agent's message) can route straight back to
that agent's session — no `#tag` needed in the reply text. Enable per-host with
`config.json` `{"replyThreading": true}` or `AGENT_WHATSAPP_REPLY_THREADING=1`.

- **On `send`**: the sent message-id → `#tag` is remembered in `msg-tags.json`.
- **On inbound**: if the message quotes a remembered id, it routes to that tag and the
  inbox entry gains additive `quotedId`/`quotedSender` fields. No quote-match → today's
  `#tag` extraction (unchanged). Quote-match wins over a `#tag` in the text.
- **Quote-reply yourself**: `send <tag> --reply-to <msgID> [--reply-to-sender <JID>] <msg>`
  renders a clean WhatsApp thread.

Flag **OFF** = byte-identical to today: `msg-tags.json` is never written, no extra inbox
fields, pure `#tag` routing. Reversible by clearing the flag. Tests: `npm test`.

## Attachments

Media sent to the ops group (images, docs, voice notes) is downloaded automatically by
the listener via `wacli --read-only media download` (never touches the daemon's store
lock) and saved to `sessions/<tag>/assets/` (tagged) or `<base>/assets/` (untagged).
The inbox entry carries `"asset": "<absolute path>"` plus the caption as `text`
(`[image]`-style placeholder when there is no caption) — so a session reading its
inbox gets the file path ready to use.

## Logs (rolling)

`bridge.log` — listener lifecycle, every routed message id, asset saves/failures,
sync-child restarts. Rotates at 2 MB to `bridge.log.1` (one generation kept).
`sync-events.ndjson` — the wacli daemon's NDJSON event stream; rotates at 5 MB.

## Why this design (spike-verified 2026-06-11)

- **Same-identity channel**: WhatsApp has no bot accounts — the bridge sends *as you*.
  Echo loops are prevented by a ledger of sent message IDs (recorded from `send`'s JSON
  response), NOT by `FromMe` (your own phone replies are also `FromMe` in the store).
- **`--webhook-allow-private` is mandatory**: wacli's webhook uses an SSRF-safe HTTP
  client that silently refuses loopback URLs without it (warning only visible with
  `--events`). Round-trip latency with it: ~1 s.
- **Sends keep working while the daemon runs** — wacli delegates send commands to the
  running `sync --follow` process over IPC, so there is no store-lock fight.
- **Low volume discipline**: unofficial WhatsApp clients carry account-ban risk. Send
  digests, not transcripts; never rapid-fire (wacli's rapid-send guardrails stay on).

## Commands

`connect [--group NAME|JID]` · `register <tag> <purpose…>` · `send <tag> <msg…>` ·
`listen` · `watch <tag>` · `list` · `whoami`

State: `~/.config/agent-whatsapp/{config.json, sent-ids.json, seen-ids.json, last-seen,
listener.lock, bridge.log, sync-events.ndjson, unrouted.ndjson, msg-tags.json, assets/,
sessions/<tag>/{session.json, inbox.ndjson, assets/}}`.
