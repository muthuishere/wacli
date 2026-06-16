#!/usr/bin/env node
/**
 * agent-whatsapp — global two-way WhatsApp bridge for AI coding sessions.
 * Zero dependencies (Node built-ins only). Engine: wacli (whatsmeow Go CLI).
 *
 * One WhatsApp group = the ops channel. Many sessions share it, kept apart by
 * a #tag per session. ANY message in the group is accepted (any sender, any
 * device) — only the skill's own sends are filtered out via a sent-ID ledger.
 *
 * Commands:
 *   connect [--group NAME|JID]  onboarding: verify wacli auth -> pick ops group -> save
 *   register <tag> <purpose…>   record a session in the registry
 *   send <tag> <message…>       send "#tag\n<message>" to the ops group
 *   listen                      singleton daemon: wacli sync --follow + local webhook
 *                               receiver; demux inbound by #tag into inboxes
 *   watch <tag>                 stream this tag's new inbound lines (use under a Monitor)
 *   list                        print registered sessions
 *   whoami                      print resolved config
 *
 * Config: ~/.config/agent-whatsapp/config.json  { groupJid, groupName, wacliBin, port }
 */
'use strict';
const fs = require('fs');
const os = require('os');
const path = require('path');
const http = require('http');
const readline = require('readline');
const { execFile, spawn } = require('child_process');

// AGENT_WHATSAPP_HOME lets the whole state dir be relocated (used by the test
// suite for full isolation — so tests never touch the live listener's config).
const BASE = process.env.AGENT_WHATSAPP_HOME || path.join(os.homedir(), '.config', 'agent-whatsapp');
const CFG = path.join(BASE, 'config.json');
const SESS = path.join(BASE, 'sessions');
const LOCK = path.join(BASE, 'listener.lock');
const LEDGER = path.join(BASE, 'sent-ids.json');   // ids we sent (echo filter)
const SEEN = path.join(BASE, 'seen-ids.json');     // ids already routed (webhook/poll dedupe)
const MSGTAGS = path.join(BASE, 'msg-tags.json');  // [{id,tag}] map: our sent msg-id -> its #tag (reply-threading)
const LASTSEEN = path.join(BASE, 'last-seen');     // ISO ts of newest routed message
const SYNCLOG = path.join(BASE, 'sync-events.ndjson');
const DEFAULT_PORT = 8799;

const LOG = path.join(BASE, 'bridge.log');
const LOG_MAX = 2 * 1024 * 1024;       // rotate bridge.log at 2 MB
const SYNCLOG_MAX = 5 * 1024 * 1024;   // rotate sync-events.ndjson at 5 MB

function ensureDirs() { fs.mkdirSync(SESS, { recursive: true }); }

// rolling logs: at maxBytes, current file becomes <file>.1 (previous .1 is dropped)
function rotate(file, maxBytes) {
  try { if (fs.statSync(file).size > maxBytes) fs.renameSync(file, file + '.1'); } catch {}
}

function logLine(msg) {
  ensureDirs();
  rotate(LOG, LOG_MAX);
  fs.appendFileSync(LOG, `${new Date().toISOString()} ${msg}\n`);
}

function loadJSON(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return fallback; }
}
function saveJSON(file, obj) { ensureDirs(); fs.writeFileSync(file, JSON.stringify(obj, null, 2)); }

function loadCfg() {
  const cfg = loadJSON(CFG, {});
  if (process.env.AGENT_WHATSAPP_GROUP) cfg.groupJid = process.env.AGENT_WHATSAPP_GROUP;
  if (process.env.AGENT_WHATSAPP_WACLI) cfg.wacliBin = process.env.AGENT_WHATSAPP_WACLI;
  cfg.wacliBin = cfg.wacliBin || 'wacli';
  cfg.port = cfg.port || DEFAULT_PORT;
  return cfg;
}

// --- wacli shell-out (always --json; args array, no shell quoting issues) ---
function wacli(cfg, args, opts = {}) {
  return new Promise((resolve, reject) => {
    execFile(cfg.wacliBin, ['--json', ...args], { maxBuffer: 64 * 1024 * 1024, ...opts },
      (err, stdout) => {
        let parsed = null;
        try { parsed = JSON.parse(stdout); } catch {}
        if (parsed) return resolve(parsed);
        reject(err || new Error('wacli: unparseable output: ' + String(stdout).slice(0, 200)));
      });
  });
}

function ask(rl, q) { return new Promise((r) => rl.question(q, (a) => r(a.trim()))); }

// --- connect: onboarding (wacli auth must already be paired) ---
async function connect(args) {
  ensureDirs();
  const cfg = loadCfg();
  let status;
  try { status = await wacli(cfg, ['auth', 'status']); }
  catch (e) { die(`✗ cannot run "${cfg.wacliBin}" — install the OFFICIAL wacli:\n    brew install openclaw/tap/wacli\n  (docs: https://wacli.sh · releases: https://github.com/openclaw/wacli/releases)\n  Do not build from source/forks — track official releases via brew.\n  (${e.message})`); }
  if (!status.success || !status.data.authenticated) {
    die('✗ wacli is not paired. Run "wacli auth" in your terminal, scan the QR with WhatsApp → Linked devices, then re-run connect.');
  }
  console.log(`✓ wacli paired as ${status.data.phone}`);

  const groupArgIdx = args.indexOf('--group');
  const wanted = groupArgIdx >= 0 ? args.slice(groupArgIdx + 1).join(' ').trim() : null;

  const res = await wacli(cfg, ['groups', 'list']);
  const groups = (res.data || []).map((g) => ({ jid: g.JID, name: g.Name || '(unnamed)' }));
  if (!groups.length) die('✗ no groups in the local store yet — create a group on your phone, run "wacli sync --once --refresh-groups", then re-run connect.');

  let pick = null;
  if (wanted) {
    pick = groups.find((g) => g.jid === wanted) ||
           groups.find((g) => g.name.toLowerCase() === wanted.toLowerCase()) ||
           groups.find((g) => g.name.toLowerCase().includes(wanted.toLowerCase()));
    if (!pick) die(`✗ no group matching "${wanted}". Known:\n` + groups.map((g) => `  ${g.name}  ${g.jid}`).join('\n'));
  } else {
    console.log('\nPick the ops group (ALL messages in it will be routed to sessions):');
    groups.forEach((g, i) => console.log(`  [${i}] ${g.name}  ${g.jid}`));
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    const n = await ask(rl, 'Group number: ');
    rl.close();
    pick = groups[Number(n) || 0];
  }
  saveJSON(CFG, { groupJid: pick.jid, groupName: pick.name, wacliBin: cfg.wacliBin === 'wacli' ? undefined : cfg.wacliBin, port: cfg.port });
  console.log(`✓ connected — ops group "${pick.name}" (${pick.jid})`);
  console.log(`\nNext: agent-whatsapp register <tag> "<purpose>"  then  agent-whatsapp listen  (background)`);
}

function sessionDir(tag) { return path.join(SESS, tag); }

function register(tag, purpose) {
  if (!tag) die('usage: agent-whatsapp register <tag> <purpose…>');
  ensureDirs();
  const d = sessionDir(tag);
  fs.mkdirSync(d, { recursive: true });
  const inbox = path.join(d, 'inbox.ndjson');
  if (!fs.existsSync(inbox)) fs.writeFileSync(inbox, '');
  fs.writeFileSync(path.join(d, 'session.json'), JSON.stringify({
    tag, purpose: purpose || '', cwd: process.cwd(),
    registered_at: new Date().toISOString(), status: 'active',
  }, null, 2));
  console.log('registered #' + tag);
}

function listenerAlive() {
  if (!fs.existsSync(LOCK)) return false;
  const pid = Number(fs.readFileSync(LOCK, 'utf8'));
  try { process.kill(pid, 0); return pid; } catch { return false; }
}

function ledgerAdd(id) {
  const l = loadJSON(LEDGER, []);
  l.push(id);
  saveJSON(LEDGER, l.slice(-500));
}

// --- reply-threading (#385) — FLAG-GATED, default OFF -----------------------
// When OFF, none of the helpers below are consulted and behaviour is identical
// to today's pure #tag routing. Enable via config.json {"replyThreading":true}
// or env AGENT_WHATSAPP_REPLY_THREADING=1.
function replyThreadingOn(cfg) {
  if (process.env.AGENT_WHATSAPP_REPLY_THREADING != null) {
    const v = String(process.env.AGENT_WHATSAPP_REPLY_THREADING).toLowerCase();
    return v === '1' || v === 'true' || v === 'yes' || v === 'on';
  }
  return (cfg || loadCfg()).replyThreading === true;
}

// remember which #tag a message-id we sent belongs to, so a native WhatsApp
// reply quoting that id can be routed back to the same session automatically.
function recordSentTag(id, tag) {
  if (!id || !tag) return;
  const m = loadJSON(MSGTAGS, []);
  m.push({ id, tag });
  saveJSON(MSGTAGS, m.slice(-1000));
}

// resolve the #tag for a quoted message-id (the message the reply quotes).
function quotedTag(quotedId) {
  if (!quotedId) return null;
  const m = loadJSON(MSGTAGS, []);
  for (let i = m.length - 1; i >= 0; i--) if (m[i].id === quotedId) return m[i].tag;
  return null;
}

async function send(args) {
  const cfg = loadCfg();
  const threading = replyThreadingOn(cfg);
  const tag = args[0];
  let words = args.slice(1);
  // reply-threading only: consume optional --reply-to / --reply-to-sender flags
  // so an agent can quote-reply and render a clean WhatsApp thread. When the
  // flag is OFF these are left in the message verbatim (byte-identical to today).
  let replyTo = null, replyToSender = null;
  if (threading) {
    const out = [];
    for (let i = 0; i < words.length; i++) {
      if (words[i] === '--reply-to') { replyTo = words[++i]; continue; }
      if (words[i] === '--reply-to-sender') { replyToSender = words[++i]; continue; }
      out.push(words[i]);
    }
    words = out;
  }
  const message = words.join(' ');
  if (!tag || !message) die('usage: agent-whatsapp send <tag> [--reply-to <msgID> [--reply-to-sender <JID>]] <message…>');
  if (!cfg.groupJid) die('not connected — run: agent-whatsapp connect');
  if (!listenerAlive()) console.error('⚠ listener not running — replies will NOT be routed (start: agent-whatsapp listen)');
  const sendArgs = ['send', 'text', '--to', cfg.groupJid, '--message', `#${tag}\n${message}`];
  if (threading && replyTo) {
    sendArgs.push('--reply-to', replyTo);
    if (replyToSender) sendArgs.push('--reply-to-sender', replyToSender);
  }
  const r = await wacli(cfg, sendArgs);
  if (!r.success) { console.error('✗ ' + (r.error || 'send failed')); process.exit(1); }
  ledgerAdd(r.data.id);
  if (threading) recordSentTag(r.data.id, tag);   // remember msg-id -> #tag for reply-routing
  console.log(`sent #${tag} (${r.data.id})`);
}

// send a file to the ops group — ALWAYS use this instead of raw `wacli send file`,
// otherwise the sent message id never enters the ledger and the catch-up poll
// re-ingests your own attachment as inbound (observed 2026-06-12).
async function sendFile(tag, file, caption) {
  if (!tag || !file) die('usage: agent-whatsapp send-file <tag> <path> [caption…]');
  if (!fs.existsSync(file)) die(`✗ file not found: ${file}`);
  const cfg = loadCfg();
  if (!cfg.groupJid) die('not connected — run: agent-whatsapp connect');
  const r = await wacli(cfg, ['send', 'file', '--to', cfg.groupJid, '--file', file,
    '--caption', `#${tag}${caption ? '\n' + caption : ''}`]);
  if (!r.success) { console.error('✗ ' + (r.error || 'send failed')); process.exit(1); }
  ledgerAdd(r.data.id);
  console.log(`sent file #${tag} (${r.data.id})`);
}

// --- attachments: download media for a message into the right assets dir ---
// Saved under sessions/<tag>/assets/ (tagged) or <BASE>/assets/ (untagged broadcast).
// Uses --read-only + explicit --output so the sync daemon's store lock is never touched.
function downloadAsset(cfg, msgId, tag, cb) {
  const baseDir = tag ? path.join(sessionDir(tag), 'assets') : path.join(BASE, 'assets');
  // per-message staging dir: concurrent downloads can never misattribute files
  const stage = path.join(baseDir, '.dl-' + msgId);
  fs.mkdirSync(stage, { recursive: true });
  execFile(cfg.wacliBin, ['--read-only', 'media', 'download', '--chat', cfg.groupJid, '--id', msgId, '--output', stage],
    { timeout: 120000 }, (err) => {
      const got = fs.existsSync(stage) ? fs.readdirSync(stage) : [];
      if (err || !got.length) {
        if (err) logLine(`asset-download FAILED id=${msgId}: ${err.message.split('\n')[0]}`);
        try { fs.rmSync(stage, { recursive: true, force: true }); } catch {}
        return cb(null);
      }
      const ext = path.extname(got[0]);
      const file = path.join(baseDir, `${msgId}${ext || ''}`);
      fs.renameSync(path.join(stage, got[0]), file);
      try { fs.rmSync(stage, { recursive: true, force: true }); } catch {}
      logLine(`asset-saved id=${msgId} -> ${file}`);
      cb(file);
    });
}

function extractTag(text) {
  for (const tok of (text || '').split(/\s+/)) { if (tok.startsWith('#') && tok.length > 1) return tok.slice(1).toLowerCase(); }
  return null;
}

// --- inbound routing (shared by webhook + catch-up poll) ---
// Accept ANYTHING in the ops group from any sender; only skip our own sends (ledger).
function routeMessage(msg) {
  // msg: {id, ts, from, text, asset?, quotedId?, quotedSender?}
  const ledger = loadJSON(LEDGER, []);
  if (ledger.includes(msg.id)) return false;
  const seen = loadJSON(SEEN, []);
  if (seen.includes(msg.id)) return false;
  seen.push(msg.id); saveJSON(SEEN, seen.slice(-1000));

  // reply-threading (#385, flag-gated): if this inbound quotes a message we
  // sent, route it to THAT session's #tag automatically — no #tag needed in the
  // reply text. Falls back to today's #tag extraction when there's no match.
  // When the flag is OFF, quotedTag() is never consulted and no extra fields are
  // written, so the inbox line is byte-identical to today.
  const threading = replyThreadingOn();
  const byQuote = threading ? quotedTag(msg.quotedId) : null;
  const tag = byQuote || extractTag(msg.text);
  const extra = (threading && msg.quotedId)
    ? { quotedId: msg.quotedId, ...(msg.quotedSender ? { quotedSender: msg.quotedSender } : {}) }
    : {};
  const entry = JSON.stringify({ ts: msg.ts, from: msg.from, id: msg.id, tag, text: msg.text, ...(msg.asset ? { asset: msg.asset } : {}), ...extra }) + '\n';

  if (tag && fs.existsSync(sessionDir(tag))) {
    fs.appendFileSync(path.join(sessionDir(tag), 'inbox.ndjson'), entry);
  } else {
    // untagged (or unknown tag): deliver to every active session + audit trail
    let delivered = 0;
    if (fs.existsSync(SESS)) {
      for (const d of fs.readdirSync(SESS, { withFileTypes: true })) {
        if (!d.isDirectory()) continue;
        const s = loadJSON(path.join(SESS, d.name, 'session.json'), null);
        if (!s || s.status !== 'active') continue;
        fs.appendFileSync(path.join(SESS, d.name, 'inbox.ndjson'), entry);
        delivered++;
      }
    }
    fs.appendFileSync(path.join(BASE, 'unrouted.ndjson'), entry);
    if (!delivered) return false;
  }
  const ts = Date.parse(msg.ts);
  const prev = fs.existsSync(LASTSEEN) ? Date.parse(fs.readFileSync(LASTSEEN, 'utf8').trim()) : 0;
  if (ts > prev) fs.writeFileSync(LASTSEEN, msg.ts);
  return true;
}

// catch-up: poll the wacli store for messages the webhook may have missed
async function catchUp(cfg) {
  let res;
  try { res = await wacli(cfg, ['--read-only', 'messages', 'list', '--chat', cfg.groupJid, '--limit', '50']); }
  catch { return; }
  if (!res.success) return;
  const since = fs.existsSync(LASTSEEN) ? Date.parse(fs.readFileSync(LASTSEEN, 'utf8').trim()) : Date.now() - 5 * 60 * 1000;
  const rows = (res.data.messages || []).slice().reverse(); // store returns newest-first
  for (const m of rows) {
    if (Date.parse(m.Timestamp) <= since) continue;
    if (!m.Text && !m.MediaType) continue;
    if (m.SenderName === 'me') continue; // wacli-originated sends (belt for un-ledgered ones)
    const text = (m.Text || m.MediaCaption || `[${m.MediaType}]`).trim();
    const base = { id: m.MsgID, ts: m.Timestamp, from: m.SenderName || m.SenderJID || '?', text,
      quotedId: m.QuotedMsgID || null, quotedSender: m.QuotedSenderJID || null };
    if (m.MediaType) {
      downloadAsset(cfg, m.MsgID, extractTag(text), (file) => {
        if (routeMessage({ ...base, ...(file ? { asset: file } : {}) })) logLine(`routed(catchup) id=${m.MsgID} media=yes`);
      });
    } else if (routeMessage(base)) {
      logLine(`routed(catchup) id=${m.MsgID}`);
    }
  }
}

async function listen() {
  const cfg = loadCfg();
  if (!cfg.groupJid) die('not connected — run: agent-whatsapp connect');
  const alive = listenerAlive();
  if (alive) { console.log(`listener already running (pid ${alive})`); return; }
  ensureDirs();
  fs.writeFileSync(LOCK, String(process.pid));

  let child = null;
  const cleanup = () => {
    logLine('listener stopped');
    try { fs.unlinkSync(LOCK); } catch {}
    if (child) try { child.kill('SIGTERM'); } catch {}
    process.exit(0);
  };
  process.on('SIGINT', cleanup); process.on('SIGTERM', cleanup);

  // webhook receiver (loopback only)
  const server = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', () => {
      res.writeHead(200); res.end('ok');
      try {
        const pm = JSON.parse(body);
        if (pm.Chat !== cfg.groupJid) return; // webhook fans out ALL chats — keep only ops group
        if (!pm.Text && !pm.Media) return;
        const text = (pm.Text || (pm.Media && pm.Media.Caption) || `[${(pm.Media && pm.Media.Type) || 'media'}]`).trim();
        const base = { id: pm.ID, ts: pm.Timestamp, from: pm.PushName || pm.SenderJID || '?', text,
          quotedId: pm.ReplyToID || null, quotedSender: pm.ReplyToSenderJID || null };
        if (pm.Media) {
          downloadAsset(cfg, pm.ID, extractTag(text), (file) => {
            const ok = routeMessage({ ...base, ...(file ? { asset: file } : {}) });
            if (ok) logLine(`routed id=${pm.ID} tag=${extractTag(text) || '(broadcast)'} media=yes`);
          });
        } else {
          const ok = routeMessage(base);
          if (ok) logLine(`routed id=${pm.ID} tag=${extractTag(text) || '(broadcast)'}`);
        }
      } catch (e) { logLine(`webhook-parse-error: ${e.message}`); }
    });
  });
  server.listen(cfg.port, '127.0.0.1', () => console.error(`receiver up on 127.0.0.1:${cfg.port}`));

  // supervised wacli sync --follow (the single store-lock owner)
  rotate(SYNCLOG, SYNCLOG_MAX);
  const synclog = fs.createWriteStream(SYNCLOG, { flags: 'a' });
  const startSync = () => {
    rotate(SYNCLOG, SYNCLOG_MAX);
    child = spawn(cfg.wacliBin, [
      'sync', '--follow', '--max-reconnect', '0', '--stale-threshold', '60s', '--events',
      '--webhook', `http://127.0.0.1:${cfg.port}`, '--webhook-allow-private',
    ], { stdio: ['ignore', 'ignore', 'pipe'] });
    child.stderr.pipe(synclog, { end: false });
    child.on('exit', (code) => {
      console.error(`wacli sync exited (${code}) — restarting in 5s`);
      logLine(`sync-child exited code=${code}, restarting in 5s`);
      setTimeout(startSync, 5000);
    });
  };
  startSync();
  logLine(`listener started pid=${process.pid} group="${cfg.groupName}" port=${cfg.port}`);
  console.error(`listener started — ops group "${cfg.groupName}" demuxed by #tag (untagged → all active sessions)`);

  // catch-up on boot + safety-net poll (webhook is best-effort by design)
  setTimeout(() => catchUp(cfg), 15000);
  setInterval(() => catchUp(cfg), 120000);
}

// stream new inbound lines for a tag (run under a Monitor)
function watch(tag) {
  if (!tag) die('usage: agent-whatsapp watch <tag>');
  const inbox = path.join(sessionDir(tag), 'inbox.ndjson');
  fs.mkdirSync(sessionDir(tag), { recursive: true });
  if (!fs.existsSync(inbox)) fs.writeFileSync(inbox, '');
  let size = fs.statSync(inbox).size; // start at end (only new replies)
  setInterval(() => {
    let st; try { st = fs.statSync(inbox); } catch { return; }
    if (st.size <= size) { size = st.size; return; }
    const fd = fs.openSync(inbox, 'r');
    const buf = Buffer.alloc(st.size - size);
    fs.readSync(fd, buf, 0, buf.length, size); fs.closeSync(fd);
    size = st.size;
    for (const line of buf.toString('utf8').split('\n')) {
      if (!line.trim()) continue;
      try { const o = JSON.parse(line); console.log(`[${o.from}] ${o.text}`); }
      catch { console.log(line); }
    }
  }, 1000);
}

// stop the listener daemon (and its wacli sync child)
function stop() {
  const pid = listenerAlive();
  if (!pid) { console.log('listener not running'); try { fs.unlinkSync(LOCK); } catch {} return; }
  try { process.kill(pid, 'SIGTERM'); } catch {}
  // the listener's SIGTERM handler kills its sync child; belt-and-braces after a beat:
  setTimeout(() => {
    try { process.kill(pid, 0); process.kill(pid, 'SIGKILL'); } catch {}
    try { fs.unlinkSync(LOCK); } catch {}
    console.log(`stopped listener (pid ${pid})`);
  }, 1500);
}

// end a session registration: mark ended so untagged broadcasts skip it
function end(tag) {
  if (!tag) die('usage: agent-whatsapp end <tag>');
  const f = path.join(sessionDir(tag), 'session.json');
  if (!fs.existsSync(f)) die(`✗ no session registered as #${tag}`);
  const s = loadJSON(f, {});
  s.status = 'ended'; s.ended_at = new Date().toISOString();
  fs.writeFileSync(f, JSON.stringify(s, null, 2));
  console.log(`ended #${tag}`);
}

function list() {
  ensureDirs();
  const tags = fs.readdirSync(SESS, { withFileTypes: true }).filter((d) => d.isDirectory());
  if (!tags.length) { console.log('(no sessions registered)'); return; }
  for (const t of tags) {
    const s = loadJSON(path.join(SESS, t.name, 'session.json'), null);
    if (s) console.log(`#${s.tag}  ${s.status}  ${s.purpose}  (${s.cwd})`);
  }
}

function whoami() {
  const cfg = loadCfg();
  console.log(JSON.stringify({
    groupJid: cfg.groupJid || '(none)', groupName: cfg.groupName || '?',
    wacliBin: cfg.wacliBin, port: cfg.port,
    replyThreading: replyThreadingOn(cfg),
    listener: listenerAlive() || 'not running', base: BASE,
  }, null, 2));
}

function die(msg) { console.error(msg); process.exit(1); }

// Exported for the test suite (require'd as a module). The CLI dispatcher below
// only runs when executed directly, so importing this file has no side effects.
module.exports = {
  routeMessage, extractTag, replyThreadingOn, recordSentTag, quotedTag,
  loadJSON, saveJSON, sessionDir,
  _paths: { BASE, SESS, MSGTAGS, LEDGER, SEEN, CFG },
};

if (require.main !== module) return;

(async () => {
  const [cmd, ...rest] = process.argv.slice(2);
  switch (cmd) {
    case 'connect': return connect(rest);
    case 'register': return register(rest[0], rest.slice(1).join(' '));
    case 'send': return send(rest);
    case 'send-file': return sendFile(rest[0], rest[1], rest.slice(2).join(' '));
    case 'listen': return listen();
    case 'stop': return stop();
    case 'end': return end(rest[0]);
    case 'watch': return watch(rest[0]);
    case 'list': return list();
    case 'whoami': return whoami();
    default:
      console.log('agent-whatsapp <connect|register|send|listen|stop|end|watch|list|whoami>');
      console.log('  stop                      stop the listener daemon (+ its wacli sync child)');
      console.log('  end <tag>                 end a session registration (broadcasts skip it)');
      console.log('  connect [--group NAME]    onboarding: verify wacli auth -> pick ops group');
      console.log('  register <tag> <purpose…> record this session');
      console.log('  send <tag> <message…>     tagged outbound to the ops group');
      console.log('  send-file <tag> <path> [caption…]  send a file (ledgered — never use raw wacli send file)');
      console.log('  listen                    singleton daemon: sync --follow + webhook demux');
      console.log('  watch <tag>               stream this tag\'s replies (run under a Monitor)');
      console.log('  list | whoami');
  }
})();
