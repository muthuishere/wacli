'use strict';
// Reply-threading (#385) tests. Fully isolated: AGENT_WHATSAPP_HOME points at a
// throwaway temp dir, so nothing here can touch the live listener's real config
// or its session inboxes. Run: node --test  (from this directory).
const { test, after } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

const HOME = fs.mkdtempSync(path.join(os.tmpdir(), 'aw-reply-test-'));
process.env.AGENT_WHATSAPP_HOME = HOME;
delete process.env.AGENT_WHATSAPP_REPLY_THREADING; // start with the flag OFF

const cli = require('./cli.js');
const { routeMessage, recordSentTag, quotedTag, _paths } = cli;

after(() => { try { fs.rmSync(HOME, { recursive: true, force: true }); } catch {} });

function mkSession(tag) {
  const d = path.join(_paths.SESS, tag);
  fs.mkdirSync(d, { recursive: true });
  fs.writeFileSync(path.join(d, 'inbox.ndjson'), '');
  fs.writeFileSync(path.join(d, 'session.json'), JSON.stringify({ tag, status: 'active' }));
  return d;
}
function inbox(tag) {
  const f = path.join(_paths.SESS, tag, 'inbox.ndjson');
  if (!fs.existsSync(f)) return [];
  return fs.readFileSync(f, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
}

// --- send-side capture --------------------------------------------------------
test('capture: recordSentTag -> quotedTag round-trips, unknown id -> null', () => {
  recordSentTag('MID-cap', 'beta');
  assert.equal(quotedTag('MID-cap'), 'beta');
  assert.equal(quotedTag('never-sent'), null);
  assert.equal(quotedTag(null), null);
});

// --- flag OFF: byte-identical to today ---------------------------------------
test('flag OFF: quoted-id is ignored, routes by #tag, NO quoted fields written', () => {
  delete process.env.AGENT_WHATSAPP_REPLY_THREADING;
  mkSession('alpha');
  recordSentTag('MID-A', 'alpha'); // present in map, but OFF must not consult it
  const ok = routeMessage({ id: 'in-off', ts: 'T1', from: 'CEO',
    text: 'reply body #alpha', quotedId: 'MID-A', quotedSender: 's@wa' });
  assert.equal(ok, true);
  const lines = inbox('alpha');
  assert.equal(lines.length, 1);
  // exact key set + values == today's legacy entry shape (byte-identical proof)
  assert.deepEqual(lines[0], { ts: 'T1', from: 'CEO', id: 'in-off', tag: 'alpha', text: 'reply body #alpha' });
  assert.equal('quotedId' in lines[0], false);
  assert.equal('quotedSender' in lines[0], false);
});

// --- flag ON: route by quoted-id, no #tag needed -----------------------------
test('flag ON: routes by quoted-id with NO #tag in the reply text', () => {
  process.env.AGENT_WHATSAPP_REPLY_THREADING = '1';
  mkSession('gamma');
  recordSentTag('MID-G', 'gamma');
  const ok = routeMessage({ id: 'in-on1', ts: 'T2', from: 'CEO',
    text: 'looks good, ship it', quotedId: 'MID-G', quotedSender: 's@y' });
  assert.equal(ok, true);
  const lines = inbox('gamma');
  assert.equal(lines.length, 1);
  assert.equal(lines[0].tag, 'gamma');
  assert.equal(lines[0].quotedId, 'MID-G');       // additive field present
  assert.equal(lines[0].quotedSender, 's@y');
});

// --- flag ON: quoted-id wins over a (different) #tag in the text -------------
test('flag ON: quoted-id takes precedence over a #tag present in the text', () => {
  process.env.AGENT_WHATSAPP_REPLY_THREADING = '1';
  mkSession('eps');
  mkSession('zed');
  recordSentTag('MID-E', 'eps');
  const ok = routeMessage({ id: 'in-on2', ts: 'T3', from: 'CEO',
    text: 'reply #zed (wrong tag, should be ignored)', quotedId: 'MID-E' });
  assert.equal(ok, true);
  assert.equal(inbox('eps').length, 1); // routed to eps (the quoted message's tag)
  assert.equal(inbox('zed').length, 0); // NOT to the #zed mentioned in text
});

// --- flag ON: falls back to #tag when the quoted-id is unknown ---------------
test('flag ON: falls back to #tag matching when quoted-id is unknown', () => {
  process.env.AGENT_WHATSAPP_REPLY_THREADING = '1';
  mkSession('delta');
  const ok = routeMessage({ id: 'in-on3', ts: 'T4', from: 'CEO',
    text: 'hello #delta', quotedId: 'UNKNOWN-ID', quotedSender: null });
  assert.equal(ok, true);
  const lines = inbox('delta');
  assert.equal(lines.length, 1);
  assert.equal(lines[0].tag, 'delta');
  assert.equal(lines[0].quotedId, 'UNKNOWN-ID'); // still recorded additively
});

// --- dedupe regression: same id is routed at most once -----------------------
test('seen-dedupe still holds: a repeated message-id routes only once', () => {
  process.env.AGENT_WHATSAPP_REPLY_THREADING = '1';
  mkSession('omega');
  recordSentTag('MID-O', 'omega');
  const msg = { id: 'dup-1', ts: 'T5', from: 'CEO', text: 'hi', quotedId: 'MID-O' };
  assert.equal(routeMessage(msg), true);
  assert.equal(routeMessage(msg), false); // second time: already seen
  assert.equal(inbox('omega').length, 1);
});
