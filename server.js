import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';
import { Doc } from './crdt.js';

const dir = path.dirname(fileURLToPath(import.meta.url));
const dataDir = path.join(dir, 'data');
fs.mkdirSync(dataDir, { recursive: true });

const rooms = new Map(); // docId -> { doc, clients:Set<ws>, file }

function room(id) {
  id = id.replace(/[^\w-]/g, '').slice(0, 40) || 'main';
  if (!rooms.has(id)) {
    const file = path.join(dataDir, `${id}.jsonl`);
    const doc = new Doc('server');
    if (fs.existsSync(file))
      fs.readFileSync(file, 'utf8').split('\n').filter(Boolean).forEach(l => doc.receive(JSON.parse(l)));
    rooms.set(id, { doc, clients: new Set(), file });
  }
  return rooms.get(id);
}

const files = { '/': 'index.html', '/crdt.js': 'crdt.js' };
const server = http.createServer((req, res) => {
  const f = files[req.url.split('?')[0]];
  if (!f) return res.writeHead(404).end();
  res.writeHead(200, { 'Content-Type': f.endsWith('.js') ? 'text/javascript' : 'text/html' });
  fs.createReadStream(path.join(dir, f)).pipe(res);
});

const wss = new WebSocketServer({ server });
const send = (ws, m) => ws.readyState === 1 && ws.send(JSON.stringify(m));

// Presence is ephemeral (not part of the CRDT): who is connected and who is typing right now.
function presence(r) {
  const users = new Map();
  r.clients.forEach(c => c.user && users.set(c.user.id, c.user));
  const list = [...users.values()];
  r.clients.forEach(c => send(c, { type: 'PRESENCE', users: list }));
}

wss.on('connection', ws => {
  let r = null;
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'HELLO') {                       // client announces its state vector
      r = room(m.doc);
      ws.user = { id: String(m.client), name: String(m.name || m.client).slice(0, 20), editing: false };
      r.clients.add(ws);
      send(ws, { type: 'SYNC', ops: r.doc.opsSince(m.vv || {}), vv: r.doc.vv });
      presence(r);
    } else if (m.type === 'NAME' && r && ws.user) {   // client renamed itself
      ws.user.name = String(m.name || ws.user.id).slice(0, 20);
      presence(r);
    } else if (m.type === 'CURSOR' && r && ws.user) {  // caret moved: relay to the others
      ws.user.cursor = m.anchor ?? null;
      r.clients.forEach(c => c !== ws && send(c, { type: 'CURSOR', id: ws.user.id, anchor: ws.user.cursor }));
    } else if (m.type === 'OPS' && r) {             // client pushes ops
      const n0 = r.doc.log.length;
      (m.ops || []).forEach(op => r.doc.receive(op));
      const fresh = r.doc.log.slice(n0);            // only ops that were newly applied
      if (fresh.length) {
        fs.appendFileSync(r.file, fresh.map(o => JSON.stringify(o)).join('\n') + '\n');
        r.clients.forEach(c => c !== ws && send(c, { type: 'OPS', ops: fresh }));
      }
      if (ws.user && fresh.length) {                  // mark as "editing" until 2s of quiet
        clearTimeout(ws.typingTimer);
        if (!ws.user.editing) { ws.user.editing = true; presence(r); }
        ws.typingTimer = setTimeout(() => { ws.user.editing = false; presence(r); }, 2000);
      }
      send(ws, { type: 'ACK', vv: r.doc.vv });
    }
  });

  ws.on('close', () => {
    clearTimeout(ws.typingTimer);
    if (r) { r.clients.delete(ws); presence(r); }
  });
});

// Heartbeat: drop dead connections so clients notice and reconnect.
setInterval(() => wss.clients.forEach(ws => {
  if (!ws.isAlive) return ws.terminate();
  ws.isAlive = false;
  ws.ping();
}), 15000);

server.listen(process.env.PORT || 3000, () => console.log('http://localhost:' + (process.env.PORT || 3000)));