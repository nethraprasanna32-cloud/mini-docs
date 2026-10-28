
import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { setTimeout as sleep } from 'node:timers/promises';
import { rmSync } from 'node:fs';
import { WebSocket } from 'ws';
import { Doc } from './crdt.js';

async function waitUntil(check, timeout = 4000) {
  const start = Date.now();
  while (!check()) {
    if (Date.now() - start > timeout)
      throw new Error('Convergence timed out');
    await sleep(15);
  }
}

class Peer {
  constructor(id, url, name) {
    this.id = id;
    this.url = url;
    this.name = name;
    this.doc = new Doc(id);
    this.ws = null;
  }

  async connect() {
    this.ws = new WebSocket(this.url);

    await new Promise((resolve, reject) => {
      this.ws.once('open', resolve);
      this.ws.once('error', reject);
    });

    this.ws.on('message', bytes => {
      const msg = JSON.parse(String(bytes));

      if (msg.type === 'SYNC' || msg.type === 'OPS') {
        msg.ops.forEach(op => this.doc.receive(op));
      }

      if (msg.type === 'SYNC') {
        const missing = this.doc.opsSince(msg.vv);
        if (missing.length)
          this.send({ type: 'OPS', ops: missing });
      }
    });

    this.send({
      type: 'HELLO',
      doc: this.name,
      client: this.id,
      name: this.id,
      vv: this.doc.vv
    });
  }

  send(msg) {
    if (this.ws?.readyState === WebSocket.OPEN)
      this.ws.send(JSON.stringify(msg));
  }

  insert(ch) {
    const op = this.doc.insertAt(this.doc.text.length, ch);
    this.send({ type: 'OPS', ops: [op] });
  }

  async disconnect() {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.close();
      await once(this.ws, 'close');
    }
    this.ws = null;
  }
}

test('Three clients converge after disconnection',
  { timeout: 20000 }, async () => {

  const port = 30000 + Math.floor(Math.random() * 20000);
  const name = `test-${process.pid}-${Date.now()}`;
  const url = `ws://127.0.0.1:${port}`;

  const startServer = () => spawn(
    process.execPath,
    ['server.js'],
    {
      env: { ...process.env, PORT: String(port) },
      stdio: 'ignore'
    }
  );

  let server = startServer();

  const clients = ['A', 'B', 'C'].map(id =>
    new Peer(`client-${id}`, url, name)
  );

  async function waitForServer() {
    for (let i = 0; i < 70; i++) {
      try {
        const socket = new WebSocket(url);

        await new Promise((resolve, reject) => {
          socket.once('open', resolve);
          socket.once('error', reject);
        });

        socket.close();
        return;
      } catch {
        await sleep(30);
      }
    }
    throw new Error('Server failed to start');
  }

  try {
    await waitForServer();

    for (const client of clients)
      await client.connect();

    await sleep(80);

    // Test 1: Concurrent editing
    clients[0].insert('A');
    clients[1].insert('B');
    clients[2].insert('C');

    await waitUntil(() =>
      clients.every(c => c.doc.text.length === 3)
    );

    assert.equal(
      new Set(clients.map(c => c.doc.text)).size, 1
    );

    // Test 2: Network disconnection
    await clients[1].disconnect();

    // Offline client edits locally
    clients[1].insert('X');

    // Online clients continue editing
    clients[0].insert('Y');
    clients[2].insert('Z');

    await waitUntil(() =>
      clients[0].doc.text.length === 5 &&
      clients[2].doc.text.length === 5
    );

    // Test 3: Reconnection and convergence
    await clients[1].connect();

    await waitUntil(() =>
      clients.every(c => c.doc.text.length === 6) &&
      new Set(clients.map(c => c.doc.text)).size === 1
    );

    assert.ok(
      clients.every(c => c.doc.pending.length === 0)
    );

    const finalText = clients[0].doc.text;

    // Test 4: Server persistence
    for (const client of clients)
      await client.disconnect();

    server.kill();
    await once(server, 'exit');

    server = startServer();
    await waitForServer();

    const fresh = new Peer('new-client', url, name);

    try {
      await fresh.connect();

      await waitUntil(() =>
        fresh.doc.text.length === 6
      );

      assert.equal(fresh.doc.text, finalText);
    } finally {
      await fresh.disconnect();
    }

  } finally {
    for (const client of clients)
      await client.disconnect().catch(() => {});

    if (server.exitCode === null) {
      server.kill();
      await once(server, 'exit').catch(() => {});
    }

    rmSync(`data/${name}.jsonl`, { force: true });
  }
});
