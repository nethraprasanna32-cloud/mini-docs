import test from 'node:test';
import assert from 'node:assert/strict';
import { Doc } from './crdt.js';

const rng = seed => () => (seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0) / 2 ** 32;
const shuffle = (a, r) => { a = [...a]; for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(r() * (i + 1)); [a[i], a[j]] = [a[j], a[i]]; } return a; };

function edit(d, r) {
  const n = d.text.length;
  return n && r() < 0.3 ? d.deleteAt(Math.floor(r() * n)) : d.insertAt(Math.floor(r() * (n + 1)), 'abcdefgh'[Math.floor(r() * 8)]);
}
const pull = (to, from) => from.opsSince(to.vv).forEach(op => to.receive(op)); // anti-entropy via state vector

test('concurrent inserts at the same position converge and keep both', () => {
  const a = new Doc('A'), b = new Doc('B');
  const oa = a.insertAt(0, 'x'), ob = b.insertAt(0, 'y');
  a.receive(ob); b.receive(oa);
  assert.equal(a.text, b.text);
  assert.equal(a.text.length, 2);
});

test('concurrent delete and insert next to the deleted char converge', () => {
  const a = new Doc('A'), b = new Doc('B');
  const seed = a.insertAt(0, 'q'); b.receive(seed);
  const od = a.deleteAt(0), oi = b.insertAt(1, 'z');
  a.receive(oi); b.receive(od);
  assert.equal(a.text, 'z'); assert.equal(b.text, 'z');
});

test('duplicate delivery is idempotent', () => {
  const a = new Doc('A'), b = new Doc('B');
  const ops = 'hello'.split('').map((ch, i) => a.insertAt(i, ch));
  [...ops, ...ops, ...ops].forEach(op => b.receive(op));
  assert.equal(b.text, 'hello');
});

test('random order and duplicated delivery: 3 replicas converge (50 seeds)', () => {
  for (let seed = 1; seed <= 50; seed++) {
    const r = rng(seed), docs = ['A', 'B', 'C'].map(c => new Doc(c));
    const made = docs.map(d => Array.from({ length: 40 }, () => edit(d, r)));
    docs.forEach((d, i) => {
      const incoming = made.flatMap((ops, j) => (j === i ? [] : ops));
      shuffle([...incoming, ...incoming.slice(0, 20)], r).forEach(op => d.receive(op));
    });
    assert.equal(docs[1].text, docs[0].text, `seed ${seed}`);
    assert.equal(docs[2].text, docs[0].text, `seed ${seed}`);
    docs.forEach(d => assert.equal(d.pending.length, 0));
  }
});

test('partitions and healing: replicas diverge, then converge after sync (50 seeds)', () => {
  for (let seed = 1; seed <= 50; seed++) {
    const r = rng(seed), docs = ['A', 'B', 'C'].map(c => new Doc(c));
    for (let round = 0; round < 15; round++) {
      docs.forEach(d => { for (let k = 0; k < 1 + Math.floor(r() * 4); k++) edit(d, r); });
      if (r() < 0.5) { const [x, y] = shuffle(docs, r); pull(x, y); pull(y, x); } // only some pairs reconnect
    }
    docs.forEach(x => docs.forEach(y => pull(x, y)));
    docs.forEach(d => assert.equal(d.text, docs[0].text, `seed ${seed}`));
  }
});

test('lossy network (30% drops) is repaired by a state-vector sync', () => {
  const r = rng(7), a = new Doc('A'), b = new Doc('B');
  for (let i = 0; i < 100; i++) {
    const d = r() < 0.5 ? a : b, other = d === a ? b : a, op = edit(d, r);
    if (r() > 0.3) other.receive(op);
  }
  pull(a, b); pull(b, a);
  assert.equal(a.text, b.text);
});

test('reload while offline: replaying the saved log restores text, resumes numbering, and syncs', () => {
  const a = new Doc('A'), b = new Doc('B'), srv = new Doc('server');
  'hi'.split('').forEach((ch, i) => a.insertAt(i, ch));          // offline edits by A
  b.receive(a.log[0]); b.insertAt(1, 'B');                          // B edits concurrently
  const saved = JSON.parse(JSON.stringify(a.log));                  // what localStorage holds
  const a2 = new Doc('A');                                          // page reload
  saved.forEach(op => a2.receive(op));
  assert.equal(a2.text, 'hi');
  assert.equal(a2.insertAt(2, '!').s, 3);                           // numbering continues, no reuse
  [a2, b].forEach(d => pull(srv, d));
  [a2, b].forEach(d => pull(d, srv));
  assert.equal(a2.text, b.text);
  assert.equal(srv.text, a2.text);
});

test('cursor anchor follows its character through concurrent edits', () => {
  const a = new Doc('A'), b = new Doc('B');
  'abc'.split('').forEach((ch, i) => a.insertAt(i, ch));
  a.log.forEach(o => b.receive(o));
  const anchor = a.cursorAt(2);                 // caret right after "b"
  a.receive(b.insertAt(0, 'X'));                // someone types before it
  assert.equal(a.cursorIndex(anchor), 3);       // Xab|c
  a.deleteAt(2);                                // the anchor char itself is deleted
  assert.equal(a.cursorIndex(anchor), 2);       // Xa|c
});