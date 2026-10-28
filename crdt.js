// RGA-style text CRDT. Dependency-free: runs in Node (server, tests) and the browser.
// Op:  {c, s, ts, t:'i', o:'c:s'|null, ch}  insert ch after origin o
//      {c, s, ts, t:'d', x:'c:s'}            delete (tombstone) char x
// c = client id, s = per-client sequence (1,2,3...), ts = Lamport clock.
const key = (c, s) => `${c}:${s}`;
const greater = (el, op) => el.ts > op.ts || (el.ts === op.ts && el.c > op.c);

export class Doc {
  constructor(client) {
    this.client = client;
    this.seq = 0;
    this.ts = 0;
    this.els = [];          // every char, tombstones included, in document order
    this.byId = new Map();  // "c:s" -> element
    this.vv = {};           // state vector: client -> highest contiguous seq applied
    this.log = [];          // applied ops in causal order (used for sync + persistence)
    this.pending = [];      // received ops waiting for their dependencies
  }

  get text() { return this.els.filter(e => !e.dead).map(e => e.ch).join(''); }

  _pos(i) { // els index of the i-th visible char
    for (let k = 0, n = 0; k < this.els.length; k++) if (!this.els[k].dead && n++ === i) return k;
    return -1;
  }

  insertAt(i, ch) {
    const o = i === 0 ? null : this.els[this._pos(i - 1)].id;
    const op = { c: this.client, s: ++this.seq, ts: ++this.ts, t: 'i', o, ch };
    this._apply(op);
    return op;
  }

  deleteAt(i) {
    const op = { c: this.client, s: ++this.seq, ts: ++this.ts, t: 'd', x: this.els[this._pos(i)].id };
    this._apply(op);
    return op;
  }

  // Remote op entry point: safe to call with duplicates, reordering, or gaps.
  receive(op) {
    if (op.s <= (this.vv[op.c] || 0)) return;  // already applied -> idempotent
    this.pending.push(op);
    this._drain();
  }

  // Cursors are stored as "the char I am right after" so they follow that char through concurrent edits.
  cursorAt(i) { const k = i === 0 ? -1 : this._pos(i - 1); return k < 0 ? null : this.els[k].id; }
  cursorIndex(id) { // visible index just after the anchor char (works even if that char was deleted)
    if (id === null) return 0;
    const el = this.byId.get(id);
    if (!el) return this.text.length;
    let n = 0;
    for (const e of this.els) { if (!e.dead) n++; if (e === el) break; }
    return n;
  }

  opsSince(vv) { return this.log.filter(op => op.s > (vv[op.c] || 0)); }

  _ready(op) { // per-client FIFO + causal dependency present
    if (op.s !== (this.vv[op.c] || 0) + 1) return false;
    return op.t === 'i' ? op.o === null || this.byId.has(op.o) : this.byId.has(op.x);
  }

  _drain() {
    for (let again = true; again;) {
      again = false;
      this.pending = this.pending.filter(op => {
        if (op.s <= (this.vv[op.c] || 0)) return false;
        if (!this._ready(op)) return true;
        this._apply(op);
        again = true;
        return false;
      });
    }
  }

  _apply(op) {
    this.ts = Math.max(this.ts, op.ts);
    if (op.t === 'i') {
      let i = op.o === null ? 0 : this.els.indexOf(this.byId.get(op.o)) + 1;
      while (i < this.els.length && greater(this.els[i], op)) i++; // RGA: skip concurrent inserts with larger id
      const el = { id: key(op.c, op.s), ts: op.ts, c: op.c, ch: op.ch, dead: false };
      this.els.splice(i, 0, el);
      this.byId.set(el.id, el);
    } else {
      this.byId.get(op.x).dead = true;
    }
    this.vv[op.c] = op.s;
    if (op.c === this.client) this.seq = Math.max(this.seq, op.s); // resume numbering after a reload
    this.log.push(op);
  }
}