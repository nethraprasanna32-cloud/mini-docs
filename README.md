# Mini Docs: collaborative editor (Project 17)

Run: `npm install && npm start`, then open http://localhost:3000 in 3+ tabs (`#name` in the URL picks a document).
Test: `npm test` (CRDT correctness and fault-injection suite, no dependencies needed).

## Design
- **Engine** (`crdt.js`): RGA-style sequence CRDT. Chars have unique ids `(client, seq)` and a Lamport `ts`. Deletes are tombstones. Ops are applied per-client in order and only once their dependency (origin or target) exists, so duplicates and reordering are harmless. Guarantee: strong eventual consistency.
- **State vector** `vv`: client -> highest contiguous seq applied. Two replicas sync by sending `opsSince(otherVv)`.

## WebSocket protocol (JSON)
| Message | Direction | Meaning |
|---|---|---|
| `HELLO {doc, client, vv}` | client -> server | join a document, announce what the client has |
| `SYNC {ops, vv}` | server -> client | ops the client is missing + server's vector; client then pushes what the server lacks |
| `OPS {ops}` | both | new ops; server persists and relays only newly applied ops to other clients |
| `ACK {vv}` | server -> sender | server's vector; client computes unsynced edits as `opsSince(vv)` |

Offline: edits apply locally and stay in the op log. On reconnect (exponential backoff, 0.5s to 8s) the client sends `HELLO` with its vector and the exchange above merges both sides. Server heartbeat: ws ping every 15s, dead sockets terminated.

## Known limitations (for the report)
Tombstones are never garbage-collected; `insertAt` is O(n); offline edits use localStorage; storage can be unavailable or quota-limited, and the page itself is not available from an offline server without caching; no presence/cursors.

## Offline demo procedure

1. Run `npm install && npm start` and open `http://localhost:3000/#demo` in three browser tabs.
2. Make sure the clients sync while online.
3. In tab B, select **Go offline** (this intentionally closes just that tab's WebSocket).
4. Type in tab B and separately in tabs A and C. Tab B should remain editable and show pending edits.
5. Select **Go online** in tab B. Its missing operations should merge and all three tabs should show matching content.
6. For refresh persistence, go offline in B, type, reload B, and then select **Go online**. This tests localStorage; note that reloading while the server itself is down may prevent the HTML page from loading unless the browser cached it.

Offline operations are stored in browser localStorage on each input event. Browser settings or storage quotas may prevent persistence, so verify the final result in your browser.
