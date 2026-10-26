// OpenDesk relay — the small public server you deploy once.
//
// Job: let two OpenDesk apps find each other by a 9-digit ID and pass data
// between them, so neither needs an open inbound port. This is the piece that
// makes "share your number, friend connects from anywhere" work.
//
// Protocol (all JSON unless noted):
//   endpoint -> relay  {type:"register", id?}      announce "I'm reachable as <id>"
//     relay -> endpoint {type:"registered", id}     id assigned/confirmed
//   endpoint -> relay  {type:"connect", target}    "pair me with host <target>"
//     relay -> both     {type:"paired"} / {type:"incoming"} / {type:"error",reason}
//   after pairing: every other JSON message and every BINARY frame is forwarded
//   verbatim to the paired peer.
//
// Runs on Render/Railway/Fly/any Node host. Listens on process.env.PORT.

import http from "http";
import { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT || 8080);

const registry = new Map(); // id -> ws (endpoints waiting to be controlled)

function makeId() {
  let id;
  do {
    id = String(Math.floor(100000000 + Math.random() * 900000000)); // 9 digits
  } while (registry.has(id));
  return id;
}

// Minimal HTTP surface so hosting platforms see a healthy web service.
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { "content-type": "text/plain" });
  res.end("OpenDesk relay is running.\n");
});

const wss = new WebSocketServer({ server: httpServer });

function unpair(ws) {
  const peer = ws.peer;
  if (peer) {
    peer.peer = null;
    if (peer.readyState === peer.OPEN) peer.send(JSON.stringify({ type: "peer-left" }));
    ws.peer = null;
  }
}

wss.on("connection", (ws) => {
  ws.id = null;
  ws.peer = null;

  ws.on("message", (data, isBinary) => {
    // Once paired, forward media/input straight through.
    if (isBinary) {
      if (ws.peer && ws.peer.readyState === ws.peer.OPEN) ws.peer.send(data, { binary: true });
      return;
    }

    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.type === "register") {
      const id = msg.id && !registry.has(msg.id) ? String(msg.id) : makeId();
      ws.id = id;
      registry.set(id, ws);
      ws.send(JSON.stringify({ type: "registered", id }));
      return;
    }

    if (msg.type === "connect") {
      const host = registry.get(String(msg.target || ""));
      if (!host || host.readyState !== host.OPEN) {
        ws.send(JSON.stringify({ type: "error", reason: "not-online" }));
        return;
      }
      if (host.peer || ws.peer) {
        ws.send(JSON.stringify({ type: "error", reason: "busy" }));
        return;
      }
      ws.peer = host;
      host.peer = ws;
      host.send(JSON.stringify({ type: "incoming" }));   // host: start streaming
      ws.send(JSON.stringify({ type: "paired" }));        // controller: show view
      return;
    }

    // Anything else (input events, screen-info, …): forward to the peer.
    if (ws.peer && ws.peer.readyState === ws.peer.OPEN) ws.peer.send(data.toString());
  });

  ws.on("close", () => {
    if (ws.id && registry.get(ws.id) === ws) registry.delete(ws.id);
    unpair(ws);
  });
  ws.on("error", () => {});
});

httpServer.listen(PORT, () => {
  console.log(`OpenDesk relay listening on :${PORT}`);
});
