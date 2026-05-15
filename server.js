import http from "http";
import { WebSocketServer } from "ws";

const PORT = process.env.PORT || 8080;
const rooms = new Map();

function getRoom(roomId) {
  if (!rooms.has(roomId)) rooms.set(roomId, new Set());
  return rooms.get(roomId);
}

function broadcast(roomId, payload) {
  const room = getRoom(roomId);
  const message = JSON.stringify(payload);

  for (const client of room) {
    if (client.readyState === 1) {
      client.send(message);
    }
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Sottotitoli WebSocket server is running.");
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  let roomId = url.searchParams.get("room") || "default";
  ws.roomId = roomId;

  getRoom(roomId).add(ws);

  ws.send(JSON.stringify({
    type: "system",
    event: "connected",
    room: roomId
  }));

  ws.on("message", (raw) => {
    try {
      const payload = JSON.parse(raw.toString());

      if (payload.join && typeof payload.join === "string") {
        const oldRoom = ws.roomId;
        if (rooms.has(oldRoom)) rooms.get(oldRoom).delete(ws);

        ws.roomId = payload.join;
        roomId = payload.join;
        getRoom(roomId).add(ws);

        ws.send(JSON.stringify({
          type: "system",
          event: "joined",
          room: roomId
        }));
        return;
      }

      broadcast(ws.roomId, payload);
    } catch (error) {
      ws.send(JSON.stringify({
        type: "system",
        event: "error",
        message: "Invalid JSON payload"
      }));
    }
  });

  ws.on("close", () => {
    const room = rooms.get(ws.roomId);
    if (room) {
      room.delete(ws);
      if (room.size === 0) rooms.delete(ws.roomId);
    }
  });
});

server.listen(PORT, () => {
  console.log(`Sottotitoli WebSocket server listening on ${PORT}`);
});