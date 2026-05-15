import http from "http";
import { WebSocketServer, WebSocket } from "ws";

const PORT = process.env.PORT || 8080;
const rooms = new Map();

function ensureRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, new Set());
  }
  return rooms.get(roomId);
}

function leaveCurrentRoom(ws) {
  if (!ws.roomId) return;
  const room = rooms.get(ws.roomId);
  if (!room) return;

  room.delete(ws);
  if (room.size === 0) {
    rooms.delete(ws.roomId);
  }
}

function joinRoom(ws, roomId) {
  leaveCurrentRoom(ws);
  ws.roomId = roomId;
  ensureRoom(roomId).add(ws);
}

function broadcastToRoom(roomId, payload) {
  const room = rooms.get(roomId);
  if (!room) return 0;

  const message = JSON.stringify(payload);
  let count = 0;

  for (const client of room) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
      count += 1;
    }
  }

  return count;
}

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      ok: true,
      rooms: Array.from(rooms.keys()),
      roomCount: rooms.size
    }));
    return;
  }

  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Sottotitoli WebSocket server is running.");
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const requestedRoom = url.searchParams.get("room") || "default";

  joinRoom(ws, requestedRoom);

  ws.send(JSON.stringify({
    type: "system",
    event: "connected",
    room: ws.roomId
  }));

  ws.on("message", (raw) => {
    try {
      const payload = JSON.parse(raw.toString());

      if (payload.join && typeof payload.join === "string") {
        joinRoom(ws, payload.join);

        ws.send(JSON.stringify({
          type: "system",
          event: "joined",
          room: ws.roomId
        }));
        return;
      }

      const outbound = {
        ...payload,
        room: ws.roomId,
        serverTs: new Date().toISOString()
      };

      const delivered = broadcastToRoom(ws.roomId, outbound);

      ws.send(JSON.stringify({
        type: "system",
        event: "broadcast",
        room: ws.roomId,
        delivered
      }));
    } catch (error) {
      ws.send(JSON.stringify({
        type: "system",
        event: "error",
        room: ws.roomId,
        message: "Invalid JSON payload"
      }));
    }
  });

  ws.on("close", () => {
    leaveCurrentRoom(ws);
  });

  ws.on("error", () => {
    leaveCurrentRoom(ws);
  });
});

server.listen(PORT, () => {
  console.log(`Sottotitoli WebSocket server listening on ${PORT}`);
});