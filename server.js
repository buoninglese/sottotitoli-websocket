import http from "http";
import express from "express";
import cors from "cors";
import multer from "multer";
import { WebSocketServer, WebSocket } from "ws";

const PORT = process.env.PORT || 8080;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY || "";

const rooms = new Map();
const deepgramSockets = new Map(); // roomId → { ws, lang, pendingFinal: '' }
const app = express();
app.use(express.json({ limit: '100kb' }));
app.use(cors({
  origin: (origin, cb) => {
    const allowed = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
    if (!origin || allowed.includes(origin)) return cb(null, true);
    return cb(new Error('CORS origin not allowed'));
  }
}));

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 25 * 1024 * 1024  // 25 MB max — cost guardrail
  }
});

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
  const MAX_ROOMS = 1000;
  if (!rooms.has(roomId) && rooms.size >= MAX_ROOMS) {
    ws.send(JSON.stringify({ type: "system", event: "error", message: "Server at capacity" }));
    ws.close();
    return;
  }
  const room = ensureRoom(roomId);
  const MAX_CLIENTS_PER_ROOM = 50;
  if (room.size >= MAX_CLIENTS_PER_ROOM) {
    ws.send(JSON.stringify({ type: "system", event: "error", message: "Room full" }));
    ws.close();
    return;
  }
  room.add(ws);
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

function countWords(text) {
  return (text || "").trim().split(/\s+/).filter(Boolean).length;
}

function analyzeSegments(segments) {
  const bySpeaker = {};
  let totalDuration = 0;
  let interruptions = 0;

  for (let i = 0; i < segments.length; i++) {
    const seg = segments[i];
    const speaker = seg.speaker || "Unknown";
    const start = Number(seg.start || 0);
    const end = Number(seg.end || 0);
    const duration = Math.max(0, end - start);
    const words = countWords(seg.text);

    totalDuration += duration;

    if (!bySpeaker[speaker]) {
      bySpeaker[speaker] = {
        speaker,
        turns: 0,
        duration: 0,
        words: 0
      };
    }

    bySpeaker[speaker].turns += 1;
    bySpeaker[speaker].duration += duration;
    bySpeaker[speaker].words += words;

    if (i > 0) {
      const prev = segments[i - 1];
      const prevEnd = Number(prev.end || 0);
      const gap = start - prevEnd;
      if (prev.speaker !== speaker && gap < 0.35) {
        interruptions += 1;
      }
    }
  }

  const speakers = Object.values(bySpeaker)
    .map(item => ({
      ...item,
      avgTurnDuration: item.turns ? item.duration / item.turns : 0,
      shareOfTime: totalDuration ? item.duration / totalDuration : 0
    }))
    .sort((a, b) => b.duration - a.duration);

  return {
    totalDuration,
    interruptions,
    speakers
  };
}

// ── Deepgram streaming helpers ──

function closeDeepgram(roomId) {
  const dg = deepgramSockets.get(roomId);
  if (!dg) return;
  try {
    // Send close message so Deepgram finalizes
    if (dg.ws.readyState === WebSocket.OPEN) {
      dg.ws.send(JSON.stringify({ type: "CloseStream" }));
      dg.ws.close();
    }
  } catch (e) { /* ignore */ }
  deepgramSockets.delete(roomId);
}

function startDeepgram(roomId, lang) {
  // Close any existing connection for this room
  closeDeepgram(roomId);

  if (!DEEPGRAM_API_KEY) {
    console.error("Deepgram API key not configured");
    return null;
  }

  // Map language codes to Deepgram's expected format
  const langMap = {
    'en-US': 'en', 'en-GB': 'en', 'it-IT': 'it', 'fr-FR': 'fr',
    'es-ES': 'es', 'de-DE': 'de', 'nl-NL': 'nl', 'pt-PT': 'pt',
    'pt-BR': 'pt', 'ja-JP': 'ja', 'ko-KR': 'ko', 'zh-CN': 'zh'
  };
  const deepgramLang = langMap[lang] || lang?.split('-')[0] || 'en';

  const query = new URLSearchParams({
    encoding: 'opus',
    sample_rate: '48000',
    channels: '1',
    language: deepgramLang,
    interim_results: 'true',
    punctuate: 'true',
    smart_format: 'true',
    model: 'nova-2'
  }).toString();

  const dgWs = new WebSocket(`wss://api.deepgram.com/v1/listen?${query}`, {
    headers: { Authorization: `Token ${DEEPGRAM_API_KEY}` }
  });

  const dg = { ws: dgWs, lang, pendingFinal: '' };
  deepgramSockets.set(roomId, dg);

  dgWs.on('open', () => {
    console.log(`Deepgram streaming started for room ${roomId} (${lang})`);
  });

  dgWs.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type !== 'Results') return;

      const alt = msg.channel?.alternatives?.[0];
      if (!alt?.transcript) return;

      const transcript = alt.transcript.trim();
      if (!transcript) return;

      if (msg.is_final) {
        // Final result — broadcast as caption final
        dg.pendingFinal = '';
        broadcastToRoom(roomId, {
          msg: true,
          final: transcript,
          id: Date.now(),
          label: 'deepgram'
        });
      } else {
        // Interim result
        broadcastToRoom(roomId, {
          msg: true,
          interm: transcript,
          id: Date.now()
        });
      }
    } catch (e) {
      // Ignore parse errors on Deepgram messages
    }
  });

  dgWs.on('error', (err) => {
    console.error(`Deepgram error for room ${roomId}:`, err.message);
  });

  dgWs.on('close', () => {
    console.log(`Deepgram connection closed for room ${roomId}`);
    deepgramSockets.delete(roomId);
  });

  return dg;
}

app.get("/health", (req, res) => {
  res.json({ ok: true, uptime: process.uptime() });
});

app.post("/analyze-speakers", upload.single("file"), async (req, res) => {
      if (process.env.INTERNAL_API_KEY && req.headers['x-api-key'] !== process.env.INTERNAL_API_KEY) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
  try {
    if (!OPENAI_API_KEY) {
      res.status(500).send("OPENAI_API_KEY is not configured on the server.");
      return;
    }

    if (!req.file) {
      res.status(400).send("No audio file uploaded.");
      return;
    }

    const ALLOWED_MIMES = ['audio/webm', 'audio/wav', 'audio/mpeg', 'audio/mp4', 'audio/ogg', 'audio/x-m4a'];
    if (!ALLOWED_MIMES.includes(req.file.mimetype)) {
      return res.status(400).json({ error: `Unsupported audio type: ${req.file.mimetype}` });
    }

    const formData = new FormData();
    const blob = new Blob([req.file.buffer], {
      type: req.file.mimetype || "application/octet-stream"
    });

    formData.append("file", blob, req.file.originalname || "session.webm");
    formData.append("model", "gpt-4o-transcribe-diarize");
    formData.append("response_format", "diarized_json");
    formData.append("chunking_strategy", "auto");

    const response = await fetch("https://api.openai.com/v1/audio/transcriptions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`
      },
      body: formData
    });

       if (!response.ok) {
      const text = await response.text();
      console.error('OpenAI error:', text);  // log server-side only
      res.status(502).json({ error: 'Transcription provider failed' });
      return;
    }

    const data = await response.json();
    const segments = Array.isArray(data.segments) ? data.segments : [];
    const analytics = analyzeSegments(segments);

    res.json({
      text: data.text || "",
      duration: data.duration || 0,
      segments,
      analytics
    });
  } catch (error) {
    res.status(500).send(error.message || "Speaker analysis failed.");
  }
});

const server = http.createServer(app);
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

  // Heartbeat: mark alive, respond to pings
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    try {
      const MAX_RAW_SIZE = 20000;
      if (raw.toString().length > MAX_RAW_SIZE) {
        ws.send(JSON.stringify({ type: "system", event: "error", message: "Message too large" }));
        return;
      }
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

      // ── Deepgram audio streaming messages ──
      if (payload.type === "audio-start" && DEEPGRAM_API_KEY) {
        startDeepgram(ws.roomId, payload.lang || 'en-US');
        ws.send(JSON.stringify({ type: "system", event: "deepgram-ready", room: ws.roomId }));
        return;
      }

      if (payload.type === "audio" && deepgramSockets.has(ws.roomId)) {
        const dg = deepgramSockets.get(ws.roomId);
        if (dg.ws.readyState === WebSocket.OPEN && payload.data) {
          try {
            const buf = Buffer.from(payload.data, 'base64');
            dg.ws.send(buf);
          } catch (e) {
            // Ignore bad audio data
          }
        }
        return;
      }

      if (payload.type === "audio-stop") {
        closeDeepgram(ws.roomId);
        ws.send(JSON.stringify({ type: "system", event: "deepgram-stopped", room: ws.roomId }));
        return;
      }

      const ALLOWED_TYPES = ['caption', 'translation', 'system'];
      if (payload.type && !ALLOWED_TYPES.includes(payload.type)) {
        ws.send(JSON.stringify({ type: "system", event: "error", message: "Invalid message type" }));
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
    // If this client was the only one using Deepgram for this room, clean up
    const room = rooms.get(ws.roomId);
    if (room && room.size <= 1) {
      closeDeepgram(ws.roomId);
    }
    leaveCurrentRoom(ws);
  });

  ws.on("error", () => {
    const room = rooms.get(ws.roomId);
    if (room && room.size <= 1) {
      closeDeepgram(ws.roomId);
    }
    leaveCurrentRoom(ws);
  });
});

const heartbeatInterval = setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) {
      leaveCurrentRoom(ws);
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on('close', () => clearInterval(heartbeatInterval));

server.listen(PORT, () => {
  console.log(`Sottotitoli WebSocket server listening on ${PORT}`);
});
// Clean up stale rooms every 5 minutes
setInterval(() => {
  for (const [roomId, clients] of rooms) {
    if (clients.size === 0) {
      rooms.delete(roomId);
    }
  }
}, 300000);
