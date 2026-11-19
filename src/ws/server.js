import { WebSocket, WebSocketServer } from "ws";
import { eq } from "drizzle-orm";
import { webSocketArcjet as wsArcjet } from "../arcjet.js";
import { db } from "../db/db.js";
import { commentary, matches } from "../db/schema.js";

const WS_PATH = "/ws";
const HEARTBEAT_INTERVAL = 30000;
const COMMENTARY_INTERVAL = 5000;

const matchSubscribers = new Map();

// matchId -> { timer, startedAt, busy, sequence, verified }
const commentaryGenerators = new Map();

const SAMPLE_EVENTS = [
  { eventType: "attack", message: "هجمة خطيرة على مرمى الفريق المضيف!" },
  { eventType: "pass", message: "تمريرة رائعة في وسط الملعب." },
  { eventType: "corner", message: "ركلة ركنية للفريق الضيف." },
  { eventType: "shot", message: "تسديدة قوية تمر بجوار القائم." },
  { eventType: "foul", message: "تدخل قوي وحكم المباراة يتدخل." },
  { eventType: "save", message: "الحارس ينقذ الموقف ببراعة!" },
  {
    eventType: "possession",
    message: "استحواذ كبير على الكرة في هذه الدقائق.",
  },
  { eventType: "substitution", message: "تبديل جديد على أرض الملعب." },
];

function randomEvent() {
  return SAMPLE_EVENTS[Math.floor(Math.random() * SAMPLE_EVENTS.length)];
}

async function generateCommentary(matchId) {
  const generator = commentaryGenerators.get(matchId);
  if (!generator || generator.busy) return;

  generator.busy = true;
  try {
    // تحقق مرة واحدة أن المباراة موجودة
    if (!generator.verified) {
      const [match] = await db
        .select({ id: matches.id })
        .from(matches)
        .where(eq(matches.id, matchId))
        .limit(1);

      if (!match) {
        console.warn(`[WS] Match ${matchId} not found. Stopping generator.`);
        stopCommentaryGenerator(matchId);
        return;
      }
      generator.verified = true;
    }

    // قد يكون آخر مشترك غادر أثناء الانتظار
    if (!commentaryGenerators.has(matchId)) return;

    const minute = Math.floor((Date.now() - generator.startedAt) / 60000);
    const event = randomEvent();
    generator.sequence += 1;

    const [result] = await db
      .insert(commentary)
      .values({
        matchId,
        minute,
        sequence: generator.sequence,
        period: minute < 45 ? "first_half" : "second_half",
        eventType: event.eventType,
        actor: null,
        team: null,
        message: event.message,
        metadata: { auto: true },
        tags: ["auto"],
      })
      .returning();

    const receivers = matchSubscribers.get(matchId)?.size ?? 0;
    console.log(
      `[WS] Commentary #${result?.id} inserted for match ${matchId} -> ${receivers} subscriber(s)`,
    );

    broadcastToMatch(matchId, { type: "commentary", data: result });
  } catch (error) {
    console.error(
      `[WS] Failed to generate commentary for match ${matchId}:`,
      error,
    );
  } finally {
    generator.busy = false;
  }
}

function startCommentaryGenerator(matchId) {
  if (commentaryGenerators.has(matchId)) return;

  const generator = {
    startedAt: Date.now(),
    busy: false,
    verified: false,
    sequence: 0,
    timer: setInterval(() => generateCommentary(matchId), COMMENTARY_INTERVAL),
  };
  commentaryGenerators.set(matchId, generator);
  console.log(`[WS] Commentary generator started for match ${matchId}`);
}

function stopCommentaryGenerator(matchId) {
  const generator = commentaryGenerators.get(matchId);
  if (!generator) return;

  clearInterval(generator.timer);
  commentaryGenerators.delete(matchId);
  console.log(`[WS] Commentary generator stopped for match ${matchId}`);
}

function stopAllCommentaryGenerators() {
  for (const matchId of [...commentaryGenerators.keys()]) {
    stopCommentaryGenerator(matchId);
  }
}

function subscribe(matchId, socket) {
  const id = Number(matchId);
  if (!matchSubscribers.has(id)) {
    matchSubscribers.set(id, new Set());
  }
  matchSubscribers.get(id).add(socket);
  console.log(
    `[WS] Subscribed to match ${id} (total: ${matchSubscribers.get(id).size})`,
  );

  // أول مشترك => ابدأ توليد التعليقات
  startCommentaryGenerator(id);
}

function unsubscribe(matchId, socket) {
  const id = Number(matchId);
  const subscribers = matchSubscribers.get(id);
  if (!subscribers) return;
  subscribers.delete(socket);
  if (subscribers.size === 0) {
    matchSubscribers.delete(id);
    // آخر مشترك غادر => أوقف التوليد
    stopCommentaryGenerator(id);
  }
}

function cleanUpSubscription(socket) {
  for (const matchId of socket.subscriptions) {
    unsubscribe(matchId, socket);
  }
  socket.subscriptions.clear();
}

function sendJson(socket, payload) {
  if (socket.readyState !== WebSocket.OPEN) return;
  socket.send(JSON.stringify(payload));
}

function broadcastToAll(wss, payload) {
  const message = JSON.stringify(payload);
  for (const client of wss.clients) {
    if (client.readyState !== WebSocket.OPEN) continue;
    client.send(message);
  }
}

function broadcastToMatch(matchId, payload) {
  const id = Number(matchId);
  const subscribers = matchSubscribers.get(id);

  if (!subscribers || subscribers.size === 0) {
    console.log(`[WS] No subscribers found for match ID: ${id}`);
    return;
  }

  const message = JSON.stringify(payload);
  for (const client of subscribers) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  }
}

function handleMessage(socket, data) {
  let message;
  try {
    message = JSON.parse(data.toString());
  } catch (error) {
    sendJson(socket, { type: "error", message: "Invalid Json" });
    return;
  }

  if (message?.type === "pong") {
    socket.isAlive = true;
    return;
  }

  if (message?.type === "subscribe" && message.matchId !== undefined) {
    const matchIdNum = Number(message.matchId);
    if (!Number.isFinite(matchIdNum)) {
      sendJson(socket, { type: "error", message: "Invalid matchId" });
      return;
    }
    subscribe(matchIdNum, socket);
    socket.subscriptions.add(matchIdNum);
    sendJson(socket, { type: "subscribed", matchId: matchIdNum });
    return;
  }

  if (message?.type === "unsubscribe" && message.matchId !== undefined) {
    const matchIdNum = Number(message.matchId);
    if (!Number.isFinite(matchIdNum)) {
      sendJson(socket, { type: "error", message: "Invalid matchId" });
      return;
    }
    unsubscribe(matchIdNum, socket);
    socket.subscriptions.delete(matchIdNum);
    sendJson(socket, { type: "unsubscribed", matchId: matchIdNum });
  }
}

let globalWssInstance = null;

export function broadcastMatchCreated(match) {
  if (globalWssInstance) {
    broadcastToAll(globalWssInstance, { type: "match_created", data: match });
  }
}

export function broadcastCommentary(matchId, comment) {
  broadcastToMatch(Number(matchId), { type: "commentary", data: comment });
}

export function attachWebSocketServer(server) {
  const wss = new WebSocketServer({
    noServer: true,
    maxPayload: 1024 * 1024,
  });

  globalWssInstance = wss;

  server.on("upgrade", async (req, socket, head) => {
    let pathname;
    try {
      pathname = new URL(req.url, "http://localhost").pathname;
    } catch {
      socket.destroy();
      return;
    }
    if (pathname !== WS_PATH) return;

    try {
      if (wsArcjet) {
        const decision = await wsArcjet.protect(req);
        if (decision.isDenied()) {
          socket.end(
            `HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`,
            () => socket.destroy(),
          );
          return;
        }
      }

      wss.handleUpgrade(req, socket, head, (ws) => {
        wss.emit("connection", ws, req);
      });
    } catch (error) {
      console.error("ws upgrade error:", error);
      socket.destroy();
    }
  });

  wss.on("connection", (socket) => {
    socket.isAlive = true;
    socket.subscriptions = new Set();
    console.log(`[WS] Client connected (total: ${wss.clients.size})`);

    socket.on("pong", () => {
      socket.isAlive = true;
    });

    sendJson(socket, { type: "welcome" });

    socket.on("message", (data) => {
      handleMessage(socket, data);
    });

    socket.on("close", () => {
      cleanUpSubscription(socket);
    });

    socket.on("error", (err) => {
      console.error("[WS] socket error:", err);
    });
  });

  const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
      if (ws.isAlive === false) return ws.terminate();
      ws.isAlive = false;
      ws.ping();
    });
  }, HEARTBEAT_INTERVAL);

  const cleanup = () => {
    clearInterval(interval);
    stopAllCommentaryGenerators();
  };

  wss.on("close", cleanup);
  server.on("close", cleanup);

  return { broadcastMatchCreated, broadcastCommentary };
}
