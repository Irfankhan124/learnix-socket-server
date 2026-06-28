const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const { createClient } = require("@supabase/supabase-js");

require("dotenv").config();

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
  pingInterval: 10000,
  pingTimeout: 15000,
});

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

/**
 * userSockets:
 * userId => {
 *   userId,
 *   fullName,
 *   role,
 *   sockets: Map(socketId => { activeThreadId, activeScreen, lastSeenAt })
 * }
 */
const userSockets = new Map();

function nowIso() {
  return new Date().toISOString();
}

function getUserPresence(userId) {
  const record = userSockets.get(userId);

  if (!record || record.sockets.size === 0) {
    return {
      userId,
      online: false,
      activeThreadId: null,
      activeScreen: null,
      lastSeenAt: null,
      fullName: "",
      role: "",
    };
  }

  let activeThreadId = null;
  let activeScreen = "app";
  let lastSeenAt = null;

  for (const item of record.sockets.values()) {
    if (item.activeThreadId) {
      activeThreadId = item.activeThreadId;
      activeScreen = "chat";
      lastSeenAt = item.lastSeenAt;
      break;
    }

    lastSeenAt = item.lastSeenAt;
  }

  return {
    userId,
    online: true,
    activeThreadId,
    activeScreen,
    lastSeenAt,
    fullName: record.fullName || "",
    role: record.role || "",
  };
}

function emitPresence(userId) {
  io.emit("presence:update", getUserPresence(userId));
}

function addOrUpdateUserSocket({ socket, userId, fullName, role }) {
  if (!userId) return;

  let record = userSockets.get(userId);

  if (!record) {
    record = {
      userId,
      fullName: fullName || "",
      role: role || "",
      sockets: new Map(),
    };
  }

  record.fullName = fullName || record.fullName || "";
  record.role = role || record.role || "";

  const oldSocketData = record.sockets.get(socket.id) || {};

  record.sockets.set(socket.id, {
    activeThreadId: oldSocketData.activeThreadId || null,
    activeScreen: oldSocketData.activeScreen || "app",
    lastSeenAt: nowIso(),
  });

  userSockets.set(userId, record);

  socket.data.userId = userId;
}

function updateSocketActivity({ socket, activeThreadId = null, activeScreen = "app" }) {
  const userId = socket.data.userId;
  if (!userId) return;

  const record = userSockets.get(userId);
  if (!record) return;

  const oldSocketData = record.sockets.get(socket.id) || {};

  record.sockets.set(socket.id, {
    ...oldSocketData,
    activeThreadId,
    activeScreen,
    lastSeenAt: nowIso(),
  });

  userSockets.set(userId, record);
}

function removeUserSocket(socket) {
  const userId = socket.data.userId;
  if (!userId) return null;

  const record = userSockets.get(userId);
  if (!record) return userId;

  record.sockets.delete(socket.id);

  if (record.sockets.size === 0) {
    userSockets.delete(userId);
  } else {
    userSockets.set(userId, record);
  }

  return userId;
}

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "Learnix Socket Server",
    onlineUsers: userSockets.size,
    time: nowIso(),
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    onlineUsers: userSockets.size,
    time: nowIso(),
  });
});

io.on("connection", (socket) => {
  socket.on("user:online", ({ userId, fullName, role }) => {
    if (!userId) return;

    addOrUpdateUserSocket({
      socket,
      userId,
      fullName,
      role,
    });

    updateSocketActivity({
      socket,
      activeThreadId: null,
      activeScreen: "app",
    });

    socket.emit("presence:ready", {
      userId,
      online: true,
    });

    emitPresence(userId);
  });

  socket.on("presence:get", ({ userIds }, callback) => {
    const result = {};

    (userIds || []).forEach((userId) => {
      result[userId] = getUserPresence(userId);
    });

    if (callback) callback(result);
  });

  socket.on("chat:join", ({ userId, threadId }) => {
    if (!userId || !threadId) return;

    if (!socket.data.userId) {
      addOrUpdateUserSocket({
        socket,
        userId,
        fullName: "",
        role: "",
      });
    }

    socket.join(threadId);

    updateSocketActivity({
      socket,
      activeThreadId: threadId,
      activeScreen: "chat",
    });

    socket.to(threadId).emit("chat:user-active", {
      userId,
      threadId,
    });

    emitPresence(userId);
  });

  socket.on("chat:leave", ({ userId, threadId }) => {
    if (!userId || !threadId) return;

    socket.leave(threadId);

    updateSocketActivity({
      socket,
      activeThreadId: null,
      activeScreen: "app",
    });

    socket.to(threadId).emit("chat:user-left", {
      userId,
      threadId,
    });

    emitPresence(userId);
  });

  socket.on("typing:start", ({ userId, threadId }) => {
    if (!userId || !threadId) return;

    socket.to(threadId).emit("typing:update", {
      userId,
      threadId,
      isTyping: true,
    });
  });

  socket.on("typing:stop", ({ userId, threadId }) => {
    if (!userId || !threadId) return;

    socket.to(threadId).emit("typing:update", {
      userId,
      threadId,
      isTyping: false,
    });
  });

  socket.on("message:send", async ({ threadId, senderId, message }, callback) => {
    try {
      const cleanMessage = String(message || "").trim();

      if (!threadId || !senderId || !cleanMessage) {
        throw new Error("Missing message data");
      }

      const { data, error } = await supabase
        .from("chat_messages")
        .insert({
          thread_id: threadId,
          sender_id: senderId,
          message: cleanMessage,
          message_type: "text",
          metadata: {},
        })
        .select(
          "id, thread_id, sender_id, message, message_type, metadata, created_at"
        )
        .single();

      if (error) throw error;

      await supabase
        .from("chat_threads")
        .update({ updated_at: nowIso() })
        .eq("id", threadId);

      io.to(threadId).emit("message:new", data);

      if (callback) {
        callback({
          ok: true,
          message: data,
        });
      }
    } catch (error) {
      if (callback) {
        callback({
          ok: false,
          error: error.message || "Message failed",
        });
      }
    }
  });

  socket.on("message:seen", async ({ threadId, userId }) => {
    if (!threadId || !userId) return;

    const time = nowIso();

    const { error } = await supabase.from("chat_thread_reads").upsert(
      {
        thread_id: threadId,
        user_id: userId,
        last_read_at: time,
        updated_at: time,
      },
      { onConflict: "thread_id,user_id" }
    );

    if (error) {
      console.log("message:seen error:", error.message);
      return;
    }

    io.to(threadId).emit("message:seen:update", {
      threadId,
      userId,
      lastReadAt: time,
    });
  });

  socket.on("disconnect", () => {
    const oldUserId = socket.data.userId;
    const oldRecord = oldUserId ? userSockets.get(oldUserId) : null;
    const oldSocketData = oldRecord?.sockets?.get(socket.id);
    const oldThreadId = oldSocketData?.activeThreadId || null;

    const userId = removeUserSocket(socket);

    if (!userId) return;

    if (oldThreadId) {
      socket.to(oldThreadId).emit("chat:user-left", {
        userId,
        threadId: oldThreadId,
      });
    }

    emitPresence(userId);
  });
});

const PORT = process.env.PORT || 10000;

server.listen(PORT, () => {
  console.log(`Learnix socket server running on port ${PORT}`);
});
