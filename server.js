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
  pingTimeout: 12000,
});

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const onlineUsers = new Map();
const activeChats = new Map();

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "Learnix Socket Server",
    onlineUsers: onlineUsers.size,
  });
});

function emitPresence(userId) {
  const user = onlineUsers.get(userId);
  io.emit("presence:update", {
    userId,
    online: Boolean(user),
    activeThreadId: user?.activeThreadId || null,
    activeScreen: user?.activeScreen || null,
    lastSeenAt: new Date().toISOString(),
  });
}

io.on("connection", (socket) => {
  socket.on("user:online", ({ userId, fullName, role }) => {
    if (!userId) return;

    onlineUsers.set(userId, {
      socketId: socket.id,
      fullName: fullName || "",
      role: role || "",
      activeThreadId: null,
      activeScreen: "app",
      lastSeenAt: new Date().toISOString(),
    });

    socket.data.userId = userId;

    emitPresence(userId);
  });

  socket.on("chat:join", ({ userId, threadId }) => {
    if (!userId || !threadId) return;

    socket.join(threadId);

    const old = onlineUsers.get(userId) || {};
    onlineUsers.set(userId, {
      ...old,
      socketId: socket.id,
      activeThreadId: threadId,
      activeScreen: "chat",
      lastSeenAt: new Date().toISOString(),
    });

    activeChats.set(userId, threadId);

    socket.to(threadId).emit("chat:user-active", {
      userId,
      threadId,
    });

    emitPresence(userId);
  });

  socket.on("chat:leave", ({ userId, threadId }) => {
    if (!userId || !threadId) return;

    socket.leave(threadId);

    const old = onlineUsers.get(userId);
    if (old) {
      onlineUsers.set(userId, {
        ...old,
        activeThreadId: null,
        activeScreen: "app",
        lastSeenAt: new Date().toISOString(),
      });
    }

    activeChats.delete(userId);

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
      if (!threadId || !senderId || !message?.trim()) {
        throw new Error("Missing message data");
      }

      const { data, error } = await supabase
        .from("chat_messages")
        .insert({
          thread_id: threadId,
          sender_id: senderId,
          message: message.trim(),
          message_type: "text",
          metadata: {},
        })
        .select("id, thread_id, sender_id, message, message_type, metadata, created_at")
        .single();

      if (error) throw error;

      await supabase
        .from("chat_threads")
        .update({ updated_at: new Date().toISOString() })
        .eq("id", threadId);

      io.to(threadId).emit("message:new", data);

      if (callback) callback({ ok: true, message: data });
    } catch (error) {
      if (callback) callback({ ok: false, error: error.message });
    }
  });

  socket.on("message:seen", async ({ threadId, userId }) => {
    if (!threadId || !userId) return;

    const now = new Date().toISOString();

    await supabase.from("chat_thread_reads").upsert(
      {
        thread_id: threadId,
        user_id: userId,
        last_read_at: now,
        updated_at: now,
      },
      { onConflict: "thread_id,user_id" }
    );

    io.to(threadId).emit("message:seen:update", {
      threadId,
      userId,
      lastReadAt: now,
    });
  });

  socket.on("disconnect", () => {
    const userId = socket.data.userId;

    if (!userId) return;

    const user = onlineUsers.get(userId);
    const threadId = user?.activeThreadId;

    onlineUsers.delete(userId);
    activeChats.delete(userId);

    if (threadId) {
      socket.to(threadId).emit("chat:user-left", {
        userId,
        threadId,
      });
    }

    emitPresence(userId);
  });
});

const PORT = process.env.PORT || 10000;

server.listen(PORT, () => {
  console.log(`Learnix socket server running on ${PORT}`);
});