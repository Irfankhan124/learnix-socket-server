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
const lastSeenMap = new Map();

function nowIso() {
  return new Date().toISOString();
}

function getUserPresence(userId) {
  const record = userSockets.get(userId);
  const savedLastSeen = lastSeenMap.get(userId) || null;

  if (!record || record.sockets.size === 0) {
    return {
      userId,
      online: false,
      activeThreadId: null,
      activeScreen: null,
      lastSeenAt: savedLastSeen,
      fullName: "",
      role: "",
    };
  }

  let activeThreadId = null;
  let activeScreen = "app";
  let lastSeenAt = savedLastSeen || nowIso();

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

  const time = nowIso();
  lastSeenMap.set(userId, time);

  record.sockets.delete(socket.id);

  if (record.sockets.size === 0) {
    userSockets.delete(userId);
  } else {
    userSockets.set(userId, record);
  }

  return userId;
}

app.get("/", (req, res) => {
  let totalSockets = 0;

  for (const user of userSockets.values()) {
    totalSockets += user.sockets.size;
  }

  res.json({
    ok: true,
    service: "Learnix Socket Server",
    onlineUsers: userSockets.size,
    totalSockets,
    users: Array.from(userSockets.keys()),
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

  socket.on("presence:check", ({ myUserId, otherUserId, threadId }, callback) => {
  const other = getUserPresence(otherUserId);

  const result = {
    ...other,
    inThisChat:
      Boolean(other.online) &&
      Boolean(threadId) &&
      other.activeThreadId === threadId,
  };

  if (callback) callback(result);

  if (myUserId) {
    socket.emit("presence:update", result);
  }
});
  
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

 socket.on("chat:join", ({ userId, threadId, otherUserId }) => {
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

  if (otherUserId) {
    const other = getUserPresence(otherUserId);

    socket.emit("presence:update", {
      ...other,
      inThisChat:
        Boolean(other.online) &&
        Boolean(threadId) &&
        other.activeThreadId === threadId,
    });
  }
});

  socket.on("message:react", async ({ threadId, messageId, userId, emoji }, callback) => {
  try {
    if (!threadId || !messageId || !userId) {
      throw new Error("Missing reaction data");
    }

    if (!emoji) {
      const { error } = await supabase
        .from("chat_message_reactions")
        .delete()
        .eq("message_id", messageId)
        .eq("user_id", userId);

      if (error) throw error;

      io.to(threadId).emit("message:reaction:update", {
        threadId, messageId, userId, emoji: null,
      });

      if (callback) callback({ ok: true });
      return;
    }

    const { data, error } = await supabase
      .from("chat_message_reactions")
      .upsert(
        { thread_id: threadId, message_id: messageId, user_id: userId, emoji },
        { onConflict: "message_id,user_id" }
      )
      .select("*")
      .single();

    if (error) throw error;

    io.to(threadId).emit("message:reaction:update", {
      threadId, messageId, userId, emoji, reaction: data,
    });

    if (callback) callback({ ok: true, reaction: data });
  } catch (error) {
    if (callback) callback({ ok: false, error: error.message });
  }
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

 socket.on("message:send", async (payload, callback) => {
  try {
    const {
      threadId,
      senderId,
      message = "",
      messageType = "text",
      metadata = {},
      mediaUrl = null,
      mediaType = null,
      audioDuration = null,
      replyToMessageId = null,
      clientTempId = null,
    } = payload || {};

    if (!threadId || !senderId) {
      throw new Error(
        `Missing message data: threadId=${threadId || "empty"}, senderId=${senderId || "empty"}`
      );
    }

    const finalMetadata = {
      ...(metadata || {}),
      ...(clientTempId ? { client_temp_id: clientTempId } : {}),
    };

    const { data, error } = await supabase
      .from("chat_messages")
      .insert({
        thread_id: threadId,
        sender_id: senderId,
        message: message || "",
        message_type: messageType || "text",
        metadata: finalMetadata,
        media_url: mediaUrl || null,
        media_type: mediaType || null,
        audio_duration: audioDuration || null,
        reply_to_message_id: replyToMessageId || null,
      })
      .select("*")
      .single();

    if (error) throw error;

    await supabase
      .from("chat_threads")
      .update({
        last_message:
          messageType === "audio"
            ? "🎤 Voice message"
            : messageType === "image"
            ? "📷 Photo"
            : message || "",
        updated_at: new Date().toISOString(),
      })
      .eq("id", threadId);

    io.to(threadId).emit("message:new", data);

    if (callback) callback({ ok: true, message: data });
  } catch (error) {
    console.log("❌ message:send error:", error.message);
    if (callback) callback({ ok: false, error: error.message });
  }
});
  socket.on("message:delete-for-me", async ({ messageId, userId }, callback) => {
  try {
    if (!messageId || !userId) throw new Error("Missing delete data");

    const { error } = await supabase.from("chat_message_deletes").upsert(
      {
        message_id: messageId,
        user_id: userId,
      },
      { onConflict: "message_id,user_id" }
    );

    if (error) throw error;

    if (callback) callback({ ok: true, messageId });
  } catch (error) {
    if (callback) callback({ ok: false, error: error.message });
  }
});

socket.on("message:delete-for-everyone", async ({ messageId, userId, threadId }, callback) => {
  try {
    if (!messageId || !userId || !threadId) throw new Error("Missing delete data");

    const { data: msg, error: msgError } = await supabase
      .from("chat_messages")
      .select("id, sender_id")
      .eq("id", messageId)
      .single();

    if (msgError) throw msgError;

    if (msg.sender_id !== userId) {
      throw new Error("You can only delete your own message for everyone");
    }

    const { data, error } = await supabase
      .from("chat_messages")
      .update({
        message: "This message was deleted",
        deleted_for_everyone: true,
        deleted_at: new Date().toISOString(),
        deleted_by: userId,
      })
      .eq("id", messageId)
      .select("id, thread_id, sender_id, message, message_type, metadata, created_at, deleted_for_everyone, deleted_at, deleted_by")
      .single();

    if (error) throw error;

    io.to(threadId).emit("message:deleted", data);

    if (callback) callback({ ok: true, message: data });
  } catch (error) {
    if (callback) callback({ ok: false, error: error.message });
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
