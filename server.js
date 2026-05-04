const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// 初始化数据库
const dbPath = path.join(__dirname, 'data.db');
const db = new sqlite3.Database(dbPath);

// 创建表（如果不存在）
db.run(`
  CREATE TABLE IF NOT EXISTS room_data (
    room_id TEXT PRIMARY KEY,
    data TEXT NOT NULL
  )
`);

// 默认初始数据（与前端默认值完全一致）
function getDefaultRoomData() {
  return {
    u1: {
      name: "小茗",
      avt: "🐱",
      sign: "⭐",
      coin: 0,
      daily: [
        { name: "早晨喝水", done: false },
        { name: "学习30分钟", done: false }
      ],
      weekly: [{ name: "周末复盘", done: false }],
      check: {}
    },
    u2: {
      name: "阿悠",
      avt: "🐰",
      sign: "🌺",
      coin: 0,
      daily: [
        { name: "散步20分钟", done: false },
        { name: "整理笔记", done: false }
      ],
      weekly: [{ name: "帮助他人", done: false }],
      check: {}
    }
  };
}

// 从数据库加载房间数据，若不存在则创建默认
function loadRoomData(roomId, callback) {
  db.get('SELECT data FROM room_data WHERE room_id = ?', [roomId], (err, row) => {
    if (err) {
      console.error('数据库读取错误', err);
      callback(getDefaultRoomData());
      return;
    }
    if (row) {
      try {
        const parsed = JSON.parse(row.data);
        callback(parsed);
      } catch(e) {
        callback(getDefaultRoomData());
      }
    } else {
      const defaultData = getDefaultRoomData();
      db.run('INSERT INTO room_data (room_id, data) VALUES (?, ?)', [roomId, JSON.stringify(defaultData)], (err) => {
        if (err) console.error('插入默认数据失败', err);
      });
      callback(defaultData);
    }
  });
}

// 保存房间数据到数据库
function saveRoomData(roomId, data) {
  db.run('UPDATE room_data SET data = ? WHERE room_id = ?', [JSON.stringify(data), roomId], (err) => {
    if (err) console.error('保存数据失败', err);
  });
}

// 内存中管理每个房间的所有 WebSocket 连接
const rooms = new Map(); // roomId -> { clients: Set<WebSocket> }

function broadcastToRoom(roomId, message, excludeWs = null) {
  const room = rooms.get(roomId);
  if (!room) return;
  room.clients.forEach(client => {
    if (client !== excludeWs && client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(message));
    }
  });
}

wss.on('connection', (ws) => {
  let currentRoomId = null;

  ws.on('message', async (raw) => {
    try {
      const { type, roomId, payload } = JSON.parse(raw);
      if (!roomId) return;

      // 确保内存中有房间的 client 集合
      if (!rooms.has(roomId)) {
        rooms.set(roomId, { clients: new Set() });
      }
      const room = rooms.get(roomId);
      room.clients.add(ws);
      currentRoomId = roomId;

      if (type === 'join') {
        // 从数据库加载数据发送给该客户端
        loadRoomData(roomId, (data) => {
          ws.send(JSON.stringify({ type: 'sync', data }));
        });
      }
      else if (type === 'update') {
        // 保存最新数据到数据库
        saveRoomData(roomId, payload);
        // 广播给房间内所有其他客户端（包括自己也可以，但避免自己又渲染一次，无所谓）
        broadcastToRoom(roomId, { type: 'sync', data: payload }, ws);
        // 同时也给自己发一份确保最新（可选，但为了保持一致）
        ws.send(JSON.stringify({ type: 'sync', data: payload }));
      }
    } catch (err) {
      console.error('消息处理错误', err);
    }
  });

  ws.on('close', () => {
    if (currentRoomId) {
      const room = rooms.get(currentRoomId);
      if (room) {
        room.clients.delete(ws);
        if (room.clients.size === 0) {
          // 可选：清理无人的房间内存占用，但数据库数据保留
          rooms.delete(currentRoomId);
        }
      }
    }
  });
});

// 托管静态文件（前端页面）
app.use(express.static(path.join(__dirname, 'public')));

// 所有其他路径返回 index.html（支持前端路由）
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
  console.log(`✅ 服务器运行在 http://localhost:${PORT}`);
  console.log(`📁 数据库文件位置: ${dbPath}`);
});