const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const dbPath = path.join(__dirname, 'data.db');
const db = new sqlite3.Database(dbPath);

db.run(`CREATE TABLE IF NOT EXISTS room_data (room_id TEXT PRIMARY KEY, data TEXT NOT NULL)`);

function getDefaultRoomData() {
  return {
    u1: { name: "小茗", avt: "🐱", sign: "⭐", coin: 0, daily: [{ name: "早晨喝水", done: false }, { name: "学习30分钟", done: false }], weekly: [{ name: "周末复盘", done: false }], check: {} },
    u2: { name: "阿悠", avt: "🐰", sign: "🌺", coin: 0, daily: [{ name: "散步20分钟", done: false }, { name: "整理笔记", done: false }], weekly: [{ name: "帮助他人", done: false }], check: {} }
  };
}

function loadRoomData(roomId, callback) {
  db.get('SELECT data FROM room_data WHERE room_id = ?', [roomId], (err, row) => {
    if (err || !row) {
      const defaultData = getDefaultRoomData();
      db.run('INSERT INTO room_data (room_id, data) VALUES (?, ?)', [roomId, JSON.stringify(defaultData)], (err) => { if (err) console.error(err); });
      callback(defaultData);
    } else {
      try { callback(JSON.parse(row.data)); } catch(e) { callback(getDefaultRoomData()); }
    }
  });
}

function saveRoomData(roomId, data) {
  db.run('UPDATE room_data SET data = ? WHERE room_id = ?', [JSON.stringify(data), roomId], (err) => { if (err) console.error(err); });
}

// 存储房间信息：clients 是 WebSocket 对象 Set，data 是房间数据（内存缓存）
const rooms = new Map(); // roomId -> { clients: Set, data: object }

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

      // ========== 房间人数限制检查（仅在 join 时）==========
      if (type === 'join') {
        // 初始化 room 结构（如果不存在）
        if (!rooms.has(roomId)) {
          rooms.set(roomId, { clients: new Set(), data: null });
        }
        const room = rooms.get(roomId);
        
        // 检查当前房间已有连接数量
        if (room.clients.size >= 2) {
          // 发送拒绝消息并关闭连接
          ws.send(JSON.stringify({ type: 'error', message: '房间人数已满（最多2人）' }));
          ws.close();
          return;
        }
        
        // 允许加入
        room.clients.add(ws);
        currentRoomId = roomId;

        // 加载数据库数据到内存（如果尚未加载）
        if (!room.data) {
          loadRoomData(roomId, (data) => {
            room.data = data;
            ws.send(JSON.stringify({ type: 'sync', data }));
          });
        } else {
          ws.send(JSON.stringify({ type: 'sync', data: room.data }));
        }
      }
      else if (type === 'update') {
        const room = rooms.get(roomId);
        if (!room) return;
        if (!room.clients.has(ws)) {
          // 未加入房间的非法更新，忽略
          return;
        }
        // 更新内存数据
        room.data = payload;
        // 保存到数据库
        saveRoomData(roomId, payload);
        // 广播给房间内所有客户端（包括自己，但不加排除）
        broadcastToRoom(roomId, { type: 'sync', data: payload });
      }
    } catch (err) { console.error('消息处理错误', err); }
  });

  ws.on('close', () => {
    if (currentRoomId) {
      const room = rooms.get(currentRoomId);
      if (room) {
        room.clients.delete(ws);
        // 如果房间空了，可以选择保留内存数据（不清除），以便下次快速恢复
        if (room.clients.size === 0) {
          // 可选：保留 room.data，删除 rooms 条目？若保留，下次加入可直接使用内存数据
          // 为节省内存，可以删除，但会重新从数据库读取。为了快速，不删除也行
          // 这里保留数据，但若长期无人加入，也无大碍
        }
      }
    }
  });
});

app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => { res.sendFile(path.join(__dirname, 'public', 'index.html')); });

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => { console.log(`✅ 服务器运行在 http://localhost:${PORT}`); });