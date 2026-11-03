const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 10000;
const INDEX = path.join(__dirname, 'index.html');
const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  if (url.pathname === '/health') { res.writeHead(200, {'content-type':'application/json'}); return res.end(JSON.stringify({ok:true, service:'BASE STRIKE', queued:queue.length, rooms:rooms.size})); }
  if (url.pathname === '/' || url.pathname === '/index.html') {
    fs.readFile(INDEX, (err, data) => { if (err) { res.writeHead(500); return res.end('index.html not found'); } res.writeHead(200, {'content-type':'text/html; charset=utf-8','cache-control':'no-store'}); res.end(data); });
    return;
  }
  res.writeHead(404, {'content-type':'text/plain; charset=utf-8'}); res.end('Not found');
});
const wss = new WebSocketServer({ server });
const rooms = new Map();
const queue = [];
let nextId = 1;
const uid = () => `p${nextId++}-${Math.random().toString(36).slice(2,7)}`;
const code = () => 'Q' + Math.random().toString(36).slice(2,7).toUpperCase();
const send = (ws, data) => { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data)); };
function cleanQueue(ws) { const i = queue.indexOf(ws); if (i >= 0) queue.splice(i,1); }
function getRoom(ws) { return ws.roomCode ? rooms.get(ws.roomCode) : null; }
function broadcast(room, data) { for (const p of room.players.values()) send(p.ws, data); }
function state(room) { broadcast(room, {type:'state', players:[...room.players.values()].map(p=>({id:p.id,team:p.team,x:p.x,y:p.y,z:p.z,yaw:p.yaw,hp:p.hp,weapon:p.weapon}))}); }
function addToRoom(ws, roomCode, teamHint) {
  let room = rooms.get(roomCode);
  if (!room) { room = {code:roomCode, players:new Map(), created:Date.now()}; rooms.set(roomCode, room); }
  if (room.players.size >= 4) { send(ws,{type:'error',message:'이 방은 가득 찼습니다. 다른 방을 이용하세요.'}); return false; }
  const blueCount=[...room.players.values()].filter(p=>p.team==='blue').length;
  const redCount=[...room.players.values()].filter(p=>p.team==='red').length;
  const team=teamHint || (blueCount<=redCount?'blue':'red');
  const p={id:uid(),ws,roomCode,team,x:team==='blue'?-15:15,y:1.7,z:team==='blue'?129:-129,yaw:team==='blue'?Math.PI:0,hp:200,weapon:0,lastMove:Date.now()};
  ws.playerId=p.id; ws.roomCode=roomCode; ws.queued=false; room.players.set(p.id,p);
  send(ws,{type:'joined',id:p.id,team:p.team,room:roomCode});
  state(room);
  return true;
}
function startMatch(a,b) {
  cleanQueue(a); cleanQueue(b);
  a.queued=b.queued=false;
  let roomCode=code(); while(rooms.has(roomCode)) roomCode=code();
  const room={code:roomCode,players:new Map(),created:Date.now()}; rooms.set(roomCode,room);
  addToRoom(a,roomCode,'blue'); addToRoom(b,roomCode,'red');
  broadcast(room,{type:'matchFound',room:roomCode}); state(room);
}
function tryMatch() {
  while (queue.length >= 2) {
    const a=queue.shift(), b=queue.shift();
    if (!a || !b || a.readyState!==WebSocket.OPEN || b.readyState!==WebSocket.OPEN) { if(a?.readyState===WebSocket.OPEN)queue.unshift(a); if(b?.readyState===WebSocket.OPEN)queue.unshift(b); continue; }
    startMatch(a,b);
  }
}
function removePlayer(ws) {
  cleanQueue(ws);
  const room=getRoom(ws); if (!room || !ws.playerId) { ws.roomCode=null; ws.playerId=null; tryMatch(); return; }
  const id=ws.playerId; room.players.delete(id);
  broadcast(room,{type:'left',id});
  if(room.players.size===0) rooms.delete(room.code); else state(room);
  ws.roomCode=null; ws.playerId=null; tryMatch();
}
wss.on('connection', ws => {
  ws.playerId=null; ws.roomCode=null; ws.queued=false;
  send(ws,{type:'hello',message:'BASE STRIKE matchmaking server ready'});
  ws.on('message', raw => {
    let m; try { m=JSON.parse(raw.toString()); } catch { return; }
    if (m.type==='matchmake') {
      if (ws.playerId) { send(ws,{type:'error',message:'이미 방에 들어가 있습니다. 로비로 나간 뒤 다시 시도하세요.'}); return; }
      if (!ws.queued) { ws.queued=true; queue.push(ws); send(ws,{type:'waiting',position:queue.length,message:'랜덤 매칭 대기 중'}); }
      tryMatch(); return;
    }
    if (m.type==='cancelMatchmake') { cleanQueue(ws); ws.queued=false; send(ws,{type:'matchCancelled'}); tryMatch(); return; }
    if (m.type==='join') {
      if(ws.playerId) removePlayer(ws);
      const room=String(m.room||'PUBLIC').toUpperCase().replace(/[^A-Z0-9]/g,'').slice(0,12)||'PUBLIC';
      addToRoom(ws,room); return;
    }
    const room=getRoom(ws); const p=room&&room.players.get(ws.playerId); if(!room||!p) return;
    if(m.type==='move') {
      const x=Number(m.x), y=Number(m.y), z=Number(m.z), yaw=Number(m.yaw);
      if([x,y,z,yaw].every(Number.isFinite)) { p.x=Math.max(-200,Math.min(200,x)); p.y=Math.max(0,Math.min(10,y)); p.z=Math.max(-200,Math.min(200,z)); p.yaw=yaw; p.weapon=Number.isFinite(+m.weapon)?+m.weapon:0; p.lastMove=Date.now(); }
    } else if(m.type==='grenade') {
      const x=Number(m.x),z=Number(m.z); if(Number.isFinite(x)&&Number.isFinite(z)) broadcast(room,{type:'grenadeFx',x,z,from:p.id});
    } else if(m.type==='shot') {
      const target=room.players.get(String(m.target||''));
      if(!target || target.team===p.team || target.hp<=0) return;
      const head=!!m.head; target.hp=Math.max(0,target.hp-(head?200:45));
      send(p.ws,{type:'hit',head,kill:target.hp===0});
      send(target.ws,{type:'damage',target:target.id,from:p.id});
      send(target.ws,{type:'health',id:target.id,hp:target.hp});
      state(room);
      if(target.hp===0) broadcast(room,{type:'end',winner:p.team});
    }
    if (room.players.size) state(room);
  });
  ws.on('close',()=>removePlayer(ws));
  ws.on('error',()=>removePlayer(ws));
});
setInterval(()=>{ for(const room of rooms.values()) state(room); },100);
server.listen(PORT,'0.0.0.0',()=>console.log(`BASE STRIKE server listening on ${PORT}`));
