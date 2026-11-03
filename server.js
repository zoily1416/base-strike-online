
const http = require("http");
const fs = require("fs");
const path = require("path");
const WebSocket = require("ws");
const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const server = http.createServer((req,res)=>{
  const pathname = new URL(req.url,"http://localhost").pathname;
  if(pathname==="/health"){res.writeHead(200,{"content-type":"application/json"});return res.end(JSON.stringify({ok:true,service:"BASE STRIKE"}));}
  const file = pathname==="/" ? "index.html" : path.basename(decodeURIComponent(pathname));
  const full = path.join(ROOT,file);
  if(file!=="index.html" || !fs.existsSync(full)){res.writeHead(404);return res.end("Not found");}
  res.writeHead(200,{"content-type":"text/html; charset=utf-8","cache-control":"no-store"});
  fs.createReadStream(full).pipe(res);
});
const wss = new WebSocket.Server({server});
const rooms = new Map(); let nextId=1;
const send=(ws,obj)=>{if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(obj));};
function roomState(room){return {type:"state",players:[...room.players.values()].map(p=>({id:p.id,team:p.team,x:p.x,z:p.z,yaw:p.yaw,weapon:p.weapon,hp:p.hp}))};}
function broadcast(room,obj){for(const p of room.players.values())send(p.ws,obj);}
function sync(room){broadcast(room,roomState(room));}
wss.on("connection",ws=>{
  ws.alive=true; ws.on("pong",()=>ws.alive=true);
  ws.on("message",raw=>{
    let m; try{m=JSON.parse(raw.toString())}catch{return}
    if(m.type==="join"){
      const code=String(m.room||"PUBLIC").toUpperCase().replace(/[^A-Z0-9]/g,"").slice(0,12)||"PUBLIC";
      let room=rooms.get(code); if(!room){room={players:new Map(),code};rooms.set(code,room)}
      if(room.players.size>=4)return send(ws,{type:"error",message:"방이 가득 찼습니다. (최대 4명)"});
      const id="p"+(nextId++), team=[...room.players.values()].filter(p=>p.team==="blue").length <= [...room.players.values()].filter(p=>p.team==="red").length?"blue":"red";
      const p={id,ws,room:code,team,x:team==="blue"?-15:15,y:1.7,z:team==="blue"?129:-129,yaw:team==="blue"?Math.PI:0,weapon:0,hp:200,lastMove:0};
      ws.player=p; ws.room=room; room.players.set(id,p);
      send(ws,{type:"joined",id,team,room:code}); sync(room);
      return;
    }
    const p=ws.player,room=ws.room;if(!p||!room)return;
    if(m.type==="move"){
      const now=Date.now(); if(now-p.lastMove<25)return; p.lastMove=now;
      const x=Number(m.x),z=Number(m.z),yaw=Number(m.yaw);
      if(Number.isFinite(x)&&Number.isFinite(z)&&Number.isFinite(yaw)&&Math.abs(x)<180&&Math.abs(z)<180){p.x=x;p.z=z;p.yaw=yaw;p.weapon=Math.max(0,Math.min(8,Number(m.weapon)||0));sync(room)}
    } else if(m.type==="shot"){
      const target=room.players.get(String(m.target||""));
      if(!target||target.team===p.team||target.hp<=0)return;
      const dx=target.x-p.x,dz=target.z-p.z,dist=Math.hypot(dx,dz);
      const angle=Math.atan2(-dx,-dz),diff=Math.atan2(Math.sin(angle-p.yaw),Math.cos(angle-p.yaw));
      if(dist>220||Math.abs(diff)>.30)return;
      target.hp=Math.max(0,target.hp-(m.head?200:35));
      broadcast(room,{type:"hit",kill:target.hp===0,head:!!m.head});
      send(target.ws,{type:"damage",target:target.id});
      send(target.ws,{type:"health",id:target.id,hp:target.hp});
      sync(room);
      if(target.hp===0){broadcast(room,{type:"end",winner:p.team});}
    } else if(m.type==="grenade"){
      const x=Number(m.x),z=Number(m.z); if(Number.isFinite(x)&&Number.isFinite(z))broadcast(room,{type:"grenadeFx",x,z});
    }
  });
  ws.on("close",()=>{
    const p=ws.player,room=ws.room;if(!p||!room)return;
    room.players.delete(p.id);broadcast(room,{type:"left",id:p.id});sync(room);
    if(!room.players.size)rooms.delete(room.code);
  });
});
const heartbeat=setInterval(()=>{for(const ws of wss.clients){if(!ws.alive){ws.terminate();continue}ws.alive=false;ws.ping()}},30000);
server.listen(PORT,"0.0.0.0",()=>console.log(`BASE STRIKE server listening on ${PORT}`));
process.on("SIGTERM",()=>{clearInterval(heartbeat);server.close(()=>process.exit(0))});
