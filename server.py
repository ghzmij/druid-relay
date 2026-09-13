import asyncio
import json
import os
import time
from aiohttp import web, WSMsgType

APP_VERSION = "DRUID Relay Stage 1"
rooms = {}
lock = asyncio.Lock()

async def health(request):
    return web.json_response({"service":"DRUID Relay","stage":1,"status":"OK",
                              "version":APP_VERSION,"connected_rooms":len(rooms),
                              "time":int(time.time())})

async def index(request):
    return web.Response(text="DRUID Relay Stage 1 - ONLINE\n")

async def websocket_handler(request):
    ws = web.WebSocketResponse(heartbeat=25, receive_timeout=90,
                               max_msg_size=1024*1024)
    await ws.prepare(request)
    room = role = None
    try:
        first = await ws.receive(timeout=15)
        if first.type != WSMsgType.TEXT:
            await ws.close(code=4000, message=b"registration required")
            return ws
        try:
            reg = json.loads(first.data)
        except Exception:
            await ws.close(code=4001, message=b"bad registration")
            return ws
        room = str(reg.get("room","")).strip()
        role = str(reg.get("role","")).strip().lower()
        if reg.get("type") != "register" or not room or len(room) > 128 or role not in ("windows","android"):
            await ws.close(code=4002, message=b"bad registration")
            return ws
        async with lock:
            slot = rooms.setdefault(room,{})
            old = slot.get(role)
            if old is not None and not old.closed:
                await old.close(code=4004, message=b"replaced")
            slot[role] = ws
        await ws.send_json({"type":"registered","role":role})
        peer_role = "android" if role == "windows" else "windows"
        async for msg in ws:
            if msg.type not in (WSMsgType.TEXT, WSMsgType.BINARY):
                if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                    break
                continue
            async with lock:
                peer = rooms.get(room,{}).get(peer_role)
            if peer is None or peer.closed:
                await ws.send_json({"type":"peer_offline"})
                continue
            if msg.type == WSMsgType.BINARY:
                await peer.send_bytes(msg.data)
            else:
                await peer.send_str(msg.data)
    except asyncio.TimeoutError:
        pass
    finally:
        if room and role:
            async with lock:
                slot = rooms.get(room)
                if slot and slot.get(role) is ws:
                    slot.pop(role,None)
                    if not slot:
                        rooms.pop(room,None)
        if not ws.closed:
            await ws.close()
    return ws

app = web.Application(client_max_size=1024*1024)
app.router.add_get("/", index)
app.router.add_get("/health", health)
app.router.add_get("/relay", websocket_handler)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=int(os.environ.get("PORT","10000")))
