import asyncio, json, uuid, time, base64, os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

app = FastAPI()

rooms: dict[str, dict] = {}          
clients: dict[str, WebSocket] = {}   
user_rooms: dict[str, str] = {}     
def new_room(owner_uid: str, owner_name: str, owner_color: str,
             owner_av: str, name: str, emoji: str, img: str) -> dict:
    rid = str(uuid.uuid4())[:8].upper()
    return {
        "id": rid,
        "name": name,
        "emoji": emoji,
        "imgUrl": img,
        "ownerId": owner_uid,
        "mics": [
            {"uid": owner_uid, "name": owner_name, "color": owner_color,
             "avUrl": owner_av, "talking": False, "muted": False,
             "pinned": False, "role": "host"},
            None, None, None, None
        ],
        "listeners": [owner_uid],
        "messages": [{"uid": "sys", "text": f"🎙️ {owner_name} أنشأ الغرفة",
                      "t": _t(), "type": "sys"}],
        "banned": [],
    }

def _t():
    from datetime import datetime
    return datetime.now().strftime("%H:%M")

async def broadcast_room(room_id: str, msg: dict, exclude: str | None = None):
    room = rooms.get(room_id)
    if not room:
        return
    dead = []
    for uid in list(room["listeners"]):
        if uid == exclude:
            continue
        ws = clients.get(uid)
        if ws:
            try:
                await ws.send_json(msg)
            except Exception:
                dead.append(uid)
    for uid in dead:
        await _leave_room(uid)

async def send_to(uid: str, msg: dict):
    ws = clients.get(uid)
    if ws:
        try:
            await ws.send_json(msg)
        except Exception:
            pass

async def broadcast_all(msg: dict):
    """Send to every connected client."""
    for ws in list(clients.values()):
        try:
            await ws.send_json(msg)
        except Exception:
            pass


def room_snapshot(r: dict) -> dict:
    return {
        "id": r["id"],
        "name": r["name"],
        "emoji": r["emoji"],
        "imgUrl": r["imgUrl"],
        "ownerId": r["ownerId"],
        "mics": r["mics"],
        "listenerCount": len(r["listeners"]),
        "messages": r["messages"][-200:],   # last 200 msgs
    }

def rooms_list() -> list:
    return [
        {"id": r["id"], "name": r["name"], "emoji": r["emoji"],
         "imgUrl": r["imgUrl"], "ownerId": r["ownerId"],
         "listenerCount": len(r["listeners"])}
        for r in rooms.values() if len(r["listeners"]) > 0
    ]

async def _leave_room(uid: str):
    room_id = user_rooms.get(uid)
    if not room_id or room_id not in rooms:
        user_rooms.pop(uid, None)
        return
    room = rooms[room_id]
    # remove from listeners
    if uid in room["listeners"]:
        room["listeners"].remove(uid)
    # remove from mics
    room["mics"] = [None if (m and m["uid"] == uid) else m for m in room["mics"]]
    user_rooms.pop(uid, None)

    if len(room["listeners"]) == 0:
        del rooms[room_id]
        await broadcast_all({"type": "rooms_list", "rooms": rooms_list()})
    else:
        await broadcast_room(room_id, {"type": "room_update", "room": room_snapshot(room)})
        await broadcast_all({"type": "rooms_list", "rooms": rooms_list()})


@app.websocket("/ws/{uid}")
async def ws_endpoint(ws: WebSocket, uid: str):
    await ws.accept()
    clients[uid] = ws

    # Send current rooms list on connect
    await send_to(uid, {"type": "rooms_list", "rooms": rooms_list()})

    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)
            mtype = data.get("type", "")

            if mtype == "create_room":
                # leave current room first
                if uid in user_rooms:
                    await _leave_room(uid)
                r = new_room(uid, data["name_user"], data["color"],
                             data.get("avUrl", ""),
                             data["room_name"], data["emoji"],
                             data.get("imgUrl", ""))
                rooms[r["id"]] = r
                user_rooms[uid] = r["id"]
                await send_to(uid, {"type": "joined_room", "room": room_snapshot(r)})
                await broadcast_all({"type": "rooms_list", "rooms": rooms_list()})

            # ── JOIN ROOM ─────────────────────────────────────────────────────
            elif mtype == "join_room":
                room_id = data["room_id"]
                if room_id not in rooms:
                    await send_to(uid, {"type": "error", "msg": "الغرفة غير موجودة"})
                    continue
                room = rooms[room_id]
                if uid in room.get("banned", []):
                    await send_to(uid, {"type": "error", "msg": "أنت محظور من هذه الغرفة"})
                    continue
                if uid in user_rooms:
                    await _leave_room(uid)
                if uid not in room["listeners"]:
                    room["listeners"].append(uid)
                user_rooms[uid] = room_id
                await send_to(uid, {"type": "joined_room", "room": room_snapshot(room)})
                # Notify others
                await broadcast_room(room_id, {"type": "room_update", "room": room_snapshot(room)}, exclude=uid)
                await broadcast_all({"type": "rooms_list", "rooms": rooms_list()})


            elif mtype == "leave_room":
                await _leave_room(uid)
                await send_to(uid, {"type": "left_room"})
                await broadcast_all({"type": "rooms_list", "rooms": rooms_list()})

            elif mtype == "raise_mic":
                room_id = user_rooms.get(uid)
                if not room_id or room_id not in rooms:
                    continue
                room = rooms[room_id]
                slot = data.get("slot", -1)
                # check not already on mic
                already = any(m and m["uid"] == uid for m in room["mics"])
                if already:
                    continue
                # check not pinned
                my_entry = next((m for m in room["mics"] if m and m["uid"] == uid), None)
                if my_entry and my_entry.get("pinned"):
                    continue
                if 0 <= slot < 5 and room["mics"][slot] is None:
                    room["mics"][slot] = {
                        "uid": uid, "name": data["name"], "color": data["color"],
                        "avUrl": data.get("avUrl", ""), "talking": False,
                        "muted": False, "pinned": False, "role": "spk"
                    }
                    await broadcast_room(room_id, {"type": "room_update", "room": room_snapshot(room)})

            elif mtype == "lower_mic":
                room_id = user_rooms.get(uid)
                if not room_id or room_id not in rooms:
                    continue
                room = rooms[room_id]
                room["mics"] = [None if (m and m["uid"] == uid) else m for m in room["mics"]]
                await broadcast_room(room_id, {"type": "room_update", "room": room_snapshot(room)})

            elif mtype == "move_mic":
                room_id = user_rooms.get(uid)
                if not room_id or room_id not in rooms:
                    continue
                room = rooms[room_id]
                from_slot = data.get("from_slot")
                to_slot = data.get("to_slot")
                if (from_slot is None or to_slot is None or
                        not (0 <= from_slot < 5) or not (0 <= to_slot < 5)):
                    continue
                m = room["mics"][from_slot]
                if not m or m["uid"] != uid:
                    continue
                if room["mics"][to_slot] is not None:
                    continue
                room["mics"][to_slot] = m
                room["mics"][from_slot] = None
                await broadcast_room(room_id, {"type": "room_update", "room": room_snapshot(room)})

            elif mtype == "talking":
                room_id = user_rooms.get(uid)
                if not room_id or room_id not in rooms:
                    continue
                room = rooms[room_id]
                for m in room["mics"]:
                    if m and m["uid"] == uid:
                        m["talking"] = data.get("talking", False)
                        break
                await broadcast_room(room_id, {"type": "room_update", "room": room_snapshot(room)})

            elif mtype == "chat_msg":
                room_id = user_rooms.get(uid)
                if not room_id or room_id not in rooms:
                    continue
                room = rooms[room_id]
                msg = {
                    "uid": uid,
                    "name": data["name"],
                    "color": data["color"],
                    "avUrl": data.get("avUrl", ""),
                    "text": data.get("text", "")[:500],
                    "t": _t(),
                    "type": data.get("msgType", "text"),  # text / image / video
                    "url": data.get("url", ""),
                }
                room["messages"].append(msg)
                if len(room["messages"]) > 500:
                    room["messages"] = room["messages"][-500:]
                await broadcast_room(room_id, {"type": "new_msg", "msg": msg})

            elif mtype == "admin_mute":
                room_id = user_rooms.get(uid)
                if not room_id or room_id not in rooms:
                    continue
                room = rooms[room_id]
                if room["ownerId"] != uid:
                    continue
                target_uid = data["target_uid"]
                for m in room["mics"]:
                    if m and m["uid"] == target_uid:
                        m["muted"] = not m["muted"]
                        action = "كتم" if m["muted"] else "رفع كتم"
                        room["messages"].append({
                            "uid": "sys",
                            "text": f"🔇 المشرف {action} {m['name']}",
                            "t": _t(), "type": "sys"
                        })
                        break
                await broadcast_room(room_id, {"type": "room_update", "room": room_snapshot(room)})


            elif mtype == "admin_pin":
                room_id = user_rooms.get(uid)
                if not room_id or room_id not in rooms:
                    continue
                room = rooms[room_id]
                if room["ownerId"] != uid:
                    continue
                target_uid = data["target_uid"]
                for m in room["mics"]:
                    if m and m["uid"] == target_uid:
                        m["pinned"] = not m["pinned"]
                        if m["pinned"]:
                            m["muted"] = True
                            m["talking"] = False
                        else:
                            m["muted"] = False
                        action = "تشبيص" if m["pinned"] else "رفع تشبيص"
                        room["messages"].append({
                            "uid": "sys",
                            "text": f"📌 المشرف {action} {m['name']}",
                            "t": _t(), "type": "sys"
                        })
                        break
                await broadcast_room(room_id, {"type": "room_update", "room": room_snapshot(room)})

            elif mtype == "admin_ban":
                room_id = user_rooms.get(uid)
                if not room_id or room_id not in rooms:
                    continue
                room = rooms[room_id]
                if room["ownerId"] != uid:
                    continue
                target_uid = data["target_uid"]
                target_name = data.get("target_name", "")
                if target_uid not in room.get("banned", []):
                    room.setdefault("banned", []).append(target_uid)
                # remove from mics and listeners
                room["mics"] = [None if (m and m["uid"] == target_uid) else m for m in room["mics"]]
                if target_uid in room["listeners"]:
                    room["listeners"].remove(target_uid)
                user_rooms.pop(target_uid, None)
                room["messages"].append({
                    "uid": "sys",
                    "text": f"🚫 تم حظر {target_name} من الغرفة",
                    "t": _t(), "type": "sys"
                })
                await send_to(target_uid, {"type": "banned"})
                await broadcast_room(room_id, {"type": "room_update", "room": room_snapshot(room)})
                await broadcast_all({"type": "rooms_list", "rooms": rooms_list()})

            # ── EDIT ROOM ─────────────────────────────────────────────────────
            elif mtype == "edit_room":
                room_id = user_rooms.get(uid)
                if not room_id or room_id not in rooms:
                    continue
                room = rooms[room_id]
                if room["ownerId"] != uid:
                    continue
                room["name"] = data.get("name", room["name"])[:40]
                if data.get("imgUrl"):
                    room["imgUrl"] = data["imgUrl"]
                await broadcast_room(room_id, {"type": "room_update", "room": room_snapshot(room)})
                await broadcast_all({"type": "rooms_list", "rooms": rooms_list()})

            # ── WEBRTC SIGNAL (offer/answer/ice) ──────────────────────────────
            elif mtype in ("rtc_offer", "rtc_answer", "rtc_ice"):
                target_uid = data.get("to")
                if target_uid:
                    await send_to(target_uid, {**data, "from": uid})

    except WebSocketDisconnect:
        pass
    finally:
        clients.pop(uid, None)
        await _leave_room(uid)
        await broadcast_all({"type": "rooms_list", "rooms": rooms_list()})


HTML_PATH = os.path.join(os.path.dirname(__file__), "index.html")

@app.get("/")
async def root():
    return FileResponse(HTML_PATH)


if __name__ == "__main__":
    print("\n🎙️  سواليف - Voice Rooms Server")
    print("━" * 40)
    print("✅ : http://localhost:8000")
    print("━" * 40 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
