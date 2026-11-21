import time
import uuid
from collections import deque
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

DECAY_SECONDS = 60      # a tap counts for 60s, then fades out
COOLDOWN = 5            # one tap per student every 5s (anti-spam)

class Session:
    def __init__(self):
        self.students = {}
        self.teachers = []
        self.taps = deque()          # (timestamp, student_id, topic)
        self.last_tap = {}
        self.topic = "Introduction"

    def active_taps(self):
        cutoff = time.time() - DECAY_SECONDS
        while self.taps and self.taps[0][0] < cutoff:
            self.taps.popleft()
        return len(self.taps)

    def state(self):
        n = len(self.students)
        c = self.active_taps()
        return {
            "type": "state",
            "students": n,
            "confused": c,
            "percent": round(100 * c / n, 1) if n else 0,
            "topic": self.topic,
        }

session = Session()

async def broadcast_to_teachers():
    data = session.state()
    for t in list(session.teachers):
        try:
            await t.send_json(data)
        except Exception:
            session.teachers.remove(t)

@app.get("/")
def home():
    return FileResponse("static/student.html")

@app.get("/teacher")
def teacher_page():
    return FileResponse("static/teacher.html")

@app.websocket("/ws/student")
async def student_ws(ws: WebSocket):
    await ws.accept()
    sid = str(uuid.uuid4())           # anonymous random token, no names
    session.students[sid] = ws
    await broadcast_to_teachers()
    try:
        while True:
            msg = await ws.receive_json()
            if msg.get("type") == "confused":
                now = time.time()
                if now - session.last_tap.get(sid, 0) >= COOLDOWN:
                    session.last_tap[sid] = now
                    session.taps.append((now, sid, session.topic))
                    await broadcast_to_teachers()
    except WebSocketDisconnect:
        session.students.pop(sid, None)
        await broadcast_to_teachers()

@app.websocket("/ws/teacher")
async def teacher_ws(ws: WebSocket):
    await ws.accept()
    session.teachers.append(ws)
    await ws.send_json(session.state())
    try:
        while True:
            msg = await ws.receive_json()
            if msg.get("type") == "set_topic":
                session.topic = msg["topic"]
                await broadcast_to_teachers()
    except WebSocketDisconnect:
        if ws in session.teachers:
            session.teachers.remove(ws)