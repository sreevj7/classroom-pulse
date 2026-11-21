import asyncio
import socket
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

DECAY_SECONDS = 60        # a tap counts for 60s, then fades out
COOLDOWN = 5              # one tap per student every 5s (anti-spam)
SAMPLE_EVERY = 2          # record the class state every 2 seconds
SPIKE_MIN_PERCENT = 20    # a spike must be at least this confused
SPIKE_JUMP = 15           # ...and this far above the recent average
SPIKE_GAP = 30            # at least 30s between two spikes


class Session:
    def __init__(self):
        self.students = {}
        self.teachers = []
        self.taps = deque()        # (timestamp, student_id, topic)
        self.last_tap = {}
        self.topic = "Introduction"
        self.history = []          # timeline points
        self.spikes = []           # detected spike moments
        self.last_spike_time = 0
        self.tap_counts = {}       # total taps per topic

    def active_taps(self):
        cutoff = time.time() - DECAY_SECONDS
        while self.taps and self.taps[0][0] < cutoff:
            self.taps.popleft()
        return len(self.taps)

    def percent(self):
        n = len(self.students)
        c = self.active_taps()
        return round(100 * c / n, 1) if n else 0

    def sample(self):
        """Called every few seconds: record a point and look for a spike."""
        now = time.time()
        p = self.percent()
        recent = [h["percent"] for h in self.history[-10:]]
        baseline = sum(recent) / len(recent) if recent else 0
        self.history.append({"t": now, "percent": p, "topic": self.topic})
        if (p >= SPIKE_MIN_PERCENT
                and p - baseline >= SPIKE_JUMP
                and now - self.last_spike_time >= SPIKE_GAP):
            self.spikes.append({"t": now, "percent": p, "topic": self.topic})
            self.last_spike_time = now

    def state(self):
        return {
            "type": "state",
            "students": len(self.students),
            "confused": self.active_taps(),
            "percent": self.percent(),
            "topic": self.topic,
            "history": self.history[-150:],
            "spikes": self.spikes,
        }


session = Session()


async def broadcast_to_teachers():
    data = session.state()
    for t in list(session.teachers):
        try:
            await t.send_json(data)
        except Exception:
            if t in session.teachers:
                session.teachers.remove(t)


async def ticker():
    """Runs forever in the background, once per second."""
    count = 0
    while True:
        await asyncio.sleep(1)
        count += 1
        if count % SAMPLE_EVERY == 0:
            session.sample()
        await broadcast_to_teachers()


@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(ticker())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


def lan_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


@app.get("/")
def home():
    return FileResponse("static/student.html")


@app.get("/teacher")
def teacher_page():
    return FileResponse("static/teacher.html")


@app.get("/summary")
def summary_page():
    return FileResponse("static/summary.html")


@app.get("/api/info")
def info():
    return {"join_url": "http://" + lan_ip() + ":8000/"}


@app.get("/api/summary")
def summary_data():
    topics = {}
    for h in session.history:
        t = topics.setdefault(h["topic"], {"topic": h["topic"], "samples": []})
        t["samples"].append(h["percent"])

    result = []
    for name, t in topics.items():
        samples = t["samples"]
        avg = round(sum(samples) / len(samples), 1)
        peak = max(samples)
        spikes = sum(1 for s in session.spikes if s["topic"] == name)
        taps = session.tap_counts.get(name, 0)
        score = round(avg + 0.5 * peak + 10 * spikes, 1)
        if score >= 60:
            advice = "Revisit this topic"
        elif score >= 25:
            advice = "Quick recap needed"
        else:
            advice = "Understood well"
        result.append({
            "topic": name, "avg": avg, "peak": peak,
            "spikes": spikes, "taps": taps,
            "score": score, "advice": advice,
        })

    result.sort(key=lambda r: r["score"], reverse=True)
    return result


@app.websocket("/ws/student")
async def student_ws(ws: WebSocket):
    await ws.accept()
    sid = str(uuid.uuid4())        # anonymous random token, no names
    session.students[sid] = ws
    try:
        while True:
            msg = await ws.receive_json()
            if msg.get("type") == "confused":
                now = time.time()
                if now - session.last_tap.get(sid, 0) >= COOLDOWN:
                    session.last_tap[sid] = now
                    session.taps.append((now, sid, session.topic))
                    session.tap_counts[session.topic] = session.tap_counts.get(session.topic, 0) + 1
    except WebSocketDisconnect:
        session.students.pop(sid, None)


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
    except WebSocketDisconnect:
        if ws in session.teachers:
            session.teachers.remove(ws)