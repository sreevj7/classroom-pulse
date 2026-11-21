import asyncio
import socket
import time
import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

SAMPLE_EVERY = 2         # record a chart point every 2 seconds
SPIKE_WINDOW = 30        # seconds
SPIKE_MIN_TAPS = 3       # at least this many taps in the window...
SPIKE_PERCENT = 20       # ...or this % of the class, whichever is bigger
SPIKE_GAP = 30           # at least 30s between two spikes


class Session:
    def __init__(self):
        self.teachers = []
        self.students = {}      # sid -> {"num", "name", "online", "ws"}
        self.counter = 0
        self.reset_data()

    def reset_data(self):
        self.topic = "Introduction"
        self.topic_taps = {"Introduction": {}}    # topic -> {sid: time}
        self.history = {"Introduction": []}       # topic -> chart points
        self.spikes = []
        self.last_spike_time = 0
        self.log = []

    def who(self, sid):
        st = self.students[sid]
        return st["name"] or "Anonymous #" + str(st["num"])

    def percent_for(self, topic):
        joined = len(self.students)
        lost = len(self.topic_taps.get(topic, {}))
        return round(100 * lost / joined, 1) if joined else 0

    def sample(self):
        """Every few seconds: add a chart point for the current topic and look for a spike."""
        now = time.time()
        taps = self.topic_taps.setdefault(self.topic, {})
        joined = len(self.students)
        pct = self.percent_for(self.topic)
        self.history.setdefault(self.topic, []).append({"t": now, "percent": pct})
        recent = sum(1 for t in taps.values() if now - t <= SPIKE_WINDOW)
        need = max(SPIKE_MIN_TAPS, round(joined * SPIKE_PERCENT / 100))
        if recent >= need and now - self.last_spike_time >= SPIKE_GAP:
            self.spikes.append({"t": now, "topic": self.topic,
                                "count": recent, "percent": pct})
            self.last_spike_time = now

    def topic_rows(self):
        rows = []
        for name, taps in self.topic_taps.items():
            pct = self.percent_for(name)
            spikes = sum(1 for s in self.spikes if s["topic"] == name)
            score = pct + 10 * spikes
            if score >= 40:
                advice = "Revisit this topic"
            elif score >= 15:
                advice = "Quick recap needed"
            else:
                advice = "Understood well"
            shown = [self.students[sid]["name"] for sid in taps
                     if self.students[sid]["name"]]
            rows.append({"topic": name, "lost": len(taps),
                         "joined": len(self.students), "percent": pct,
                         "spikes": spikes, "score": score, "advice": advice,
                         "names": shown, "anonymous": len(taps) - len(shown)})
        return rows

    def state(self):
        taps = self.topic_taps.get(self.topic, {})
        return {
            "type": "state",
            "topic": self.topic,
            "joined": len(self.students),
            "online": sum(1 for s in self.students.values() if s["online"]),
            "lost": len(taps),
            "percent": self.percent_for(self.topic),
            "history": self.history.get(self.topic, [])[-300:],
            "spikes": self.spikes[-20:],
            "log": self.log[-12:],
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


async def send_student_state(sid):
    st = session.students.get(sid)
    if not st or not st.get("ws"):
        return
    try:
        await st["ws"].send_json({
            "type": "you",
            "topic": session.topic,
            "tapped": sid in session.topic_taps.get(session.topic, {}),
        })
    except Exception:
        pass


async def notify_all_students():
    for sid in list(session.students):
        await send_student_state(sid)


async def ticker():
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


def lan_ips():
    ips = []
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))
        ips.append(s.getsockname()[0])
        s.close()
    except Exception:
        pass
    try:
        for item in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ips.append(item[4][0])
    except Exception:
        pass
    out = []
    for ip in ips:
        if ip.startswith("127.") or ip.startswith("169.254.") or ip in out:
            continue
        out.append(ip)
    return out or ["127.0.0.1"]


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
def info(request: Request):
    port = request.url.port or 8000
    return {"urls": ["http://" + ip + ":" + str(port) + "/" for ip in lan_ips()]}


@app.get("/api/summary")
def summary_data():
    rows = session.topic_rows()
    rows.sort(key=lambda r: r["score"], reverse=True)
    return rows


@app.websocket("/ws/student")
async def student_ws(ws: WebSocket):
    await ws.accept()
    q = ws.query_params
    sid = (q.get("sid") or str(uuid.uuid4()))[:64]
    name = q.get("name", "").strip()[:30] if q.get("show") == "1" else ""
    st = session.students.get(sid)
    if st is None:
        session.counter += 1
        st = {"num": session.counter}
        session.students[sid] = st
    st["name"] = name
    st["ws"] = ws
    st["online"] = True
    await send_student_state(sid)
    try:
        while True:
            msg = await ws.receive_json()
            if msg.get("type") == "confused":
                taps = session.topic_taps.setdefault(session.topic, {})
                if sid not in taps:          # one tap per student per topic
                    now = time.time()
                    taps[sid] = now
                    session.log.append({"t": now, "who": session.who(sid),
                                        "topic": session.topic})
                    await broadcast_to_teachers()
                await send_student_state(sid)
    except WebSocketDisconnect:
        pass
    finally:
        # the answer stays recorded and the student stays counted
        if st.get("ws") is ws:
            st["online"] = False
            st["ws"] = None


@app.websocket("/ws/teacher")
async def teacher_ws(ws: WebSocket):
    await ws.accept()
    session.teachers.append(ws)
    await ws.send_json(session.state())
    try:
        while True:
            msg = await ws.receive_json()
            kind = msg.get("type")
            if kind == "set_topic":
                topic = str(msg.get("topic", "")).strip()[:60]
                if topic:
                    session.topic = topic          # new topic = fresh chart from 0
                    session.topic_taps.setdefault(topic, {})
                    session.history.setdefault(topic, [])
                    await notify_all_students()
                    await broadcast_to_teachers()
            elif kind == "reset":
                session.reset_data()
                await notify_all_students()
                await broadcast_to_teachers()
    except WebSocketDisconnect:
        if ws in session.teachers:
            session.teachers.remove(ws)