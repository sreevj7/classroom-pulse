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
GRACE_SECONDS = 20       # a closed tab stays counted this long (covers refreshes / Wi-Fi blips)


class Session:
    def __init__(self):
        self.teachers = []
        self.students = {}      # sid -> {"num", "name", "online", "ws", "left_at"}
        self.counter = 0
        self.reset_data()

    def reset_data(self):
        self.topic = "Introduction"
        self.topic_taps = {"Introduction": {}}    # topic -> {sid: time}  (live: only present students)
        self.topic_who = {"Introduction": {}}     # topic -> {sid: shown name or ""}  (permanent record)
        self.history = {"Introduction": []}       # topic -> chart points (permanent record)
        self.spikes = []
        self.last_spike_time = 0
        self.log = []
        self.poll = None          # the current / latest poll
        self.polls = []           # every poll, for the summary
        self.poll_counter = 0

    def who(self, sid):
        st = self.students[sid]
        return st["name"] or "Anonymous #" + str(st["num"])

    def prune(self):
        """Remove students whose tab has been closed longer than the grace period."""
        now = time.time()
        gone = [sid for sid, st in self.students.items()
                if not st["online"] and now - st.get("left_at", now) > GRACE_SECONDS]
        for sid in gone:
            del self.students[sid]
            for taps in self.topic_taps.values():
                taps.pop(sid, None)

    def percent_for(self, topic):
        joined = len(self.students)
        lost = len(self.topic_taps.get(topic, {}))
        return round(100 * lost / joined, 1) if joined else 0

    def sample(self):
        """Every few seconds: add a chart point for the current topic and look for a spike."""
        now = time.time()
        taps = self.topic_taps.setdefault(self.topic, {})
        joined = len(self.students)
        lost = len(taps)
        pct = self.percent_for(self.topic)
        self.history.setdefault(self.topic, []).append(
            {"t": now, "percent": pct, "lost": lost, "joined": joined})
        recent = sum(1 for t in taps.values() if now - t <= SPIKE_WINDOW)
        need = max(SPIKE_MIN_TAPS, round(joined * SPIKE_PERCENT / 100))
        if recent >= need and now - self.last_spike_time >= SPIKE_GAP:
            self.spikes.append({"t": now, "topic": self.topic,
                                "count": recent, "percent": pct})
            self.last_spike_time = now

    # ---------- polls ----------
    def poll_view(self, sid):
        """What one student is allowed to see about the current poll."""
        p = self.poll
        if not p:
            return None
        view = {"question": p["question"], "options": p["options"],
                "open": p["open"], "answered": p["answers"].get(sid)}
        if not p["open"]:
            view["correct"] = p["correct"]
        return view

    def poll_for_teacher(self):
        p = self.poll
        if not p:
            return None
        counts = [0] * len(p["options"])
        for choice in p["answers"].values():
            counts[choice] += 1
        right = counts[p["correct"]] if p["correct"] is not None else None
        return {"question": p["question"], "options": p["options"],
                "correct": p["correct"], "open": p["open"],
                "counts": counts, "answered": len(p["answers"]), "right": right}

    def topic_rows(self):
        """Summary rows. Uses the saved peak for each topic, so it still works after students leave."""
        rows = []
        for name in self.topic_taps:
            points = self.history.get(name, [])
            if points:
                best = max(points, key=lambda h: h["percent"])
                pct, lost, joined = best["percent"], best["lost"], best["joined"]
            else:
                pct, lost, joined = 0, 0, len(self.students)
            spikes = sum(1 for s in self.spikes if s["topic"] == name)

            topic_polls = [p for p in self.polls if p["topic"] == name]
            graded = [p for p in topic_polls if p["correct"] is not None]
            answered = sum(len(p["answers"]) for p in graded)
            right = sum(1 for p in graded for c in p["answers"].values()
                        if c == p["correct"])
            accuracy = round(100 * right / answered, 1) if answered else None

            score = pct + 10 * spikes
            if accuracy is not None:
                score += (100 - accuracy) * 0.3   # wrong poll answers count too
            score = round(score, 1)

            if score >= 40:
                advice = "Revisit this topic"
            elif score >= 15:
                advice = "Quick recap needed"
            else:
                advice = "Understood well"

            who = self.topic_who.get(name, {})
            shown = [n for n in who.values() if n]
            rows.append({"topic": name, "lost": lost, "joined": joined,
                         "percent": pct, "spikes": spikes,
                         "polls": len(topic_polls), "accuracy": accuracy,
                         "score": score, "advice": advice,
                         "names": shown, "anonymous": len(who) - len(shown)})
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
            "poll": self.poll_for_teacher(),
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
            "poll": session.poll_view(sid),
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
        session.prune()
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
    st.pop("left_at", None)
    await send_student_state(sid)
    try:
        while True:
            msg = await ws.receive_json()
            kind = msg.get("type")
            if kind == "confused":
                taps = session.topic_taps.setdefault(session.topic, {})
                if sid not in taps:          # one tap per student per topic
                    now = time.time()
                    taps[sid] = now
                    session.topic_who.setdefault(session.topic, {})[sid] = st["name"]
                    session.log.append({"t": now, "who": session.who(sid),
                                        "topic": session.topic})
                    await broadcast_to_teachers()
                await send_student_state(sid)
            elif kind == "answer":
                p = session.poll
                choice = msg.get("choice")
                if (p and p["open"] and isinstance(choice, int)
                        and 0 <= choice < len(p["options"])
                        and sid not in p["answers"]):   # one answer per poll
                    p["answers"][sid] = choice
                    await broadcast_to_teachers()
                await send_student_state(sid)
    except WebSocketDisconnect:
        pass
    finally:
        # closed tab: start the grace timer (the ticker removes them if they don't come back)
        if st.get("ws") is ws:
            st["online"] = False
            st["ws"] = None
            st["left_at"] = time.time()


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
                    session.topic_who.setdefault(topic, {})
                    session.history.setdefault(topic, [])
                    await notify_all_students()
                    await broadcast_to_teachers()
            elif kind == "start_poll":
                question = str(msg.get("question", "")).strip()[:200]
                raw = msg.get("options")
                if not isinstance(raw, list):
                    raw = []
                options = [str(o).strip()[:80] for o in raw][:4]
                options = [o for o in options if o]
                correct = msg.get("correct", -1)
                if not isinstance(correct, int) or not (0 <= correct < len(options)):
                    correct = None
                if question and len(options) >= 2:
                    if session.poll:
                        session.poll["open"] = False
                    session.poll_counter += 1
                    session.poll = {"id": session.poll_counter, "topic": session.topic,
                                    "question": question, "options": options,
                                    "correct": correct, "open": True, "answers": {}}
                    session.polls.append(session.poll)
                    await notify_all_students()
                    await broadcast_to_teachers()
            elif kind == "close_poll":
                if session.poll and session.poll["open"]:
                    session.poll["open"] = False
                    await notify_all_students()
                    await broadcast_to_teachers()
            elif kind == "reset":
                session.reset_data()
                # forget students who are gone, keep the ones still connected
                session.students = {k: v for k, v in session.students.items()
                                    if v["online"]}
                await notify_all_students()
                await broadcast_to_teachers()
    except WebSocketDisconnect:
        if ws in session.teachers:
            session.teachers.remove(ws)