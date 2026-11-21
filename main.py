import asyncio
import socket
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

DECAY_SECONDS = 60         # a tap counts for 60s, then fades out
COOLDOWN = 5               # one tap per student every 5s (anti-spam)
SAMPLE_EVERY = 2           # record the class state every 2 seconds
SPIKE_MIN_PERCENT = 20     # a spike must be at least this confused
SPIKE_JUMP = 15            # ...and this far above the recent average
SPIKE_GAP = 30             # at least 30s between two spikes
REEXPLAIN_WINDOW = 30      # seconds students have to answer "clearer now?"
REEXPLAIN_LOOKBACK = 180   # "was lost" = tapped in the last 3 minutes

REASONS = {
    "fast": "Too fast",
    "concept": "Don't get the concept",
    "example": "Need an example",
    "access": "Can't see or hear",
}
ADVICE = {
    "fast": "Slow down a little",
    "concept": "Explain the idea another way",
    "example": "Give a worked example",
    "access": "Check the audio, board or screen",
}


class Session:
    def __init__(self):
        self.students = {}         # sid -> websocket
        self.teachers = []
        self.taps = deque()        # live taps: {"t", "sid", "topic", "reason"}
        self.last_tap = {}         # sid -> time of last tap (cooldown)
        self.last_entry = {}       # sid -> their latest tap entry
        self.tap_times = {}        # sid -> when they were last "lost"
        self.topic = "Introduction"
        self.history = []          # timeline points
        self.spikes = []           # detected spike moments
        self.last_spike_time = 0
        self.tap_counts = {}       # total taps per topic
        self.reasons = {}          # topic -> {reason: count}
        self.reexplains = []       # every re-explain check
        self.current = None        # the latest re-explain check

    # ---------- confusion ----------
    def active_taps(self):
        cutoff = time.time() - DECAY_SECONDS
        while self.taps and self.taps[0]["t"] < cutoff:
            self.taps.popleft()
        return len(self.taps)

    def percent(self):
        n = len(self.students)
        c = self.active_taps()
        return min(100.0, round(100 * c / n, 1)) if n else 0

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

    # ---------- reasons ----------
    def add_reason(self, sid, reason):
        if reason not in REASONS:
            return
        entry = self.last_entry.get(sid)
        if entry and entry["reason"] is None:
            entry["reason"] = reason
            d = self.reasons.setdefault(entry["topic"], {})
            d[reason] = d.get(reason, 0) + 1

    def live_reasons(self):
        counts = {}
        for e in self.taps:
            if e["reason"]:
                counts[e["reason"]] = counts.get(e["reason"], 0) + 1
        ordered = sorted(counts.items(), key=lambda kv: -kv[1])
        rows = [{"label": REASONS[k], "count": v} for k, v in ordered]
        suggestion = ADVICE[ordered[0][0]] if ordered else None
        return rows, suggestion

    # ---------- re-explain loop ----------
    def start_reexplain(self):
        """Returns an error message, or None when the check started."""
        now = time.time()
        if self.current and self.current["open"]:
            return "A check is already running."
        targets = {sid for sid, t in self.tap_times.items()
                   if sid in self.students and now - t <= REEXPLAIN_LOOKBACK}
        if not targets:
            return "Nobody has signalled lately, so there is nobody to re-check."
        recent = [h["percent"] for h in self.history if now - h["t"] <= 90]
        before = max(recent + [self.percent()])
        ev = {"id": len(self.reexplains) + 1, "t": now, "topic": self.topic,
              "before": before, "after": None, "targets": targets,
              "yes": set(), "no": set(), "open": True,
              "closes_at": now + REEXPLAIN_WINDOW, "recovery_seconds": None}
        self.reexplains.append(ev)
        self.current = ev
        return None

    def clearer_answer(self, sid, ev_id, answer):
        ev = self.current
        if answer not in ("yes", "no"):
            return
        if (not ev or not ev["open"] or ev["id"] != ev_id
                or sid not in ev["targets"] or sid in ev["yes"] or sid in ev["no"]):
            return
        now = time.time()
        self.taps = deque(e for e in self.taps if e["sid"] != sid)
        if answer == "yes":
            ev["yes"].add(sid)
            self.tap_times.pop(sid, None)       # no longer lost
            if ev["recovery_seconds"] is None and len(ev["yes"]) * 2 >= len(ev["targets"]):
                ev["recovery_seconds"] = round(now - ev["t"])
        else:
            ev["no"].add(sid)
            entry = {"t": now, "sid": sid, "topic": self.topic, "reason": None}
            self.taps.append(entry)             # still lost: keep them on the chart
            self.last_entry[sid] = entry
            self.tap_times[sid] = now

    def check_reexplain(self):
        """Close the check when time is up or everyone answered. True if it just closed."""
        ev = self.current
        if not ev or not ev["open"]:
            return False
        answered = len(ev["yes"]) + len(ev["no"])
        if time.time() >= ev["closes_at"] or answered >= len(ev["targets"]):
            ev["open"] = False
            ev["after"] = self.percent()
            return True
        return False

    def reexplain_view(self):
        ev = self.current
        if not ev:
            return None
        return {"id": ev["id"], "open": ev["open"], "topic": ev["topic"],
                "targets": len(ev["targets"]), "yes": len(ev["yes"]),
                "no": len(ev["no"]), "before": ev["before"], "after": ev["after"],
                "recovery": ev["recovery_seconds"],
                "left": max(0, round(ev["closes_at"] - time.time())) if ev["open"] else 0}

    # ---------- state for the teacher ----------
    def state(self):
        confused = self.active_taps()
        rows, suggestion = self.live_reasons()
        return {
            "type": "state",
            "students": len(self.students),
            "confused": confused,
            "percent": self.percent(),
            "topic": self.topic,
            "history": self.history[-150:],
            "spikes": self.spikes,
            "reasons": rows,
            "suggestion": suggestion,
            "reexplain": self.reexplain_view(),
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


async def send_prompt(sid):
    ws = session.students.get(sid)
    if not ws:
        return
    ev = session.current
    show = bool(ev and ev["open"] and sid in ev["targets"]
                and sid not in ev["yes"] and sid not in ev["no"])
    try:
        await ws.send_json({"type": "prompt", "show": show,
                            "id": ev["id"] if ev else 0})
    except Exception:
        pass


async def broadcast_prompt():
    for sid in list(session.students):
        await send_prompt(sid)


async def ticker():
    """Runs forever in the background, once per second."""
    count = 0
    while True:
        await asyncio.sleep(1)
        try:
            count += 1
            if count % SAMPLE_EVERY == 0:
                session.sample()
            if session.check_reexplain():
                await broadcast_prompt()
            await broadcast_to_teachers()
        except Exception as e:
            print("ticker error:", repr(e))


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
        rc = session.reasons.get(name, {})
        reasons = ", ".join(REASONS[k] + " (" + str(v) + ")"
                            for k, v in sorted(rc.items(), key=lambda kv: -kv[1]))
        result.append({
            "topic": name, "avg": avg, "peak": peak,
            "spikes": spikes, "taps": taps,
            "reasons": reasons or "-",
            "score": score, "advice": advice,
        })

    result.sort(key=lambda r: r["score"], reverse=True)
    return result


@app.get("/api/reexplains")
def reexplains_data():
    out = []
    for ev in session.reexplains:
        n = len(ev["targets"])
        y = len(ev["yes"])
        ratio = y / n if n else 0
        if ev["open"]:
            verdict = "Still collecting answers"
        elif ratio >= 0.7:
            verdict = "Worked well"
        elif ratio >= 0.4:
            verdict = "Partly worked"
        else:
            verdict = "Did not land, try a different approach"
        out.append({"time": ev["t"], "topic": ev["topic"], "targets": n,
                    "yes": y, "no": len(ev["no"]),
                    "recovery": ev["recovery_seconds"],
                    "before": ev["before"], "after": ev["after"],
                    "verdict": verdict})
    return out


@app.websocket("/ws/student")
async def student_ws(ws: WebSocket):
    await ws.accept()
    sid = str(uuid.uuid4())        # anonymous random token, no names
    session.students[sid] = ws
    try:
        while True:
            msg = await ws.receive_json()
            kind = msg.get("type")
            if kind == "confused":
                now = time.time()
                if now - session.last_tap.get(sid, 0) >= COOLDOWN:
                    session.last_tap[sid] = now
                    entry = {"t": now, "sid": sid, "topic": session.topic,
                             "reason": None}
                    session.taps.append(entry)
                    session.last_entry[sid] = entry
                    session.tap_times[sid] = now
                    session.tap_counts[session.topic] = session.tap_counts.get(session.topic, 0) + 1
            elif kind == "reason":
                session.add_reason(sid, msg.get("reason"))
            elif kind == "clearer":
                session.clearer_answer(sid, msg.get("id"), msg.get("answer"))
                await send_prompt(sid)
    except WebSocketDisconnect:
        pass
    finally:
        session.students.pop(sid, None)
        session.last_entry.pop(sid, None)
        session.tap_times.pop(sid, None)


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
                session.topic = str(msg.get("topic", "")).strip()[:60] or session.topic
            elif kind == "reexplain":
                err = session.start_reexplain()
                if err:
                    await ws.send_json({"type": "notice", "text": err})
                else:
                    await broadcast_prompt()
                    await broadcast_to_teachers()
    except WebSocketDisconnect:
        if ws in session.teachers:
            session.teachers.remove(ws)