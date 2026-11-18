"""单一职责：边缘缓存 —— 链路中断时持久化暂存，溢出时留下显式缺口。

- uplink down：样本写入 sqlite 缓存表（重启不丢），暂不上送；
- uplink up：按入队顺序排空缓存，缓存消费计数落 meta，重启后可对账；
- 缓存满：逐出最旧样本，并为其生成 reason=buffer_overflow 的显式缺口，
  宁可留缺口也不静默丢失；缺口等待补采或永久保留。
"""
import json
from .config import BUFFER_CAP
from . import store as _store


def open(ctx):
    pass


def process(rec, ctx):
    con = ctx["con"]
    if rec.get("kind") == "uplink":
        state = rec.get("state")
        _store.set_meta(con, "uplink", state)
        if state == "up":
            return _drain(con)
        return []
    if rec.get("route") == "sample" and _store.get_meta(con, "uplink", "up") == "down":
        return _enqueue(con, rec)
    return [rec]


def _enqueue(con, rec):
    emitted = []
    if _count(con) >= BUFFER_CAP:
        old = con.execute("SELECT id,payload FROM buffer ORDER BY id LIMIT 1").fetchone()
        if old:
            con.execute("DELETE FROM buffer WHERE id=?", (old[0],))
            _store.bump(con, "buffer:overflowed")
            gap = _gap_for(json.loads(old[1]), rec.get("tick", 0))
            if gap:
                emitted.append(gap)
    con.execute(
        "INSERT INTO buffer(device,channel,epoch,seq,enqueued_tick,payload) VALUES(?,?,?,?,?,?)",
        (rec.get("device"), rec.get("channel"), rec.get("epoch"), rec.get("seq"),
         rec.get("tick", 0), json.dumps(rec, ensure_ascii=False)))
    _store.bump(con, "buffer:enqueued")
    return emitted


def _drain(con):
    rows = con.execute("SELECT id,payload FROM buffer ORDER BY id").fetchall()
    out = []
    for rid, payload in rows:
        rec = json.loads(payload)
        rec["buffered"] = True
        rec.setdefault("basis", [])
        if "buffered" not in rec["basis"]:
            rec["basis"].append("buffered")
        out.append(rec)
        con.execute("DELETE FROM buffer WHERE id=?", (rid,))
        _store.bump(con, "buffer:drained")
    return out


def _count(con):
    return con.execute("SELECT COUNT(*) FROM buffer").fetchone()[0]


def _gap_for(evicted, tick):
    ident = (evicted.get("device"), evicted.get("channel"),
             evicted.get("epoch"), evicted.get("seq"))
    if any(v is None for v in ident):
        return None  # 证据不足的样本无法定位缺口，仅计数
    return {"kind": "gap", "route": "gap", "device": ident[0], "channel": ident[1],
            "epoch": ident[2], "seq": ident[3], "reason": "buffer_overflow",
            "status": "open", "opened_tick": tick}
