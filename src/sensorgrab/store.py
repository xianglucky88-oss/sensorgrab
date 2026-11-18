"""单一职责：本地 sqlite3 持久化。

表结构：
- records      旧版通用记录（保留旧输入的落库行为）
- samples      量测样本：质量感知幂等合并，迟到的低质量样本不覆盖已确认值
- gaps         显式缺口（buffer_overflow / history_lost / missing），补采到位后关闭
- cursors      每 (device,channel,epoch) 的高水位 / 落库 / 设备确认游标，重启后对得上
- calibrations 校准版本（修订保留历史，新版本不改写旧版本）
- buffer       边缘缓存（持久化，重启不丢）
- events       协议事件日志（超时/坏帧/确认，按事件键幂等）
- meta         运行计数器与链路状态
"""
import json
import sqlite3
from .config import DB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS records(key TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS samples(
  key TEXT PRIMARY KEY,
  device TEXT, channel TEXT, epoch INTEGER, seq INTEGER,
  sample_ts TEXT, recv_ts TEXT,
  raw REAL, std REAL, unit TEXT, calibration_version TEXT,
  quality TEXT, status TEXT, basis TEXT,
  origin TEXT, tick INTEGER,
  data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS gaps(
  key TEXT PRIMARY KEY,
  device TEXT, channel TEXT, epoch INTEGER, seq INTEGER,
  reason TEXT, status TEXT NOT NULL DEFAULT 'open',
  opened_tick INTEGER, closed_tick INTEGER,
  data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cursors(
  key TEXT PRIMARY KEY,
  device TEXT, channel TEXT, epoch INTEGER,
  high_water_seq INTEGER NOT NULL DEFAULT -1,
  last_ingested_seq INTEGER NOT NULL DEFAULT -1,
  last_acked_seq INTEGER NOT NULL DEFAULT -1
);
CREATE TABLE IF NOT EXISTS calibrations(
  key TEXT PRIMARY KEY,
  device TEXT, channel TEXT, version TEXT,
  scale REAL, offset REAL, unit TEXT, effective_from TEXT,
  data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS buffer(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  device TEXT, channel TEXT, epoch INTEGER, seq INTEGER,
  enqueued_tick INTEGER, payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events(
  ekey TEXT UNIQUE,
  tick INTEGER, device TEXT, kind TEXT, verdict TEXT, basis TEXT,
  data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
"""

STATUS_RANK = {"confirmed": 2, "pending": 1, "rejected": 0}
QUALITY_RANK = {"good": 2, "uncertain": 1, "bad": 0}


def connect(db_path=None):
    con = sqlite3.connect(db_path or DB_PATH)
    con.executescript(SCHEMA)
    return con


def upsert(con, record):
    d = record.data
    route = d.get("route")
    if route == "sample":
        _upsert_sample(con, record.key, d)
    elif route == "gap":
        _insert_gap(con, record.key, d)
    elif route == "ack":
        _apply_ack(con, d)
        _log_event(con, record.key, d)
    elif route == "event":
        _log_event(con, record.key, d)
    elif route == "calibration":
        _upsert_calibration(con, record.key, d)
    elif route == "control":
        pass  # 控制事件（uplink/reconnect）已被对应阶段消费，不落库
    else:
        # 旧版通用记录：保持原有幂等 upsert 行为
        con.execute("INSERT INTO records(key,data) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                    (record.key, json.dumps(d, ensure_ascii=False)))


def commit(con):
    con.commit()


def count(con):
    return con.execute("SELECT COUNT(*) FROM records").fetchone()[0]


def all_records(con):
    return [(k, json.loads(d)) for k, d in con.execute("SELECT key,data FROM records")]


# ---- 量测样本：质量感知幂等合并 ----

def _rank(d):
    return (STATUS_RANK.get(d.get("status"), 1), QUALITY_RANK.get(d.get("quality"), 0))


def _upsert_sample(con, key, d):
    row = con.execute("SELECT status,quality FROM samples WHERE key=?", (key,)).fetchone()
    if row and _rank(d) <= _rank({"status": row[0], "quality": row[1]}):
        # 重复或更差的迟到样本：不覆盖已确认值，只计重复
        bump(con, "duplicates")
        return False
    con.execute(
        "INSERT INTO samples(key,device,channel,epoch,seq,sample_ts,recv_ts,raw,std,unit,"
        "calibration_version,quality,status,basis,origin,tick,data) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET device=excluded.device, channel=excluded.channel, "
        "epoch=excluded.epoch, seq=excluded.seq, sample_ts=excluded.sample_ts, "
        "recv_ts=excluded.recv_ts, raw=excluded.raw, std=excluded.std, unit=excluded.unit, "
        "calibration_version=excluded.calibration_version, quality=excluded.quality, "
        "status=excluded.status, basis=excluded.basis, origin=excluded.origin, "
        "tick=excluded.tick, data=excluded.data",
        (key, d.get("device"), d.get("channel"), d.get("epoch"), d.get("seq"),
         d.get("sample_ts"), d.get("recv_ts"), d.get("raw"), d.get("std"),
         d.get("unit"), d.get("calibration_version"), d.get("quality"), d.get("status"),
         json.dumps(d.get("basis", []), ensure_ascii=False), d.get("origin"),
         d.get("tick"), json.dumps(d, ensure_ascii=False)))
    if d.get("status") == "confirmed":
        _close_gap(con, d)
        _touch_ingested(con, d)
    return True


def _close_gap(con, d):
    con.execute("UPDATE gaps SET status='closed', closed_tick=? "
                "WHERE device=? AND channel=? AND epoch=? AND seq=? AND status='open'",
                (d.get("tick"), d.get("device"), d.get("channel"),
                 d.get("epoch"), d.get("seq")))


def _touch_ingested(con, d):
    if not isinstance(d.get("seq"), int):
        return
    con.execute(
        "INSERT INTO cursors(key,device,channel,epoch,high_water_seq,last_ingested_seq,last_acked_seq) "
        "VALUES(?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET "
        "last_ingested_seq=MAX(last_ingested_seq, excluded.last_ingested_seq)",
        (_cursor_key(d.get("device"), d.get("channel"), d.get("epoch")),
         d.get("device"), d.get("channel"), d.get("epoch"), -1, d.get("seq"), -1))


# ---- 缺口 ----

def _insert_gap(con, key, d):
    # 同一缺口只保留首次记录的原因（如 buffer_overflow 优先于后续 missing）
    con.execute("INSERT OR IGNORE INTO gaps(key,device,channel,epoch,seq,reason,status,opened_tick,data) "
                "VALUES(?,?,?,?,?,?,'open',?,?)",
                (key, d.get("device"), d.get("channel"), d.get("epoch"), d.get("seq"),
                 d.get("reason"), d.get("opened_tick"), json.dumps(d, ensure_ascii=False)))


# ---- 游标（设备确认 / 落库 / 高水位，重启后对账） ----

def _cursor_key(device, channel, epoch):
    return "%s|%s|%s" % (device, channel, epoch)


def get_cursor(con, device, channel, epoch):
    row = con.execute("SELECT high_water_seq,last_ingested_seq,last_acked_seq "
                      "FROM cursors WHERE key=?",
                      (_cursor_key(device, channel, epoch),)).fetchone()
    if not row:
        return {"high_water_seq": -1, "last_ingested_seq": -1, "last_acked_seq": -1}
    return {"high_water_seq": row[0], "last_ingested_seq": row[1], "last_acked_seq": row[2]}


def save_high_water(con, device, channel, epoch, seq):
    con.execute(
        "INSERT INTO cursors(key,device,channel,epoch,high_water_seq,last_ingested_seq,last_acked_seq) "
        "VALUES(?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET "
        "high_water_seq=MAX(high_water_seq, excluded.high_water_seq)",
        (_cursor_key(device, channel, epoch), device, channel, epoch, seq, -1, -1))


def _apply_ack(con, d):
    dev, chan, ep, ack = d.get("device"), d.get("channel"), d.get("epoch"), d.get("ack_seq")
    if None in (dev, chan, ep) or not isinstance(ack, int):
        bump(con, "ack:invalid")
        return
    con.execute(
        "INSERT INTO cursors(key,device,channel,epoch,high_water_seq,last_ingested_seq,last_acked_seq) "
        "VALUES(?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET "
        "last_acked_seq=MAX(last_acked_seq, excluded.last_acked_seq)",
        (_cursor_key(dev, chan, ep), dev, chan, ep, -1, -1, ack))


# ---- 校准版本（修订保留历史） ----

def _upsert_calibration(con, key, d):
    con.execute(
        "INSERT INTO calibrations(key,device,channel,version,scale,offset,unit,effective_from,data) "
        "VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET "
        "scale=excluded.scale, offset=excluded.offset, unit=excluded.unit, "
        "effective_from=excluded.effective_from, data=excluded.data",
        (key, d.get("device"), d.get("channel"), d.get("version"), d.get("scale"),
         d.get("offset"), d.get("unit"), d.get("effective_from"),
         json.dumps(d, ensure_ascii=False)))


def find_calibration(con, device, channel, sample_ts):
    """取 sample_ts 时刻生效的最新校准版本；无则 None。"""
    if device is None or channel is None:
        return None
    if sample_ts:
        row = con.execute(
            "SELECT data FROM calibrations WHERE device=? AND channel=? AND effective_from<=? "
            "ORDER BY effective_from DESC LIMIT 1", (device, channel, sample_ts)).fetchone()
    else:
        row = con.execute(
            "SELECT data FROM calibrations WHERE device=? AND channel=? "
            "ORDER BY effective_from DESC LIMIT 1", (device, channel)).fetchone()
    return json.loads(row[0]) if row else None


# ---- 协议事件日志（幂等） ----

def _log_event(con, ekey, d):
    con.execute("INSERT OR IGNORE INTO events(ekey,tick,device,kind,verdict,basis,data) "
                "VALUES(?,?,?,?,?,?,?)",
                (ekey, d.get("tick"), d.get("device"), d.get("kind"), d.get("verdict"),
                 d.get("verdict_basis") or "", json.dumps(d, ensure_ascii=False)))


# ---- 计数器与链路状态 ----

def bump(con, key, n=1):
    con.execute("INSERT INTO meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=CAST(value AS INTEGER)+?",
                (key, str(n), n))


def get_meta(con, key, default=None):
    row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def set_meta(con, key, value):
    con.execute("INSERT INTO meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)))


def stats(con):
    def q(sql):
        return con.execute(sql).fetchone()[0]

    out = {
        "samples": q("SELECT COUNT(*) FROM samples"),
        "metered": q("SELECT COUNT(*) FROM samples WHERE status='confirmed'"),
        "pending": q("SELECT COUNT(*) FROM samples WHERE status='pending'"),
        "gaps_open": q("SELECT COUNT(*) FROM gaps WHERE status='open'"),
        "gaps_closed": q("SELECT COUNT(*) FROM gaps WHERE status='closed'"),
        "buffered": q("SELECT COUNT(*) FROM buffer"),
        "events": q("SELECT COUNT(*) FROM events"),
        "duplicates": 0, "buffer_enqueued": 0, "buffer_drained": 0,
        "buffer_overflowed": 0, "poller_skipped": 0, "backfill_deferred": 0,
        "verdicts": {"valid": 0, "bad_frame": 0, "timeout": 0, "ack": 0},
    }
    for k, v in con.execute("SELECT key,value FROM meta"):
        if k == "uplink":
            out["uplink"] = v
        elif k.startswith("verdict:"):
            out["verdicts"][k.split(":", 1)[1]] = int(v)
        else:
            try:
                out[k.replace(":", "_")] = int(v)
            except (TypeError, ValueError):
                out[k.replace(":", "_")] = v
    return out
