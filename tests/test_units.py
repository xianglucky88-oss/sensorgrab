"""阶段级单元行为：协议判定、证据校验、身份键、质量合并、调度与预算。"""
import pytest

from src.sensorgrab import normalize, pipeline, protocol, store
from src.sensorgrab.keyer import key_of
from src.sensorgrab.models import Record


@pytest.fixture()
def ctx():
    c = store.connect(":memory:")
    yield {"con": c}
    c.close()


def one(stage, rec, ctx):
    out = stage.process(rec, ctx)
    assert len(out) == 1
    return out[0]


# ---- protocol：按协议区分有效响应 / 坏帧 / 超时 / 确认 ----

@pytest.mark.parametrize("rec,verdict", [
    ({"kind": "modbus", "raw": 1, "frame": {"crc_ok": True}}, "valid"),
    ({"kind": "modbus", "frame": {"crc_ok": False}}, "bad_frame"),
    ({"kind": "modbus", "frame": {"crc_ok": True, "exception": 2}}, "bad_frame"),
    ({"kind": "modbus", "timeout": True, "frame": {}}, "timeout"),
    ({"kind": "modbus", "event": "ack", "frame": {"crc_ok": True}}, "ack"),
    ({"kind": "mqtt", "raw": 1, "msg": {"payload_ok": True}}, "valid"),
    ({"kind": "mqtt", "raw": 1, "msg": {"payload_ok": False}}, "bad_frame"),
    ({"kind": "mqtt", "timeout": True}, "timeout"),
    ({"kind": "mqtt", "msg": {"type": "puback"}}, "ack"),
])
def test_protocol_classification(ctx, rec, verdict):
    out = one(protocol, rec, ctx)
    assert out["verdict"] == verdict
    assert out["route"] == {"valid": "sample", "ack": "ack"}.get(verdict, "event")


def test_protocol_mqtt_dup_flag(ctx):
    out = one(protocol, {"kind": "mqtt", "raw": 1,
                         "msg": {"payload_ok": True, "dup": True}}, ctx)
    assert out["verdict"] == "valid" and out["dup"] is True


# ---- normalize：证据不足保留待判断，时钟回拨留证 ----

def test_normalize_pending_on_missing_identity(ctx):
    rec = {"kind": "mqtt", "device": "d", "channel": "c", "seq": 1,
           "sample_ts": "2026-01-01T00:00:00Z", "recv_ts": "2026-01-01T00:00:01Z",
           "raw": 5}
    out = one(normalize, rec, ctx)
    assert out["status"] == "pending"
    assert "missing:epoch" in out["pending_reason"]


def test_normalize_detects_clock_rollback(ctx):
    base = {"kind": "modbus", "device": "d", "channel": "c", "epoch": 1,
            "recv_ts": "2026-01-01T00:00:10Z", "frame": {"crc_ok": True}}
    normalize.process(dict(base, seq=1, sample_ts="2026-01-01T00:00:05Z"), ctx)
    out = one(normalize, dict(base, seq=2, sample_ts="2026-01-01T00:00:01Z"), ctx)
    assert out["clock_rollback"] is True and out["quality"] == "uncertain"


def test_normalize_legacy_records_keep_old_behavior():
    out = normalize.process({"src": " Alpha ", "name": " N ", "value": "7"}, {})[0]
    assert out["src"] == "alpha" and out["name"] == "N" and out["value"] == 7
    assert out["route"] == "record"


# ---- keyer：样本身份含重启代次 ----

def test_key_includes_restart_epoch():
    a = {"route": "sample", "device": "d", "channel": "c", "epoch": 1, "seq": 1}
    assert key_of(a) == key_of(dict(a))
    assert key_of(a) != key_of(dict(a, epoch=2))   # 重启代次不同 → 不同身份
    assert key_of(a) != key_of(dict(a, seq=2))


def test_key_legacy_records_unchanged():
    assert key_of({"id": "r-1"}) == "r-1"
    assert key_of({"id": "r-1", "v": 1}) == "r-1"


# ---- store：质量感知合并 ----

def sample(**kw):
    d = {"route": "sample", "device": "d", "channel": "c", "epoch": 1, "seq": 1,
         "status": "confirmed", "quality": "good", "raw": 1}
    d.update(kw)
    return d


def test_store_late_low_quality_cannot_overwrite(ctx):
    con = ctx["con"]
    store.upsert(con, Record(key="k", data=sample(raw=10)))
    store.upsert(con, Record(key="k", data=sample(raw=99, quality="uncertain")))
    row = con.execute("SELECT raw,quality FROM samples WHERE key='k'").fetchone()
    assert row == (10, "good")


def test_store_better_quality_upgrades(ctx):
    con = ctx["con"]
    store.upsert(con, Record(key="k", data=sample(raw=10, quality="uncertain")))
    store.upsert(con, Record(key="k", data=sample(raw=20, quality="good")))
    row = con.execute("SELECT raw,quality FROM samples WHERE key='k'").fetchone()
    assert row == (20, "good")


def test_store_pending_does_not_close_gap(ctx):
    con = ctx["con"]
    store.upsert(con, Record(key="g:d|c|1|5", data={
        "route": "gap", "device": "d", "channel": "c", "epoch": 1, "seq": 5,
        "reason": "missing", "opened_tick": 1}))
    store.upsert(con, Record(key="k", data=sample(seq=5, status="pending")))
    assert con.execute("SELECT status FROM gaps").fetchone()[0] == "open"
    store.upsert(con, Record(key="k2", data=sample(seq=5)))
    assert con.execute("SELECT status FROM gaps").fetchone()[0] == "closed"


# ---- 调度与预算：慢设备隔离 + 实时优先 ----

def test_slow_device_isolated_others_unaffected(tmp_path):
    con = store.connect(tmp_path / "iso.db")
    cal = lambda dev: {"kind": "calibration", "device": dev, "channel": "p",
                       "version": "v1", "scale": 1.0, "offset": 0.0, "unit": "kpa",
                       "effective_from": "2026-01-01T00:00:00Z", "tick": 0}
    fast = lambda seq, tick: {"kind": "modbus", "device": "dev-fast", "channel": "p",
                              "epoch": 1, "seq": seq, "sample_ts": "2026-01-01T00:00:0%dZ" % seq,
                              "recv_ts": "2026-01-01T00:01:0%dZ" % seq, "raw": seq,
                              "tick": tick, "origin": "realtime", "frame": {"crc_ok": True}}
    slow_to = lambda att, tick: {"kind": "modbus", "device": "dev-slow", "channel": "p",
                                 "epoch": 1, "seq": 9, "attempt": att, "timeout": True,
                                 "recv_ts": "2026-01-01T00:02:00Z", "tick": tick,
                                 "origin": "realtime", "frame": {}}
    slow_ok = lambda tick: {"kind": "modbus", "device": "dev-slow", "channel": "p",
                            "epoch": 1, "seq": 9, "sample_ts": "2026-01-01T00:03:00Z",
                            "recv_ts": "2026-01-01T00:03:01Z", "raw": 50, "tick": tick,
                            "origin": "realtime", "frame": {"crc_ok": True}}
    events = [cal("dev-fast"), cal("dev-slow"),
              fast(1, 1), slow_to(1, 1),
              fast(2, 2), slow_to(2, 2),
              fast(3, 3), slow_to(3, 3),      # 熔断器打开（阈值 3）
              fast(4, 4), slow_ok(4),          # 慢设备冷却期：读取被跳过
              slow_ok(6)]                      # 冷却结束：恢复读取
    s = pipeline.run(con=con, fixture=events)
    assert s["poller_skipped"] == 1
    assert s["metered"] == 5                    # 快设备 4 条全部入库 + 慢设备恢复后 1 条
    for seq in (1, 2, 3, 4):
        row = con.execute("SELECT COUNT(*) FROM samples WHERE device='dev-fast' AND seq=?",
                          (seq,)).fetchone()
        assert row[0] == 1
    con.close()


def test_backfill_uses_only_leftover_budget(tmp_path):
    con = store.connect(tmp_path / "bf.db")
    events = [
        {"kind": "calibration", "device": "dev-x", "channel": "p", "version": "v1",
         "scale": 1.0, "offset": 0.0, "unit": "kpa",
         "effective_from": "2026-01-01T00:00:00Z", "tick": 0},
        {"kind": "reconnect", "protocol": "modbus", "device": "dev-x", "channel": "p",
         "epoch": 1, "recv_ts": "2026-01-01T01:00:00Z", "tick": 1,
         "batch": [{"seq": i, "sample_ts": "2026-01-01T00:00:%02dZ" % i, "raw": i}
                   for i in range(1, 7)]},     # 6 条补采 > 每 tick 预算 4
    ]
    s = pipeline.run(con=con, fixture=events)
    assert s["metered"] == 6                    # 最终全部入库
    assert s["backfill_deferred"] == 2          # 超出预算的 2 条被排到后续 tick
    con.close()
