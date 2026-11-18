"""网关场景端到端：断网缓存、缺口补采、幂等入库、重启对账。"""
import json

import pytest

from src.sensorgrab import pipeline, store
from src.sensorgrab.config import GATEWAY_FIXTURE, ROOT

PART1 = ROOT / "fixtures" / "gateway_restart_part1.json"
PART2 = ROOT / "fixtures" / "gateway_restart_part2.json"


@pytest.fixture()
def con(tmp_path):
    c = store.connect(tmp_path / "gw.db")
    yield c
    c.close()


def get_sample(con, device, channel, epoch, seq):
    row = con.execute(
        "SELECT data FROM samples WHERE device=? AND channel=? AND epoch=? AND seq=?",
        (device, channel, epoch, seq)).fetchone()
    return json.loads(row[0]) if row else None


def get_gap(con, device, channel, epoch, seq):
    return con.execute(
        "SELECT reason,status FROM gaps WHERE device=? AND channel=? AND epoch=? AND seq=?",
        (device, channel, epoch, seq)).fetchone()


def test_gateway_end_to_end_summary(con):
    s = pipeline.run(con=con, fixture=GATEWAY_FIXTURE)
    assert s["fed"] == 40
    assert s["samples"] == 21          # 20 条确认 + 1 条待判断
    assert s["metered"] == 20          # 重复补传不增加计量
    assert s["pending"] == 1
    assert s["gaps_open"] == 2         # dev-c 历史丢失的永久缺口
    assert s["gaps_closed"] == 3       # 溢出缺口被补采关闭
    assert s["buffered"] == 0          # 缓存最终排空
    assert s["buffer_enqueued"] == 8
    assert s["buffer_drained"] == 5
    assert s["buffer_overflowed"] == 3
    assert s["backfill_deferred"] == 6  # 预算不足被排到后续 tick 的补采样本
    assert s["poller_skipped"] == 1     # 熔断期间跳过对慢设备的拉取
    assert s["duplicates"] == 12        # 重传/重复补传被幂等去重
    assert s["verdicts"] == {"valid": 21, "bad_frame": 1, "timeout": 4, "ack": 5}


def test_duplicate_retransmission_does_not_change_metering(con):
    a = pipeline.run(con=con, fixture=GATEWAY_FIXTURE)
    b = pipeline.run(con=con, fixture=GATEWAY_FIXTURE)
    for k in ("samples", "metered", "pending", "gaps_open", "gaps_closed", "buffered"):
        assert b[k] == a[k]


def test_sample_identity_keeps_restart_epoch_and_split_timestamps(con):
    pipeline.run(con=con, fixture=GATEWAY_FIXTURE)
    s1 = get_sample(con, "dev-a", "temp", 1, 1)
    assert s1["sample_ts"] == "2026-11-10T08:00:00Z"   # 采样时间
    assert s1["recv_ts"] == "2026-11-10T08:00:01Z"     # 接收时间，分开保存
    # 设备重启后 epoch 递增：同 seq 不同身份，两条都计入
    assert get_sample(con, "dev-a", "temp", 1, 1)["raw"] == 2315
    assert get_sample(con, "dev-a", "temp", 2, 1)["raw"] == 2400


def test_clock_rollback_flagged_not_merged(con):
    pipeline.run(con=con, fixture=GATEWAY_FIXTURE)
    s = get_sample(con, "dev-a", "temp", 2, 2)
    assert s["clock_rollback"] is True
    assert s["quality"] == "uncertain"
    assert "clock_rollback" in s["basis"]
    assert s["sample_ts"] < s["recv_ts"]  # 采样时间保留原值，与接收时间分开


def test_late_low_quality_does_not_overwrite_confirmed(con):
    pipeline.run(con=con, fixture=GATEWAY_FIXTURE)
    s = get_sample(con, "dev-a", "temp", 1, 2)
    assert s["raw"] == 2300          # 补传批里的 2319/uncertain 不得覆盖
    assert s["quality"] == "good"
    assert s["status"] == "confirmed"


def test_pending_when_evidence_missing(con):
    pipeline.run(con=con, fixture=GATEWAY_FIXTURE)
    rows = con.execute("SELECT data FROM samples WHERE status='pending'").fetchall()
    assert len(rows) == 1
    rec = json.loads(rows[0][0])
    assert rec["device"] == "dev-b" and rec["seq"] == 7
    assert "missing:epoch" in rec["pending_reason"]


def test_buffer_overflow_leaves_explicit_gap_then_backfill_closes(con):
    pipeline.run(con=con, fixture=GATEWAY_FIXTURE)
    for dev, chan, seq in (("dev-a", "temp", 4), ("dev-a", "temp", 5), ("dev-b", "hum", 3)):
        reason, status = get_gap(con, dev, chan, 1, seq)
        assert reason == "buffer_overflow"
        assert status == "closed"     # 设备保留历史，补采到位后关闭


def test_history_lost_leaves_permanent_gap(con):
    pipeline.run(con=con, fixture=GATEWAY_FIXTURE)
    for seq in (3, 4):
        reason, status = get_gap(con, "dev-c", "press", 1, seq)
        assert reason == "history_lost"
        assert status == "open"       # 历史已丢失：缺口明确保留
    # 熔断期间被跳过的读取经补采找回
    assert get_sample(con, "dev-c", "press", 1, 2)["origin"] == "backfill"


def test_calibration_revision_keeps_history(con):
    pipeline.run(con=con, fixture=GATEWAY_FIXTURE)
    versions = [r[0] for r in con.execute(
        "SELECT version FROM calibrations WHERE device='dev-a' AND channel='temp' ORDER BY version")]
    assert versions == ["cal-v1", "cal-v2"]     # 修订历史保留
    s1 = get_sample(con, "dev-a", "temp", 2, 1)  # 修订前入库
    s3 = get_sample(con, "dev-a", "temp", 2, 3)  # 修订后入库
    assert s1["calibration_version"] == "cal-v1" and s1["std"] == pytest.approx(-16.0)
    assert s3["calibration_version"] == "cal-v2" and s3["std"] == pytest.approx(-15.48)
    assert s1["unit"] == s3["unit"] == "celsius"


def test_cursors_reconcile_after_acks(con):
    pipeline.run(con=con, fixture=GATEWAY_FIXTURE)
    rows = con.execute(
        "SELECT device,epoch,last_acked_seq,last_ingested_seq FROM cursors").fetchall()
    assert rows
    for dev, ep, acked, ingested in rows:
        assert acked <= ingested, (dev, ep, acked, ingested)
    cur = store.get_cursor(con, "dev-a", "temp", 1)
    assert (cur["last_acked_seq"], cur["last_ingested_seq"]) == (8, 9)


def test_restart_consistency_across_runs(tmp_path):
    db = tmp_path / "restart.db"
    con = store.connect(db)
    r1 = pipeline.run(con=con, fixture=PART1)
    assert r1["metered"] == 2 and r1["buffered"] == 5   # 断网期间缓存 5 条
    con.close()                                          # 模拟网关重启

    con = store.connect(db)                              # 重开同一库
    r2 = pipeline.run(con=con, fixture=PART2)
    assert r2["metered"] == 8                            # 缓存排空 5 + 补采新增 1
    assert r2["buffered"] == 0
    assert r2["buffer_enqueued"] == r2["buffer_drained"] + r2["buffer_overflowed"]
    cur = store.get_cursor(con, "dev-r", "temp", 1)
    assert (cur["last_acked_seq"], cur["last_ingested_seq"]) == (8, 8)

    r3 = pipeline.run(con=con, fixture=PART2)            # 重启后重复补传
    assert r3["metered"] == 8 and r3["samples"] == 8     # 不增加计量
    con.close()
