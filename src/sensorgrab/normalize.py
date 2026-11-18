"""单一职责：字段规整与证据校验（规范阶段）。

- 旧版通用记录（无 kind）：保持原有规整行为（src/name/value），route=record。
- 量测事件（modbus/mqtt）：校验样本身份证据（设备/通道/重启代次/序列号）
  与双时间戳（sample_ts 采样时刻 / recv_ts 接收时刻，二者分开保留）；
  证据不足 → status=pending 保留待判断；
  检测时钟回拨（同代次内 seq 推进但 sample_ts 倒退）→ 留证并降 quality。
- 控制事件（calibration/uplink/reconnect）：打路由标记后透传。
"""
from datetime import datetime

IDENTITY_FIELDS = ("device", "channel", "epoch", "seq")
TIME_FIELDS = ("sample_ts", "recv_ts")


def open(ctx):
    ctx.setdefault("clock", {})


def process(rec, ctx):
    if "kind" not in rec:
        out = dict(rec)
        out["src"] = str(out.get("src", "")).strip().lower()
        out["name"] = str(out.get("name", "")).strip()
        out["value"] = int(out.get("value", 0))
        out["route"] = "record"
        return [out]
    kind = rec.get("kind")
    if kind == "calibration":
        out = dict(rec)
        out["route"] = "calibration"
        return [out]
    if kind in ("uplink", "reconnect"):
        out = dict(rec)
        out["route"] = "control"
        return [out]
    if kind in ("modbus", "mqtt"):
        return [_measurement(rec, ctx)]
    return [dict(rec)]


def _parse_ts(s):
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def _measurement(rec, ctx):
    out = dict(rec)
    out.setdefault("origin", "realtime")
    out.setdefault("tick", 0)
    out.setdefault("basis", [])
    if out.get("event") == "ack" or out.get("timeout"):
        return out  # 确认/超时事件不要求样本证据，由 protocol 判定
    out["status"] = "confirmed"
    out["quality"] = "good"
    out["pending_reason"] = []
    missing = [f for f in IDENTITY_FIELDS + TIME_FIELDS if out.get(f) is None]
    for f in TIME_FIELDS:
        if out.get(f) is not None and _parse_ts(out.get(f)) is None:
            missing.append(f + "(unparseable)")
    if missing:
        out["status"] = "pending"
        out["pending_reason"] += ["missing:" + f for f in missing]
        return out
    # 时钟回拨：同 (device,channel,epoch) 内 seq 推进但 sample_ts 倒退
    clock = ctx.setdefault("clock", {})
    ident = (out["device"], out["channel"], out["epoch"])
    st = clock.get(ident)
    ts = _parse_ts(out["sample_ts"])
    if st and out["seq"] > st["max_seq"] and ts < st["ts"]:
        out["clock_rollback"] = True
        out["quality"] = "uncertain"
        if "clock_rollback" not in out["basis"]:
            out["basis"].append("clock_rollback")
    if not st or out["seq"] > st["max_seq"]:
        clock[ident] = {"max_seq": out["seq"], "ts": ts}
    return out
