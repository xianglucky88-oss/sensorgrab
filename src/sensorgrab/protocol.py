"""单一职责：按各自协议判定线上结果 —— 有效响应 / 坏帧 / 超时 / 确认。

modbus（拉取式）：CRC 校验失败、异常码响应 → 坏帧；无响应 → 超时；
写确认 → 确认；携带有效读数 → 有效响应。
mqtt（推送式）：payload 校验失败 → 坏帧；PUBACK → 确认；
dup 重传 → 有效但打重传标记（由身份键幂等去重）；静默 → 超时。
判定结论写 verdict，证据写 basis，路由写 route（sample/event/ack）。
"""
from . import store as _store


def process(rec, ctx):
    if rec.get("kind") not in ("modbus", "mqtt"):
        return [rec]
    out = dict(rec)
    verdict, basis, route = _classify(out)
    out["verdict"] = verdict
    out["verdict_basis"] = basis
    out["route"] = route
    bl = list(out.get("basis", []))
    if basis not in bl:
        bl.append(basis)
    out["basis"] = bl
    _store.bump(ctx["con"], "verdict:" + verdict)
    return [out]


def _classify(rec):
    if rec.get("kind") == "modbus":
        frame = rec.get("frame") or {}
        if rec.get("timeout"):
            return "timeout", "modbus_timeout", "event"
        if frame.get("crc_ok") is False:
            return "bad_frame", "modbus_crc_fail", "event"
        if frame.get("exception") is not None:
            return "bad_frame", "modbus_exception_%s" % frame.get("exception"), "event"
        if rec.get("event") == "ack":
            return "ack", "modbus_write_ack", "ack"
        if rec.get("raw") is not None:
            return "valid", "modbus_valid", "sample"
        return "bad_frame", "modbus_empty_response", "event"
    # mqtt
    msg = rec.get("msg") or {}
    if rec.get("timeout"):
        return "timeout", "mqtt_timeout", "event"
    if msg.get("payload_ok") is False:
        return "bad_frame", "mqtt_payload_corrupt", "event"
    if rec.get("event") == "ack" or msg.get("type") == "puback":
        return "ack", "mqtt_puback", "ack"
    if rec.get("raw") is not None:
        if msg.get("dup"):
            rec["dup"] = True
        return "valid", "mqtt_valid", "sample"
    return "bad_frame", "mqtt_empty_payload", "event"
