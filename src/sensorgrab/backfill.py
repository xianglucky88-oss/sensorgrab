"""单一职责：缺口检测与历史补采（与实时读取共享每 tick 预算）。

- 按 (device,channel,epoch) 高水位检测缺序 → 显式缺口 reason=missing；
- 重连补传（reconnect）：展开为 origin=backfill 样本；设备声明的 lost
  区间 → reason=history_lost 的永久缺口；重复补传由身份键幂等去重，不增加计量；
- 预算：实时样本优先占用 TICK_BUDGET，补采只用剩余额度，超出的补采样本
  排队到后续 tick（在线时由后续 tick 继续；离线重放在流末按预算分 tick 排空）。
"""
from .config import TICK_BUDGET
from . import store as _store


def open(ctx):
    ctx.setdefault("bf", {"last_tick": 0, "used": {}, "pending": []})


def _used(bf, tick):
    return bf["used"].get(tick, 0)


def _use(bf, tick):
    bf["used"][tick] = _used(bf, tick) + 1


def _leftover(bf, tick):
    return max(0, TICK_BUDGET - _used(bf, tick))


def process(rec, ctx):
    bf = ctx.setdefault("bf", {"last_tick": 0, "used": {}, "pending": []})
    tick = rec.get("tick", 0)
    emitted = []
    if tick > bf["last_tick"]:
        for t in range(bf["last_tick"], tick):
            emitted.extend(_flush(ctx, t))  # 用已结束 tick 的剩余额度
        bf["last_tick"] = tick
    if rec.get("kind") == "reconnect":
        emitted.extend(_reconnect(ctx, rec))
        return emitted
    if rec.get("route") == "sample":
        if rec.get("origin") == "realtime" and not rec.get("buffered"):
            _use(bf, tick)  # 实时读取优先占用预算
        emitted.extend(_gap_detect(ctx["con"], rec))
        emitted.append(rec)
        return emitted
    emitted.append(rec)
    return emitted


def drain(ctx):
    """流末排空补采队列：按预算分合成 tick 放行（保持每 tick 预算语义）。"""
    bf = ctx.get("bf")
    if not bf:
        return []
    out = []
    tick = bf["last_tick"]
    while bf["pending"]:
        tick += 1
        n = 0
        while bf["pending"] and n < TICK_BUDGET:
            sample = bf["pending"].pop(0)
            sample["backfill_emit_tick"] = tick
            sample["final_flush"] = True
            out.extend(_gap_detect(ctx["con"], sample))
            out.append(sample)
            n += 1
    return out


def _flush(ctx, tick):
    """用指定（已结束）tick 的剩余预算放行排队的补采样本。"""
    bf = ctx["bf"]
    con = ctx["con"]
    out = []
    while bf["pending"] and _leftover(bf, tick) > 0:
        sample = bf["pending"].pop(0)
        _use(bf, tick)
        sample["backfill_emit_tick"] = tick
        out.extend(_gap_detect(con, sample))
        out.append(sample)
    return out


def _reconnect(ctx, rec):
    con = ctx["con"]
    bf = ctx["bf"]
    dev, chan, ep = rec.get("device"), rec.get("channel"), rec.get("epoch")
    tick = rec.get("tick", 0)
    if dev is None or chan is None or ep is None:
        _store.bump(con, "backfill:invalid")
        return []
    emitted = []
    for lo, hi in rec.get("lost", []) or []:
        for s in range(int(lo), int(hi) + 1):
            emitted.append(_gap(dev, chan, ep, s, "history_lost", tick))
    proto = rec.get("protocol", "modbus")
    for entry in rec.get("batch", []) or []:
        sample = {
            "kind": proto,
            "device": dev, "channel": chan, "epoch": ep,
            "seq": entry.get("seq"),
            "sample_ts": entry.get("sample_ts"),
            "recv_ts": rec.get("recv_ts"),
            "raw": entry.get("raw"),
            "tick": tick,
            "origin": "backfill",
            "route": "sample",
            "verdict": "valid",
            "status": "confirmed",
            "quality": entry.get("quality", "good"),
            "basis": ["backfilled"] + list(entry.get("basis", [])),
        }
        if _leftover(bf, tick) > 0:
            _use(bf, tick)
            emitted.extend(_gap_detect(con, sample))
            emitted.append(sample)
        else:
            bf["pending"].append(sample)
            _store.bump(con, "backfill:deferred")
    return emitted


def _gap_detect(con, rec):
    dev, chan, ep, seq = (rec.get("device"), rec.get("channel"),
                          rec.get("epoch"), rec.get("seq"))
    if None in (dev, chan, ep) or not isinstance(seq, int):
        return []
    hw = _store.get_cursor(con, dev, chan, ep)["high_water_seq"]
    gaps = []
    if hw >= 0 and seq > hw + 1:
        for s in range(hw + 1, seq):
            gaps.append(_gap(dev, chan, ep, s, "missing", rec.get("tick", 0)))
    if seq > hw:
        _store.save_high_water(con, dev, chan, ep, seq)
    return gaps


def _gap(dev, chan, ep, seq, reason, tick):
    return {"kind": "gap", "route": "gap", "device": dev, "channel": chan,
            "epoch": ep, "seq": seq, "reason": reason, "status": "open",
            "opened_tick": tick}
