"""单一职责：实时轮询调度 —— 慢设备熔断隔离，保护共享读取预算。

实时流量享有优先级：poller 不丢弃健康设备的实时样本。
某设备连续超时达到阈值后打开熔断器，冷却期内不再向该设备发起拉取
（modbus 读被跳过并计数），慢设备因此不会拖住其他设备；
推送式 mqtt 无法节流，照常放行。冷却期随 tick 自然恢复。
"""
from .config import BREAKER_THRESHOLD, BREAKER_COOLDOWN
from . import store as _store


def open(ctx):
    ctx.setdefault("poller", {})


def process(rec, ctx):
    if rec.get("kind") not in ("modbus", "mqtt"):
        return [rec]
    dev = rec.get("device")
    if dev is None:
        return [rec]
    st = ctx.setdefault("poller", {}).setdefault(dev, {"timeouts": 0, "open_until": -1})
    tick = rec.get("tick", 0)
    verdict = rec.get("verdict")
    out = dict(rec)
    if verdict == "timeout" and tick > st["open_until"]:
        st["timeouts"] += 1
        if st["timeouts"] >= BREAKER_THRESHOLD:
            st["open_until"] = tick + BREAKER_COOLDOWN
            out["breaker"] = "opened"
    elif verdict in ("valid", "ack") and tick > st["open_until"]:
        st["timeouts"] = 0
    if (verdict == "valid" and st["open_until"] >= tick
            and rec.get("kind") == "modbus" and rec.get("origin") == "realtime"):
        # 熔断中：跳过对该慢设备的拉取，共享预算让给其他设备；
        # 被跳过的量测由设备侧缓存，经补采兜底
        _store.bump(ctx["con"], "poller:skipped")
        return []
    out["sched"] = "run"
    return [out]
