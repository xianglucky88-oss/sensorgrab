"""单一职责：单位与校准 —— 原始值→标准值换算，全程可追溯。

- 按 (device,channel) 取 sample_ts 时刻生效的最新校准版本：
  std = raw * scale + offset，样本记录 unit 与 calibration_version；
- 校准修订（新版本）只影响其后入库的样本，不改写已入库样本（历史保留）；
- 无可用校准 → 证据不足，样本转 pending 保留待判断，std 留空。
"""
from . import store as _store


def process(rec, ctx):
    if rec.get("route") != "sample":
        return [rec]
    out = dict(rec)
    cal = _store.find_calibration(ctx["con"], out.get("device"),
                                  out.get("channel"), out.get("sample_ts"))
    if cal:
        raw = out.get("raw")
        out["std"] = round(raw * cal["scale"] + cal["offset"], 6) \
            if isinstance(raw, (int, float)) else None
        out["unit"] = cal.get("unit")
        out["calibration_version"] = cal.get("version")
        bl = out.setdefault("basis", [])
        tag = "cal:" + str(cal.get("version"))
        if tag not in bl:
            bl.append(tag)
    else:
        out["std"] = None
        out["status"] = "pending"
        out.setdefault("pending_reason", []).append("no_calibration")
        bl = out.setdefault("basis", [])
        if "no_calibration" not in bl:
            bl.append("no_calibration")
    return [out]
