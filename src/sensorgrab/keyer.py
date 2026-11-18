"""单一职责：派生稳定主键。

量测样本身份 = 设备 + 通道 + 重启代次(epoch) + 序列号(seq)：
设备重启后 epoch 递增，同一物理量测的重传键不变，新量测键不同；
时钟回拨不影响键。证据不足的待判断样本用现有证据 + 载荷兜底。
旧版通用记录保持原有键规则（id 或内容哈希）。
"""
import hashlib
import json


def _h(s):
    return hashlib.sha256(s.encode()).hexdigest()[:16]


def key_of(record):
    route = record.get("route")
    if route == "sample":
        ident = (record.get("device"), record.get("channel"),
                 record.get("epoch"), record.get("seq"))
        if all(v is not None for v in ident):
            return "s:" + _h("|".join(str(v) for v in ident))
        # 证据不足：保留待判断样本，键由现有证据与载荷决定
        return "s:pending:" + _h(json.dumps(record, sort_keys=True, ensure_ascii=False))
    if route == "gap":
        return "g:%s|%s|%s|%s" % (record.get("device"), record.get("channel"),
                                  record.get("epoch"), record.get("seq"))
    if route == "ack":
        return "k:%s|%s|%s|%s" % (record.get("device"), record.get("channel"),
                                  record.get("epoch"), record.get("ack_seq"))
    if route == "event":
        parts = [record.get("device"), record.get("channel"), record.get("epoch"),
                 record.get("seq"), record.get("tick"), record.get("attempt"),
                 record.get("verdict"), record.get("kind")]
        return "e:" + _h("|".join(str(p) for p in parts))
    if route == "calibration":
        return "c:%s|%s|%s" % (record.get("device"), record.get("channel"),
                               record.get("version"))
    # 旧版通用记录：保持原行为
    rid = record.get("id")
    if rid:
        return str(rid)
    return _h(json.dumps(record, sort_keys=True, ensure_ascii=False))
