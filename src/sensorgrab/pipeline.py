"""薄编排：loader → normalize → protocol → poller → buffer → backfill → unit_store → keyer → store。
不含业务判断：只按顺序调用阶段，阶段可返回 0..N 条记录（缓存/补采会展开或暂存）。
"""
import importlib
from . import store as _store
from . import loader as _loader
from .keyer import key_of
from .models import Record

CHAIN_NAMES = ["normalize", "protocol", "poller", "buffer", "backfill", "unit_store"]


def run(con=None, fixture=None):
    own = con is None
    con = con or _store.connect()
    try:
        ctx = {"con": con}
        stages = [importlib.import_module("." + n, __package__) for n in CHAIN_NAMES]
        for m in stages:
            if hasattr(m, "open"):
                m.open(ctx)
        fed = 0
        for raw in _loader.load(fixture):
            for rec in _through(raw, stages, ctx):
                _store.upsert(con, Record(key=key_of(rec), data=rec))
            fed += 1
        # 阶段排空（如补采队列在流末按预算放行），只走该阶段之后的下游
        for i, m in enumerate(stages):
            drain = getattr(m, "drain", None)
            if drain:
                for head in drain(ctx) or []:
                    for rec in _through(head, stages[i + 1:], ctx):
                        _store.upsert(con, Record(key=key_of(rec), data=rec))
        for m in stages:
            if hasattr(m, "close"):
                m.close(ctx)
        _store.commit(con)
        return {"fed": fed, "rows": _store.count(con), **_store.stats(con)}
    finally:
        if own:
            con.close()


def _through(rec, stages, ctx):
    recs = [rec]
    for m in stages:
        nxt = []
        for r in recs:
            out = m.process(r, ctx)
            nxt.extend(out if isinstance(out, list) else [out])
        recs = nxt
    return recs
