"""单一职责：从本地 fixtures 读取原始记录（离线摄取入口，不联网）。

fixture 可以是路径（JSON 文件）或直接传入的事件列表（便于测试切片）。
"""
import json
from .config import FIXTURES


def load(fixture=None):
    if fixture is None:
        fixture = FIXTURES
    if isinstance(fixture, (list, tuple)):
        return list(fixture)
    with open(fixture, encoding="utf-8") as f:
        return json.load(f)
