from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "fixtures" / "input.json"
GATEWAY_FIXTURE = ROOT / "fixtures" / "gateway_input.json"
DB_PATH = ROOT / "sensorgrab.db"

# 边缘缓存容量（条）：断网期间暂存样本的上限，溢出即留显式缺口
BUFFER_CAP = 5
# 每 tick 设备链路事务预算：实时读取优先占用，补采只用剩余额度
TICK_BUDGET = 4
# 慢设备熔断：连续超时达到阈值后打开熔断器，冷却若干个 tick 不再向其拉取
BREAKER_THRESHOLD = 3
BREAKER_COOLDOWN = 2
