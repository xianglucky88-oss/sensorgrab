# sensorgrab — 边缘网关按 Modbus/MQTT 类协议轮询量测，断网缓存补采、单位与时间规整入库。

运行栈：Python 3.14 + 标准库 `sqlite3` + `httpx` + `pytest`（离线，无需联网）

## 运行方式
```text
make run     # 离线重放 fixtures/input.json（旧输入）与 fixtures/gateway_input.json（网关场景），输出摄取摘要
make test    # 运行项目测试：流水线、幂等性、网关端到端与阶段单元行为
```

## 数据流与模块边界
每个业务阶段保持单一职责；`pipeline.py` 只负责按顺序调用阶段，不在编排层加入业务判断。
阶段可返回 0..N 条记录（缓存暂存 / 补采展开），落库前经 `keyer` 派生稳定主键、`store` 幂等写入。

```text
loader → normalize → protocol → poller → buffer → backfill → unit_store → keyer → store
```

- `normalize`：身份证据校验（设备/通道/重启代次/序列号）与双时间戳（采样/接收分开）；
  证据不足 → `pending` 保留待判断；检测时钟回拨并留证。旧版通用记录保持原规整行为。
- `protocol`：按各自协议区分 **有效响应 / 坏帧 / 超时 / 确认**
  （modbus：CRC、异常码、写确认；mqtt：payload 校验、PUBACK、dup 重传）。
- `poller`：实时调度。连续超时超阈值 → 熔断，冷却期内跳过对该慢设备的拉取（mqtt 推送不节流），
  慢设备不拖住其他设备；被跳过的量测由设备侧缓存、补采兜底。
- `buffer`：边缘缓存。uplink 中断时样本持久化暂存（重启不丢）；恢复后按序排空；
  溢出逐出最旧并留下 `buffer_overflow` 显式缺口。
- `backfill`：按 (device,channel,epoch) 高水位检测缺序 → `missing` 缺口；
  重连批量补传展开为 `origin=backfill` 样本；设备声明 `lost` 区间 → `history_lost` 永久缺口；
  与实时读取共享每 tick 预算（实时优先，补采只用剩余额度，超出排队到后续 tick）。
- `unit_store`：按 sample_ts 生效的校准版本换算 `std = raw*scale + offset`，
  记录单位与校准版本；无校准 → 转 `pending`。
- `store`：质量感知幂等合并（confirmed>pending，good>uncertain>bad），
  迟到的低质量样本不覆盖已确认值；游标（高水位/落库/设备确认）、缺口、校准历史、
  缓存与事件日志全部落 sqlite，重启后对得上。

## 数据模型
- **样本身份** = 设备 + 通道 + 重启代次(epoch) + 序列号(seq)。设备重启 epoch 递增：
  同一物理量测的重传键不变（重复补传不增加计量），新量测键不同；时钟回拨不影响身份。
- **双时间戳**：`sample_ts`（设备采样时刻）与 `recv_ts`（网关接收时刻）分开保存。
- **质量与状态**：`quality`(good/uncertain/bad) + `status`(confirmed/pending)；
  `basis` 记录质量依据（协议判定、clock_rollback、buffered、backfilled、cal:版本）。
- **可追溯**：raw（原始值）、std（标准值）、unit、calibration_version、quality basis 全部随样本落库；
  校准修订保留历史版本，不改写已入库样本。
- **计量**：`metered` = confirmed 样本数；重复/更差的迟到样本只计 `duplicates`，不增加计量。

## 取舍说明（实时进度 / 补采覆盖 / 缓存资源）
- **实时优先于补采**：每 tick 预算 `TICK_BUDGET` 由实时读取先占用，补采只用剩余额度。
  实时进度不受补采风暴影响；代价是断网恢复后补采覆盖被摊到多个 tick
  （`backfill_deferred` 计数可见积压）。调大预算可加快补采，但设备链路竞争加剧。
- **慢设备熔断**：熔断保护其他设备的实时进度与共享预算；代价是冷却期该设备出现缺口，
  依赖设备侧保留历史经补采最终补齐（历史已丢失则留下 `history_lost` 永久缺口）。
- **缓存容量**：`BUFFER_CAP` 越大吸收突发能力越强、溢出缺口越少；代价是边缘存储占用
  与恢复后排空时延。溢出策略为逐出最旧 + 显式缺口：宁可留缺口也不静默丢失。
- **质量优先合并**：已确认值不被迟到的低质量样本覆盖，读数稳定；
  代价是更优证据到达前，低质量先到的样本只能以低质量形式暂存，等待升级。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于 `fixtures/`
（`input.json` 为旧输入并保持兼容，`gateway_input.json` 为网关端到端场景，
`gateway_restart_part1/2.json` 演示重启后缓存/游标/落库对账），运行方式以 `Makefile` 为准。
