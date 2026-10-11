# 历史快照与只读行情接口

`hybrid_quant.market_data.StagedMarketData`由本项目独立实现。界面查看本地候选行情与引擎接受已核验历史输入使用不同接口；当前真实数据注册表仍未通过准入。

## 候选行情查看

`catalog(dataset, q='', limit=50)`支持`stock_daily`与`etf_daily`，仅查询已登记清单。证券代码为六位数字，可带`sh`或`sz`前缀；查询最长32字符，单次最多500项。

`preview_bars(dataset, symbol, limit=200)`核验文件SHA-256后读取单份Parquet，验证行数、唯一递增日期、OHLC与清单范围。价格保持未复权，接口固定返回`backtest_ready=false`，不产生策略目标。

| 字段 | 当前处理 |
|---|---|
| `trade_date` | 已规范化交易日期，不把原时间整数或文件修改时间当作发布证据 |
| `volume_raw` | 原始成交量值 |
| `volume_shares_estimate` | 有来源估计时单独保留，不能视为已核验成交股数 |
| `turnover` | 未知时为空，不从成交量与价格推算 |
| `adjusted_close` | 未知时为空，不用未复权收盘价替代 |
| `available_at` | 未知时为空，不使用文件时间或假定收盘时间 |
| `tradable` | 未知时为空，不从存在日线记录推断可交易 |

## 引擎历史快照边界

`load_snapshot(bundle_manifest)`首先执行`DatasetRegistry.validate_for_backtest()`。当前调用会被拒绝，不能通过给快照自行标记`backtest_ready=true`来跳过注册表准入。

通过准入后，快照清单和JSON载荷都必须位于项目内。清单要求`schema_version='1.0'`、`source_kind='verified_history'`、`backtest_ready=true`、空`readiness_gaps`，并提供相对`payload`路径与`payload_sha256`。接口验证结构和保存证据，不负责产生缺失历史数据，也不提供强制准入。

载荷字段：

| 字段 | 要求 |
|---|---|
| `decision_at` | 带时区的ISO决策时间 |
| `securities` | 显式提供`Security`全部字段，包括历史元信息、穿透核验与新建仓资格；未经核验不能获新建仓资格 |
| `bars` | 显式提供`DailyBar`全部字段；复权收盘价、成交额、可用时间和三种交易标记不得缺省；同证券日期唯一递增 |
| `factors` | 显式提供质量、价值、可用时间与原始版本核验标记；缺失分数可以为空，未经核验版本保持false |
| `listing_age_sessions` | 覆盖全部证券的非负整数交易日龄，不按自然日或周一至周五近似 |

证券、行情与因子使用`hybrid_quant.models`内的数据类。布尔值须为JSON布尔值，不能用字符串`"false"`代替；数值须有限，重复JSON键拒绝接受。载荷可保留未来和不可用观察，后续`DataSnapshot.known_history`及策略按决策截止时间过滤。结构校验不能替代历史来源核验。
