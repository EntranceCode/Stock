# 混合策略量化投资系统

当前版本：v0.2.1，独立系统核心、本地研究工作台与真实数据准备。项目位于`D:\vscode\Stock`，系统Python包为`hybrid_quant`；适用范围是A股个股和场内ETF，日线决策，只做多、不使用杠杆。

用户确定的方向：个股选股 + ETF轮动；最大回撤研究目标为15%–20%。该目标不是收益或风险承诺。

已实现个股多因子、ETF轮动、防御模块、组合风控、次日订单模拟与现金持仓账本，并提供合成数据演示。新增本地工作台，可查看数据覆盖、查询股票与ETF日线、浏览只读策略配置、运行工程演示和分页检查订单/成交/决策记录。运行时无AlphaMaster依赖；原有行情只作为待核验数据来源。实现范围及后续开发见 [开发状态](docs/development-status.md)，研究规则见 [混合策略系统设计](docs/hybrid-system-design.md)。权重和参数仍是待验证假设，尚未进行真实历史策略回测或连接券商。

在本目录运行：

```powershell
python -B -m hybrid_quant serve --port 8765
python -B -m hybrid_quant demo
python -B -m hybrid_quant status
python -B -m hybrid_quant check-data
python -B scripts/run_tests.py
```

启动`serve`后打开 [本地研究工作台](http://127.0.0.1:8765)。服务仅监听本机地址；在运行服务的终端按`Ctrl+C`停止。当前已安装的解释器也可使用`D:\miniconda3\python.exe`。核心和合成演示使用Python标准库；真实行情预览另需`pyarrow`，本项目已在`.deps/parquet`保存本地依赖。

`demo`使用合成证券、行情、财务与日期验证完整程序链路，结果保存在唯一的`reports/demo/<运行ID>`目录，不能用来证明投资收益。`check-data`在真实历史证据尚不完整时返回退出码2；没有强制跳过准入的开关。

行情预览先核验归档文件哈希，再读取单只证券日线；复权、成交额、字段可用时间与交易状态未知时保持为空。预览接口不生成策略信号，合格历史输入的独立契约见 [快照接口说明](docs/snapshot-bundle-contract.md)。本轮145项自动检查通过；工作台浏览器验证在v0.2.0完成，见 [工作台验证记录](reports/dashboard-validation/validation.md)。

已检查用户提供的本地行情数据，见 [数据检查与接入评估](docs/local-data-assessment.md)。股票日线可作为候选数据，但时间编码、复权和成交量单位需核验；指数异常、分钟线精度损失及财务/ETF数据缺口尚待解决。只读检查脚本见 `scripts/inspect_market_data.py`，详细结果保存在 `reports/local-data-audit/`。

后续已完成130条参考日线比对和4,272份股票日线规范化，补充6只ETF行情、覆盖4,272只股票的137,714条财务快照。2026-10-09继续补充本地股票池26,590条分红送转源记录，规范化输出保留26,585条股票事件和36条ETF事件；5条金额字段冲突记录待原公告核验。见 [数据准备进度](docs/data-preparation-progress.md) 和 [财务字段审计](reports/financial-field-audit.md)。

已补充2018年以来上交所计划交易日历，与5只上交所ETF截至2026-09-30的日期逐日一致。交易所证券导出整理为3,268条上市/终止日期候选、7,490条历史简称事件及独立的2,904条当前元信息快照。研究窗口内236个退市A股代码中有232个缺本地行情；见 [证券元信息审计](reports/security-reference-audit.md) 和 [退市行情补采清单](reports/missing-retired-prices.md)。当前本地池不能视为完整历史股票池，当前行业和股本不能回填历史。

采集与处理时间分开记录，新增原始响应版本不覆盖旧证据。公司行动类型、历史证券状态、财务历史版本等缺口解决前，真实回测准入保持关闭。原有接入工具仍保存在`scripts/`，与系统核心分开维护。
