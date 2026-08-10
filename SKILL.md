---
name: news-hotspots
description: 聚合 RSS、Twitter、Google News 等多源科技与 AI 新闻，生成每日/每周热点报告。支持生成今日新闻热点、创建每日定时任务、换一批新闻热点（重新抓取）、查看抓取健康诊断。
version: "5.1.0"
homepage: https://github.com/cyejing/news-hotspots
source: https://github.com/cyejing/news-hotspots
metadata:
  openclaw:
    requires:
      bins: [ "python3", "uv" ]
    optionalBins: [ "bb-browser" ]
files:
  read:
    - <SKILL_DIR>/config/defaults/: 默认配置
    - <SKILL_DIR>/references/: 参考文档
    - <SKILL_DIR>/scripts/: 管道脚本
    - <WORKSPACE>/config/: 工作区覆盖配置
    - <WORKSPACE>/archive/news-hotspots/: 历史热点归档
  write:
    - /tmp/: 临时 debug 目录
    - <WORKSPACE>/archive/news-hotspots/<DATE>/json/: 热点 JSON 归档
    - <WORKSPACE>/archive/news-hotspots/<DATE>/markdown/: Markdown 归档
    - <WORKSPACE>/archive/news-hotspots/<DATE>/meta/: 运行诊断元数据
---

# News Hotspots

聚合多源新闻，生成每日/每周热点报告。最终汇报只输出**抓取概要 + 分主题结构化总结 + 热点总结**，不逐条翻译标题；跨源共振条目就地 `🔥` 加亮，完整 Markdown 归档到磁盘供需要时查阅。

## 用户指令

本技能响应四类用户指令。识别后按对应流程执行，详细步骤见 `references/execution-guide.md`。

### 指令 1：生成今日新闻热点

**触发**："生成今日热点"、"今天有什么新闻"、"看今天的科技热点" 等。

**执行**：阅读 `references/execution-guide.md`，按其执行分工用 subagent 后台跑 pipeline，主会话等通知后读归档并输出抓取概要 + 分主题结构化总结 + 热点总结。

```bash
uv run <SKILL_DIR>/scripts/run-pipeline.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --archive <WORKSPACE>/archive/news-hotspots \
  --mode daily --hours 48 --verbose --force
```

- pipeline 耗时 15–30 分钟，由 subagent 后台执行，主会话不阻塞、不退出，等完成通知后读归档输出。
- 同一机器不要并发运行多个热点任务。
- 完成定义：pipeline exit 0 + 读取归档 + 输出概要句 + 输出结构化总结（跨源条目就地 `🔥` 加亮）+ 输出热点总结，缺一不可。

### 指令 2：生成每日定时任务

**触发**："每天早上 9 点发热点"、"设置每日定时任务"、"每周生成一次报告" 等。

**执行**：参考 `references/automation-template.md`，组装 cron payload。payload 指向 `execution-guide.md`，在当前 session 同步完成全流程（subagent 跑 pipeline → 主会话读归档输出）。

- daily 用 `--hours 48`，weekly 用 `--hours 168`。
- `sessionTarget: isolated`、`delivery.mode: announce`、`payload.timeoutSeconds: 3600`。

### 指令 3：换一批新闻热点（重新抓取）

**触发**："换一批"、"再看一批"、"刷新一下热点"、"还有别的吗" 等。

**执行**：重新跑 pipeline 抓取最新内容（与指令 1 相同），输出新的抓取概要 + 分主题结构化总结 + 热点总结。已看过的归档再读没有意义——换一批意味着重新抓取，不是读旧归档。

```bash
uv run <SKILL_DIR>/scripts/run-pipeline.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --archive <WORKSPACE>/archive/news-hotspots \
  --mode daily --hours 48 --verbose --force
```

> 注：若只想基于当天已抓取数据生成新热点而不重新抓取，可直接运行 `merge-hotspots.py`（见 `references/execution-guide.md` 失败处理段的"部分失败"修复命令），但默认"换一批"指重新抓取。

### 指令 4：查看抓取健康情况

**触发**："抓取情况怎么样"、"源健康吗"、"诊断一下"、"为什么没有 Twitter" 等。

**执行**：运行 `source-health.py`，读取诊断报告并向用户解读：哪些源失败/降级、最近连续失败天数、慢请求占比、日志错误模式（超时/限流/连接/解析/空结果）。

```bash
# 当天诊断
uv run <SKILL_DIR>/scripts/source-health.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --input <WORKSPACE>/archive/news-hotspots/<DATE>/meta \
  --verbose

# 最近 7 天历史诊断
uv run <SKILL_DIR>/scripts/source-health.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --input <WORKSPACE>/archive/news-hotspots \
  --verbose
```

- `--input` 指向某天 `meta/` → 当次运行报告；指向 archive 根 → 自动聚合最近 `diagnostics.history_days`（默认 7）天。
- 报告符号：✅ 健康 / ⚠️ 降级 / ⏭️ 跳过 / ⏱️ 超时 / 🚦 限流 / 🔌 连接 / 🐛 解析 / 📭 空结果 / 🔁 连续降级。

---

## 路径占位符

- `<SKILL_DIR>`: skill 仓库根目录
- `<WORKSPACE>`: 工作区根目录
- `<DATE>`: 归档日期 `YYYY-MM-DD`，以脚本实际产出为准
- `<LANGUAGE>`: 用户使用的语言
