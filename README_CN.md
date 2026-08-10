# News Hotspots

> 自动化全球科技与 AI 热点汇总，采用单入口管道与脚本生成最终热点产物。最终汇报输出抓取概要 + 分主题结构化总结 + 热点总结，完整 Markdown 归档到磁盘供需要时查阅。

[English](README.md) | **中文**

[![Tests](https://github.com/cyejing/news-hotspots/actions/workflows/test.yml/badge.svg)](https://github.com/cyejing/news-hotspots/actions/workflows/test.yml)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![ClawHub](https://img.shields.io/badge/ClawHub-news--hotspots-blueviolet)](https://clawhub.com/cyejing/news-hotspots)
[![MIT License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## 一句话安装

跟你的 [OpenClaw](https://openclaw.ai) AI 助手说任意一句：

> **"生成今日新闻热点"** — 运行管道，立即输出一份总结。
> **"设置每天早上 9 点的每日热点任务"** — 创建定时任务。
> **"换一批新闻热点"** — 重新抓取，输出一份新的总结。
> **"今天抓取健康情况怎么样"** — 运行诊断并解读报告。

Bot 会自动安装、配置、定时、抓取并交付。

或通过 CLI 安装：

```bash
clawhub install news-hotspots
```

## 你会得到什么

一个基于单入口管道生成的质量评分、去重热点流：

| 层级 | 数量 | 内容 |
|------|------|------|
| RSS | 配置驱动订阅源 | OpenAI、NVIDIA、TechCrunch、BBC、Fed、36氪 |
| 社区 / 搜索 | Twitter、Google News | 高信号社交与搜索发现 |
| API 源 | 多个接口 | 微博热搜、华尔街见闻、腾讯新闻、Hacker News |

### 数据管道

```text
       run-pipeline.py
              ↓
  RSS ────────┐
  Twitter ────┤── 并行采集 ──→ merge-sources.py
  Google ─────┤
  API ────────┤
  Zhihu ──────┤
  Weibo ──────┤
  Toutiao ────┘                          ↓
              质量评分 → 去重 → 主题分组
                             ↓
           merge-hotspots.py → 归档 JSON + Markdown
```

**质量评分**：采用分层评分。`source priority` 只作为轻量基础信号，再叠加 fetch 内部排序、有限封顶的跨源热点加分、少量时效性加分，并对与历史内容高度相似的条目做强惩罚。主题输出阶段还会做一次来源多样性重排，避免单一抓取链路占满前排。每个主题默认输出最多 **10** 条（`pipeline.default_hotspots_top_n`）。过短或无信息标题（如纯 `@mention`、单词推文）在评分前会被硬过滤。

推荐流程是：
- 只执行一次 `scripts/run-pipeline.py`
- 由 `merge-hotspots.py` 生成并归档最终热点 JSON 和 Markdown
- 对外交付**抓取概要 + 分主题结构化总结 + 热点总结**（归档 Markdown 不再逐条回显）

归档文件：
- `merge-hotspots.py` 生成最终 JSON 和 Markdown
- `run-pipeline.py` 归档 `meta` 诊断文件

诊断方面：
- 当前运行诊断来自当天归档目录中的 `meta/` 步骤元数据
- 最近 7 天历史诊断来自 `workspace/archive/news-hotspots/<DATE>/meta/`

## 配置

- `config/defaults/rss.json` / `twitter.json` — 按抓取类型拆分的内置 source 配置
- `config/defaults/api.json` — 拆分后的 API source 配置
- `config/defaults/topics.json` — 主题，含搜索查询和展示配置
- `config/defaults/runtime.json` — 抓取超时、cooldown、重试、并发、limit、诊断阈值和 pipeline 默认值
- 用户配置放在 `workspace/config/`，优先级更高

当前默认主题包括 `ai-frontier`、`ai-infra`、`twitter`、`technology`、`business`、`value-investing`、`world`、`science`、`social`。

RSS 默认源池现在更保守：明显的个人博客仍保留在 `rss.json` 中，但会放到列表尾部并以 `"enabled": false` 作为候选源处理。

`priority` 使用建议：
- `3` 作为大多数 source 的默认值。
- `4` 只给明显权威、稳定高信号的 source。
- `5` 只保留给极少数顶级一手 source。
- 不要批量抬高 priority，尽量让跨源确认、时效性和去重逻辑主导最终排序。

## 自定义数据源

```bash
cp config/defaults/rss.json workspace/config/news-hotspots-rss.json
cp config/defaults/twitter.json workspace/config/news-hotspots-twitter.json
cp config/defaults/topics.json workspace/config/news-hotspots-topics.json
```

工作区配置会与默认配置合并。匹配 `id` 会覆盖默认源，新 `id` 会追加源，`"enabled": false` 可禁用内置源。

## Runtime 覆盖

```bash
cp config/defaults/runtime.json workspace/config/news-hotspots-runtime.json
```

runtime 配置采用深合并：
- CLI 显式参数优先于 runtime 默认值
- `workspace/config/news-hotspots-runtime.json` 会覆盖 `config/defaults/runtime.json`
- 未覆盖的兄弟字段继续沿用默认值

runtime 主要收口这些可调参数：
- 各类 fetch 的 timeout、cooldown、retry、并发、limit
- pipeline 的 step timeout、merge timeout、hotspots timeout、archive retention、默认 top N
- `source-health.py` 和 `step_contract.py` 使用的诊断阈值

## 运行时配置

超时与 cooldown 等参数统一通过 `runtime.json` 配置（见 `config/defaults/runtime.json`），无必需的运行时环境变量。RSS 抓取遵循标准的 `ALL_PROXY` / `HTTPS_PROXY` / `HTTP_PROXY` 环境变量（含 `socks5h://`），设置即生效。

## 依赖

需要 Python 3.8+，以及：

```bash
pip install feedparser>=6.0.0 jsonschema>=4.0.0 requests>=2.28.0 beautifulsoup4>=4.12.0 rapidfuzz>=3.0.0
```

## 输出

- `merge-hotspots.py` 会生成并归档最终热点 JSON 和 Markdown。
- 最终对用户交付的是**抓取概要 + 分主题结构化总结 + 热点总结**（来源数量、总量、生成时间，再加按主题分块、形如 `- <总结>:<内容>` 的 bullet——每主题合并近似项、去除噪音、剩余条目全部保留且不设条数限制；跨源共振条目就地 `🔥` 加亮而非单列共振块；全部主题输出后追加一段热点总结，从主线概括/热点走势/共振异常维度收束）。归档 Markdown 留在磁盘供需要时查阅，不再逐条回显。
- JSON 热点归档到 `workspace/archive/news-hotspots/<DATE>/json/`。
- Markdown 归档到 `workspace/archive/news-hotspots/<DATE>/markdown/`。
- 归档文件名按 `mode` 命名，例如 `daily.json`、`daily.md`、`weekly.json`、`weekly1.md`。
- 步骤诊断元数据归档到 `workspace/archive/news-hotspots/<DATE>/meta/`。
- 如需同步到其他系统，请由外部自动化消费生成的 Markdown 归档。

## 健康诊断

`source-health.py` 只读取 `meta/*.meta.json`（不回读 fetch 结果 JSON），并分析：

- 各源状态、调用统计与失败项
- **日志错误模式分类**：基于 `logs.tail` 与 `failed_items[].error`，归类为 超时 / 限流 / 连接 / 解析 / 空结果
- **慢请求占比**：来自 `slow_requests` 相对总调用数
- **items 趋势**（近 N 次中位数）与**连续降级天数**

查看当前运行：

```bash
uv run scripts/source-health.py --defaults config/defaults --config workspace/config --input workspace/archive/news-hotspots/<DATE>/meta --verbose
```

查看最近 7 天历史（自动聚合最近 `diagnostics.history_days` 天，默认 7）：

```bash
uv run scripts/source-health.py --defaults config/defaults --config workspace/config --input workspace/archive/news-hotspots --verbose
```

## 仓库地址

**GitHub**: [github.com/cyejing/news-hotspots](https://github.com/cyejing/news-hotspots)

## 开源协议

MIT License — 详见 [LICENSE](LICENSE)
