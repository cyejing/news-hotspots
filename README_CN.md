# News Digest

> 自动化全球科技与 AI 资讯汇总，采用单入口管道与 `summary.json` 优先流程生成最终 Markdown 摘要。

[English](README.md) | **中文**

[![Tests](https://github.com/cyejing/news-digest/actions/workflows/test.yml/badge.svg)](https://github.com/cyejing/news-digest/actions/workflows/test.yml)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![ClawHub](https://img.shields.io/badge/ClawHub-news--digest-blueviolet)](https://clawhub.com/cyejing/news-digest)
[![MIT License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## 一句话安装

跟你的 [OpenClaw](https://openclaw.ai) AI 助手说：

> **"安装 news-digest，每天早上 9 点生成一份 Markdown 全球科技与 AI 摘要"**

Bot 会自动安装、配置、定时并生成归档。

或通过 CLI 安装：

```bash
clawhub install news-digest
```

## 你会得到什么

一个基于单入口管道生成的质量评分、去重摘要：

| 层级 | 数量 | 内容 |
|------|------|------|
| RSS | 配置驱动订阅源 | OpenAI、NVIDIA、TechCrunch、BBC、Fed、36氪 |
| GitHub | Releases + Trending | 关键项目更新和 GitHub Trending，统一归到 `github` topic |
| 社区 / 搜索 | Twitter、Reddit、Google News、V2EX | 高信号社区内容与搜索发现 |
| API 源 | 多个接口 | 微博热搜、华尔街见闻、腾讯新闻、Hacker News |

### 数据管道

```text
       run-pipeline.py
              ↓
  RSS ────────┐
  GitHub ─────┤── 并行采集 ──→ merge-sources.py
  GitHub Tr. ─┤
  API ────────┤
  Twitter ────┤
  Reddit ─────┤
  Google ─────┤
  V2EX ───────┘                          ↓
              质量评分 → 去重 → 主题分组
                             ↓
                      summary.json → Markdown 摘要
```

**质量评分**：采用分层评分。`source priority` 只作为轻量基础信号，再叠加 fetch 内部排序、有限封顶的跨源热点加分、少量时效性加分，并对与历史内容高度相似的条目做强惩罚。主题输出阶段还会做一次来源多样性重排，避免单一抓取链路占满前排。

推荐流程是：
- 只执行一次 `scripts/run-pipeline.py`
- 只读取产出的 `summary.json`
- 基于 `summary.json` 写最终 Markdown 摘要

诊断方面：
- 当前运行诊断来自当天归档目录中的 `meta/` 步骤元数据
- 最近 7 天历史诊断来自 `workspace/archive/news-digest/<DATE>/meta/`

## 配置

- `config/defaults/sources.json` — 148 个按 `type` 分组的内置配置型数据源
- `config/defaults/topics.json` — 10 个主题，含搜索查询和展示配置
- 用户配置放在 `workspace/config/`，优先级更高

当前默认主题包括 `ai-models`、`ai-agents`、`ai-ecosystem`、`technology`、`developer-tools`、`github`、`markets-business`、`macro-policy`、`world-affairs`、`cybersecurity`。

RSS 默认源池现在更保守：明显的个人博客仍保留在 `sources.rss` 中，但会放到列表尾部并以 `"enabled": false` 作为候选源处理。

`priority` 使用建议：
- `3` 作为大多数 source 的默认值。
- `4` 只给明显权威、稳定高信号的 source。
- `5` 只保留给极少数顶级一手 source。
- 不要批量抬高 priority，尽量让跨源确认、时效性和去重逻辑主导最终排序。

## 自定义数据源

```bash
cp config/defaults/sources.json workspace/config/news-digest-sources.json
cp config/defaults/topics.json workspace/config/news-digest-topics.json
```

工作区配置会与默认配置合并。匹配 `id` 会覆盖默认源，新 `id` 会追加源，`"enabled": false` 可禁用内置源。

## 环境变量

```bash
export GITHUB_TOKEN="..."
```

## 依赖

需要 Python 3.8+，以及：

```bash
pip install feedparser>=6.0.0 jsonschema>=4.0.0 requests>=2.28.0 beautifulsoup4>=4.12.0 rapidfuzz>=3.0.0
```

## 输出

- 管道先产出 `summary.json`，这是提供给大模型的唯一推荐输入。
- 最终对用户输出的是 Markdown 摘要。
- JSON 摘要归档到 `workspace/archive/news-digest/<DATE>/json/`。
- 用户 Markdown 归档到 `workspace/archive/news-digest/<DATE>/markdown/`。
- 步骤诊断元数据归档到 `workspace/archive/news-digest/<DATE>/meta/`。
- 如需同步到其他系统，请由外部自动化消费生成的 Markdown 归档。

## 健康诊断

查看当前运行：

```bash
uv run scripts/source-health.py --input-dir workspace/archive/news-digest/<DATE> --verbose
```

查看最近 7 天历史：

```bash
uv run scripts/source-health.py --input-dir workspace/archive/news-digest --verbose
```

## 仓库地址

**GitHub**: [github.com/cyejing/news-digest](https://github.com/cyejing/news-digest)

## 开源协议

MIT License — 详见 [LICENSE](LICENSE)
