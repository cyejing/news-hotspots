# Agents

维护 `news-digest` 时，优先遵循这里的规则；用户向说明和大模型主流程以 [SKILL.md](/Users/chenyejing/project/news-digest/SKILL.md) 与 [digest-prompt.md](/Users/chenyejing/project/news-digest/references/digest-prompt.md) 为准。

## 项目心智

- 主入口是 [run-pipeline.py](/Users/chenyejing/project/news-digest/scripts/run-pipeline.py)
- 最终给大模型的唯一推荐输入是 `/tmp/summary.json`
- 合并、评分、去重、topic 分组都在 [merge-sources.py](/Users/chenyejing/project/news-digest/scripts/merge-sources.py)
- 精简摘要 JSON 由 [merge-summarize.py](/Users/chenyejing/project/news-digest/scripts/merge-summarize.py) 生成
- 当前运行诊断看 `workspace/archive/news-digest/<DATE>/meta/` 中的 `*.meta.json`
- 最近 7 天历史诊断看 `workspace/archive/news-digest/<DATE>/meta/`

工作区覆盖顺序：

1. `<workspace>/config/news-digest-sources.json`
2. `<workspace>/config/news-digest-topics.json`
3. `config/defaults/`

归档目录：

- `<workspace>/archive/news-digest/<DATE>/json/`
- `<workspace>/archive/news-digest/<DATE>/markdown/`
- `<workspace>/archive/news-digest/<DATE>/meta/`

## Topic 规则

默认 topic：

- `ai-models`
- `ai-agents`
- `ai-ecosystem`
- `technology`
- `developer-tools`
- `github`
- `markets-business`
- `macro-policy`
- `world-affairs`
- `cybersecurity`

维护时重点遵守：

- `ai-models / ai-agents / ai-ecosystem` 命中时，不再同时挂 `technology`
- `github` 是独立 topic；GitHub Releases 和 GitHub Trending 统一只产出 `github`
- `macro-policy` 与 `markets-business` 不重复挂载
- `world-affairs` 独立，不再使用泛化 `news`
- 每个 source 默认只挂 1 个 topic；必要时最多 2 个；尽量不要 3 个以上

RSS 默认池规则：

- 机构媒体、官方博客、公共机构和行业媒体优先默认启用
- 明显个人博客保留在 `sources.rss` 尾部作为候选源
- 候选源统一用 `"enabled": false`

## 评分与去重

- `source priority` 只是轻量基础信号
- fetch 内互动/热度只影响该 fetch 内局部排序
- 跨 `source_type` 热点有有限加分
- 历史相似内容会明显降权
- topic 输出阶段会做来源多样性重排

如果改评分、去重或 topic 行为，优先同步检查：

- [merge-sources.py](/Users/chenyejing/project/news-digest/scripts/merge-sources.py)
- [merge-summarize.py](/Users/chenyejing/project/news-digest/scripts/merge-summarize.py)
- [references/digest-prompt.md](/Users/chenyejing/project/news-digest/references/digest-prompt.md)

## 限流规则

- 同一网站、同一域名默认串行抓取
- 默认优先靠 cooldown 主动降频，避免触发限流
- 新抓取脚本应提供可覆盖的 cooldown 环境变量
- `run-pipeline.py` 应把 `cooldown_s` 写入 `pipeline.meta.json`
- 除非确认目标站点稳定支持更高频率，否则不要加并发

## 诊断入口

配置检查：

```bash
uv run scripts/validate-config.py --verbose
```

当前运行诊断：

```bash
uv run scripts/source-health.py --input-dir workspace/archive/news-digest/<DATE> --verbose
```

最近 7 天历史诊断：

```bash
uv run scripts/source-health.py --input-dir workspace/archive/news-digest --verbose
```

说明：

- `run-pipeline.py` 会在 debug 目录下写每个步骤的 `.meta.json`
- `run-pipeline.py` 也会把 `summary.json` 和 `meta/` 归档到 `workspace/archive/news-digest/<DATE>/`
- `source-health.py` 统一使用 `--input-dir`；如果传入 archive 根目录，会自动聚合最近 7 天 `<DATE>/meta/` 下的元数据

## 测试入口

统一使用 [test-news-digest.sh](/Users/chenyejing/project/news-digest/scripts/test-news-digest.sh)。测试输出固定在 `/tmp/news-digest/`。

```bash
uv run scripts/test-news-digest.sh full
uv run scripts/test-news-digest.sh step rss
uv run scripts/test-news-digest.sh step merge
uv run scripts/test-news-digest.sh step summarize
uv run scripts/test-news-digest.sh health
uv run scripts/test-news-digest.sh unit
```

补充：

- `full` 会产出 `/tmp/news-digest/summary.json`，并把步骤诊断元数据归档到 `workspace/archive/news-digest/<DATE>/meta/`
- 单步骤测试复用 `/tmp/news-digest/` 下固定文件名
- 历史去重默认读取 `workspace/archive/news-digest/` 下所有日期目录中的 `json/`
