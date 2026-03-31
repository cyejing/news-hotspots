---
name: news-hotspots
description: 聚合 RSS、GitHub、Twitter、Reddit、Google News 等来源的全球科技与 AI 新闻，生成每日/每周新闻热点报告。
version: "3.21.2"
homepage: https://github.com/cyejing/news-hotspots
source: https://github.com/cyejing/news-hotspots
metadata:
  openclaw:
    requires:
      bins: [ "python3", "uv" ]
    optionalBins: [ "bb-browser" ]
env:
  - name: GITHUB_TOKEN
    required: false
    description: GitHub token，提高 GitHub API 速率限制
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

聚合多源新闻，生成每日/每周热点报告。

## 功能

| 用户意图    | 操作                                     |
|---------|----------------------------------------|
| 生成热点新闻  | 阅读 `references/execution-guide.md`     |
| 创建定时任务  | 阅读 `references/automation-template.md` |
| 当天换一批新闻 | 运行 `merge-hotspots.py`（见下方）            |
| 查看健康诊断  | 运行 `source-health.py`（见下方）             |

---

## 当天换一批新闻

当用户想看"新一批还没看过的新闻"时，不需要重跑抓取，直接再次执行 `merge-hotspots.py`：

```bash
uv run <SKILL_DIR>/scripts/merge-hotspots.py \
  --input <WORKSPACE>/archive/news-hotspots/<DATE>/json/merge-sources.json \
  --archive <WORKSPACE>/archive/news-hotspots \
  --debug /tmp/news-hotspots/debug \
  --mode daily
```

行为说明：

- 脚本会读取当天已有的 `daily*.json`，已看过的条目不会再次出现
- `merge-hotspots.py` 会从 `merge-sources.json` 的 `source_types` 分组重建 topic，并按 `source_type` 轮转填充每个 topic
- 新结果归档为 `daily1.json`、`daily2.json` 等

---

## 查看健康诊断

### 当天诊断

```bash
uv run <SKILL_DIR>/scripts/source-health.py \
  --input <WORKSPACE>/archive/news-hotspots/<DATE>/meta \
  --verbose
```

### 最近 7 天历史诊断

```bash
uv run <SKILL_DIR>/scripts/source-health.py \
  --input <WORKSPACE>/archive/news-hotspots \
  --verbose
```

输出结构：

- `History report`：历史健康趋势
- `Run details`：每次运行的详细诊断

---

## 路径说明

| 占位符           | 说明                             |
|---------------|--------------------------------|
| `<SKILL_DIR>` | 当前 skill 仓库根目录                 |
| `<WORKSPACE>` | 当前工作区根目录                       |
| `<DATE>`      | 归档日期，格式 `YYYY-MM-DD`，以脚本实际产出为准 |
| `<LANGUAGE>`  | 用户使用的语言                        |
