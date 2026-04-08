---
name: news-hotspots
description: 聚合 RSS、GitHub、Twitter、Reddit、Google News 等多源科技与 AI 新闻，生成每日/每周热点报告，支持查看新闻热点、换一批新闻热点、创建定时任务与健康诊断。
version: "4.0.2"
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

| 用户意图         | 操作                                     | 说明                                       |
|--------------|----------------------------------------|--------------------------------------------|
| 查看今日/本周热点   | 阅读 `references/execution-guide.md`     | 抓取新的热点新闻，不是查看归档文件             |
| 当天换一批新闻    | 运行 `merge-hotspots.py`（见下方）            | 从已有数据生成新的一批热点，不重新抓取         |
| 创建定时任务     | 阅读 `references/automation-template.md` | 设置自动化定时抓取                          |
| 查看健康诊断     | 运行 `source-health.py`（见下方）             | 诊断数据源健康状态                          |

---

## 用户意图识别

**核心原则**：不要主动展示已归档的 markdown 给用户，已看过的内容重复看意义不大。

### 意图判断流程

```
用户请求 → 判断意图类型 → 执行对应操作
```

### 三种意图类型

#### 1. 查看今日/本周热点（默认行为）

**触发条件**：
- 用户说"查看今日热点"、"看看今天的新闻"、"今天有什么新闻"
- 用户说"查看本周热点"、"这周有什么新闻"
- 用户说"生成热点新闻"、"抓取新闻"
- 用户没有明确说"换一批"或"查看归档"

**执行操作**：阅读 `references/execution-guide.md`

**重要**：这不是查看归档文件，而是重新抓取最新内容。

#### 2. 当天换一批新闻

**触发条件**：
- 用户说"换一批"、"再看一批"、"其他新闻"
- 用户说"还有别的吗"、"换一批看看"
- 用户已经看过当天的热点，想要看不同的一批

**执行操作**：运行 `merge-hotspots.py` 从已有数据生成新的一批热点

**重要**：不重新抓取，只是从当天已抓取的数据中重新组合生成新的热点列表。

#### 3. 查看归档文件（明确要求）

**触发条件**：
- 用户明确说"查看归档"、"看看历史热点"
- 用户明确说"查看之前的热点"、"看昨天的热点"
- 用户明确要求查看特定日期的归档

**执行操作**：读取归档 markdown 文件并翻译输出

**重要**：只有用户明确要求时才执行此操作。

### 判断优先级

1. **最高优先级**：用户明确要求查看归档 → 读取归档文件
2. **次优先级**：用户说"换一批" → 执行 `merge-hotspots.py`
3. **默认行为**：其他情况 → 阅读 `references/execution-guide.md`

### 典型场景示例

| 用户输入                          | 识别意图        | 执行操作                              |
|---------------------------------|-------------|-----------------------------------|
| "查看今日热点"                      | 查看今日/本周热点   | 阅读 `references/execution-guide.md` |
| "今天有什么新闻"                     | 查看今日/本周热点   | 阅读 `references/execution-guide.md` |
| "换一批"                         | 当天换一批新闻     | 运行 `merge-hotspots.py`            |
| "再看一批"                        | 当天换一批新闻     | 运行 `merge-hotspots.py`            |
| "查看归档"                        | 查看归档文件      | 读取归档 markdown 并翻译输出               |
| "看看昨天的热点"（明确要求）              | 查看归档文件      | 读取归档 markdown 并翻译输出               |

---

## 路径说明

| 占位符           | 说明                             |
|---------------|--------------------------------|
| `<SKILL_DIR>` | 当前 skill 仓库根目录                 |
| `<WORKSPACE>` | 当前工作区根目录                       |
| `<DATE>`      | 归档日期，格式 `YYYY-MM-DD`，以脚本实际产出为准 |
| `<LANGUAGE>`  | 用户使用的语言                        |


## 当天换一批新闻

当用户想看"新一批还没看过的新闻"时，不需要重跑抓取，直接再次执行 `merge-hotspots.py`：

```bash
uv run <SKILL_DIR>/scripts/merge-hotspots.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --input <WORKSPACE>/archive/news-hotspots/<DATE>/json/merge-sources.json \
  --archive <WORKSPACE>/archive/news-hotspots \
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
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --input <WORKSPACE>/archive/news-hotspots/<DATE>/meta \
  --verbose
```

### 最近 7 天历史诊断

```bash
uv run <SKILL_DIR>/scripts/source-health.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --input <WORKSPACE>/archive/news-hotspots \
  --verbose
```

输出结构：

- `History report`：历史健康趋势
- `Run details`：每次运行的详细诊断
