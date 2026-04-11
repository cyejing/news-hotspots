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

## 用户意图识别

**核心原则**：不主动展示已归档 markdown，除非用户明确要求。

### 意图判断（按优先级）

| 优先级 | 触发条件 | 执行操作 |
|--------|----------|----------|
| 1 | 用户明确说"查看归档"、"历史热点"、"昨天的热点" | 读取归档 markdown 并翻译输出 |
| 2 | 用户说"换一批"、"再看一批"、"还有别的吗" | 运行 `merge-hotspots.py`（见下方） |
| 3 | 其他情况（默认） | 阅读 `references/execution-guide.md` |

**重要**：
- 优先级 3 是重新抓取最新内容，不是查看归档
- 优先级 2 不重新抓取，从当天已有数据生成新热点

---

## 操作命令

### 当天换一批新闻

```bash
uv run <SKILL_DIR>/scripts/merge-hotspots.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --input <WORKSPACE>/archive/news-hotspots/<DATE>/json/merge-sources.json \
  --archive <WORKSPACE>/archive/news-hotspots \
  --mode daily
```

### 健康诊断

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

---

## 路径占位符

- `<SKILL_DIR>`: skill 仓库根目录
- `<WORKSPACE>`: 工作区根目录
- `<DATE>`: 归档日期 `YYYY-MM-DD`，以脚本实际产出为准
- `<LANGUAGE>`: 用户使用的语言
