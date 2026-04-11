# 热点生成执行指南

## 占位符说明

| 占位符 | 说明 |
|--------|------|
| `<SKILL_DIR>` | skill 仓库根目录 |
| `<WORKSPACE>` | 工作区根目录 |
| `<DATE>` | 归档日期 `YYYY-MM-DD`，以脚本实际产出为准 |
| `<MODE>` | 运行模式，`daily` 或 `weekly` |
| `<RSS_HOURS>` | RSS 抓取时间窗口，daily 为 48，weekly 为 168 |
| `<LANGUAGE>` | 用户使用的语言 |

---

## 执行分工

**Subagent 职责**：运行脚本并等待归档文件生成，不负责读取或翻译。

完成条件：
- `<WORKSPACE>/archive/news-hotspots/<DATE>/json/<mode>*.json`
- `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/<mode>*.md`
- `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/*.meta.json`

**主会话职责**：将归档 Markdown 翻译为 `<LANGUAGE>` 并输出，结尾追加 AI 总结。

---

## 脚本执行

**长耗时任务（15-30 分钟），必须使用 subagent 执行，超时设置 30 分钟以上。**

约束：同一机器不要并发运行多个热点任务。

```bash
uv run <SKILL_DIR>/scripts/run-pipeline.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --archive <WORKSPACE>/archive/news-hotspots \
  --mode <MODE> \
  --hours <RSS_HOURS> \
  --verbose --force
```

参数：
- `<MODE>`: `daily` 或 `weekly`
- `<RSS_HOURS>`: daily 为 `48`，weekly 为 `168`

---

## 归档路径

- JSON: `<WORKSPACE>/archive/news-hotspots/<DATE>/json/<mode>.json`
- Markdown: `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/<mode>.md`
- Meta: `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/*.meta.json`

`<DATE>` 格式为 `YYYY-MM-DD`，以脚本实际产出为准。

---

## 主会话输出约束

**适用场景**：无论是 `run-pipeline.py` 还是 `merge-hotspots.py` 生成的归档 Markdown，主会话都必须遵循以下约束。

### 必须遵守

1. **顶部 Summary**：将 `summary` 字段总结为自然语言（数据来源、总量、时间等）
2. **翻译标题和摘要**：每个 item 的 `<title> - <summary>` 翻译为 `<LANGUAGE>`
3. **保持结构**：Markdown 结构、标题层级、链接格式不变
4. **保留其他字段**：`⭐分数`、`| source_name |`、`| metrics |` 不翻译
5. **完整保留**：不删减任何 topic 和 item

### 输出示例

原始：
```
---
summary: mode:daily | total_articles:15 | rss:8 | twitter:4 | github:3 | generated_at:2026-04-02T10:00:00+00:00
---
# 2026-04-02 daily 全球科技与 AI 热点
## AI Frontier
1. ⭐9.4 | OpenAI releases GPT-5 with breakthrough reasoning - The new model shows significant improvements in multi-step reasoning tasks | OpenAI Blog | likes=120
```

翻译后（中文）：
```
---
本次热点汇总：今日共抓取 15 篇文章，主要来源包括 RSS（8 篇）、Twitter（4 篇）和 GitHub（3 篇），生成时间 2026-04-02 10:00。
---
# 2026-04-02 daily 全球科技与 AI 热点
## AI Frontier
1. ⭐9.4 | OpenAI 发布具备突破性推理能力的 GPT-5 - 新模型在多步推理任务中表现出显著提升 | OpenAI Blog | likes=120
```

### 必须追加

- daily: 结尾追加 `## 本日报告总结`
- weekly: 结尾追加 `## 本周报告总结`

AI 总结基于归档 Markdown 正文，概括热点归纳、主要主题和信号变化。

---

## 失败处理

先查看诊断：
```bash
uv run <SKILL_DIR>/scripts/source-health.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --input <WORKSPACE>/archive/news-hotspots/<DATE>/meta \
  --verbose
```

### 失败类型

1. **完全失败**：没有 `merge-sources.json` → 用 subagent 重跑 `run-pipeline.py`

2. **部分失败**：有 `merge-sources.json` 但没有 markdown → 运行：
   ```bash
   uv run <SKILL_DIR>/scripts/merge-hotspots.py \
     --defaults <SKILL_DIR>/config/defaults \
     --config <WORKSPACE>/config \
     --input <WORKSPACE>/archive/news-hotspots/<DATE>/json/merge-sources.json \
     --archive <WORKSPACE>/archive/news-hotspots \
     --mode <MODE>
   ```

3. **无法恢复**：向用户报告实际完成情况，**不能伪造完整热点结果**
