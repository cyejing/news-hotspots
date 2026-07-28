# 热点生成执行指南

## 占位符

| 占位符 | 说明 |
|--------|------|
| `<SKILL_DIR>` | skill 仓库根目录 |
| `<WORKSPACE>` | 工作区根目录 |
| `<DATE>` | 归档日期 `YYYY-MM-DD`，以脚本实际产出为准 |
| `<MODE>` | `daily` 或 `weekly` |
| `<RSS_HOURS>` | daily 为 48，weekly 为 168 |
| `<LANGUAGE>` | 用户使用的语言 |

---

## 核心原则

**谁启动 pipeline，谁负责到底。**

执行 agent 必须在**当前 session 同步完成**全流程：运行 → 等待 → 读取 → 翻译 → 输出。

**禁止**：spawn 子任务后提前结束当前 session。

---

## 执行流程

### 第 1 步：运行 pipeline

**同步执行**，设置 timeout ≥ 1800 秒（脚本耗时 15-30 分钟）：

```bash
uv run <SKILL_DIR>/scripts/run-pipeline.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --archive <WORKSPACE>/archive/news-hotspots \
  --mode <MODE> \
  --hours <RSS_HOURS> \
  --verbose --force
```

约束：同一机器不要并发运行多个热点任务。

### 第 2 步：读取归档

```bash
cat <WORKSPACE>/archive/news-hotspots/<DATE>/markdown/<mode>.md
```

`<DATE>` 以脚本实际产出的目录名为准。不确定时用 `ls <WORKSPACE>/archive/news-hotspots/` 找最新日期目录。

### 第 3 步：翻译输出

将归档 Markdown 翻译为 `<LANGUAGE>`，**直接输出文本内容**。

翻译规则：
1. **顶部 Summary**：将 `summary` 字段转为自然语言（数据来源、总量、时间）
2. **翻译标题和摘要**：每个 item 的 `<title> - <summary>` 翻译为 `<LANGUAGE>`
3. **保持结构**：Markdown 结构、标题层级、链接格式不变
4. **保留原字段**：`⭐分数`、`| source_name |`、`| metrics |` 不翻译
5. **完整保留**：不删减任何 topic 和 item

示例：
```
原始：
---
summary: mode:daily | total_articles:15 | rss:8 | twitter:4 | google:3 | generated_at:2026-04-02T10:00:00+00:00
---
# 2026-04-02 daily 全球科技与 AI 热点
## AI Frontier
1. ⭐9.4 | OpenAI releases GPT-5 with breakthrough reasoning - The new model shows significant improvements in multi-step reasoning tasks | OpenAI Blog | likes=120

翻译后（中文）：
---
本次热点汇总：今日共抓取 15 篇文章，主要来源包括 RSS（8 篇）、Twitter（4 篇）和 Google（3 篇），生成时间 2026-04-02 10:00。
---
# 2026-04-02 daily 全球科技与 AI 热点
## AI Frontier
1. ⭐9.4 | OpenAI 发布具备突破性推理能力的 GPT-5 - 新模型在多步推理任务中表现出显著提升 | OpenAI Blog | likes=120
```

### 第 4 步：追加总结

- daily: 结尾追加 `## 本日报告总结`
- weekly: 结尾追加 `## 本周报告总结`

AI 总结基于归档 Markdown 正文，概括热点归纳、主要主题和信号变化。

---

## 完成定义

**仅运行 pipeline 不算完成。** 必须完成全部 4 步：

1. ✅ pipeline 脚本运行完毕（exit code 0）
2. ✅ 读取了归档 Markdown 文件
3. ✅ 翻译并输出了完整报告
4. ✅ 追加了报告总结

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

失败类型：

1. **完全失败**：没有 `merge-sources.json` → 重新运行 `run-pipeline.py`

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
