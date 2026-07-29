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

读取归档 Markdown 后，**不要逐条翻译输出正文**。Markdown 仅作为总结的数据源，归档到磁盘供需要时查阅。

### 第 3 步：输出抓取概要 + 浓缩总结

最终汇报只输出两部分，不输出 markdown 正文：

**1. 抓取概要句** —— 从归档 Markdown 顶部 `summary` frontmatter 字段转写为自然语言。

`summary` 字段形如：
```
summary: mode:daily | total_articles:40 | api(43→3,fail=0) | google(251→13,fail=0) | rss(766→11,fail=0) | toutiao(20→1,fail=0) | twitter(351→12,fail=0) | zhihu(20→0,fail=0) | generated_at:2026-07-29T16:43:00+08:00
```

转写规则（中文示例，按 `<LANGUAGE>` 调整）：
- `total_articles` → 今日共抓取 N 篇文章
- 每个 `source(fetched→selected,fail=f)` → 来源名（fetched→selected 篇）
- `generated_at` → 生成时间 YYYY-MM-DD HH:MM
- 第二行固定输出 `<DATE> <daily|weekly> 全球科技与 AI 热点`

示例输出：
```
本次热点汇总：今日共抓取 40 篇文章，主要来源包括 API（43→3篇）、Google News（251→13篇）、RSS（766→11篇）、今日头条（20→1篇）、Twitter（351→12篇）和知乎（20→0篇），生成时间 2026-07-29 16:43。
2026-07-29 daily 全球科技与 AI 热点
```

**2. AI 浓缩总结** —— 基于归档 Markdown 全文（不逐条翻译标题），概括：
- 今日热点主题分布与主要事件
- 跨源共振信号（哪些话题被多个来源同时报道）
- 值得关注的趋势或异常

控制在 2–4 段，面向"一眼看懂今天发生了什么"，不要复述每条标题。

### 第 4 步：完成

daily 与 weekly 流程一致，均输出"抓取概要 + 浓缩总结"。不再追加独立的"本日/本周报告总结"标题——浓缩总结本身就是汇报内容。

---

## 完成定义

**仅运行 pipeline 不算完成。** 必须完成全部 4 步：

1. ✅ pipeline 脚本运行完毕（exit code 0）
2. ✅ 读取了归档 Markdown 文件
3. ✅ 输出了抓取概要句（从 summary 字段转写）
4. ✅ 输出了 AI 浓缩总结（基于全文，不逐条翻译）

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
