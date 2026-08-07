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

### 第 3 步：输出抓取概要 + 分主题结构化总结

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

**2. 分主题结构化总结** —— 基于归档 Markdown 全文，对每个主题的全部条目做处理：**合并近似项、去除噪音、剩余条目全部保留**，每条输出为 `- <总结>:<内容>` 形式：

- 按归档 Markdown 中的 `## 主题` 分组，每个主题一个二级标题（含 emoji），标题用源数据中的 `topic title`，不要自拟。
- **每条 bullet 形如 `- <总结>:<内容>`**：
  - **总结**是该条核心信息的短词或短语（如"模型即芯片"、"SK 海力士扩产"、"泰国校园枪击"），用 `:` 与正文分隔，起到扫读锚点作用。
  - **内容**是该条归纳后的一句话，提炼"谁做了什么、什么意义"，用易懂的自然语言讲清主线，不抄原始标题原文、不复述摘要原文。
- **合并近似**：同一事件的多条相关报道、或高度相似/同主题的条目，合并成一条，信息互补（如"扩产"与"短缺"可合成一条讲清因果闭环）。
- **去除噪音**：促销码、纯营销、与主题主线无关的低信号杂项直接省略，不单独成条。
- **剩余全保留**：合并近似、去掉噪音之后剩下的条目全部输出，**不设条数限制**——不为凑数压缩，也不为精简而丢掉有效信息。
- 末尾加一个独立的 `## 🔗 跨源共振` 块，2–4 条 bullet，列被多个来源同时报道的话题、叙事共振信号与值得关注的趋势或异常；无可写时省略该块。
- 全文为标题 + bullet 结构，不输出连续叙述段落，确保可纵向快速扫读。

示例（结构示意，非真实内容）：

```
## 🤖 AI 与芯片
- 模型即芯片:AMD 收购 Taalas 将模型刻入硅片，对标 Cerebras wafer-scale 路线
- 价值定价转向:DeepSeek 大幅涨价，智谱提价 83% 后调用量反增 400%，阿里 Qwen 拟向大用户收收入分成
- Qwen3.8 Max:被 agentic 指数评为最佳综合模型，开源模型以 100 倍成本优势在检索任务上击败 GPT-5.6
- Agent 标准化:Amazon/Cursor/Microsoft/OpenAI/Vercel 联合发布 Agent Plugins 1.0.0，统一扩展包格式并兼容 MCP

## 🏗️ 基础设施
- 资本涌入:Firmus 获 Coatue+Nvidia 20 亿美元融资，香港 DC 租金因字节需求暴涨
- 供应链黑天鹅:iPhone 18 Pro 遭 10 亿美元 DRAM 短缺，芯片积压
- 能源成本转嫁:弗吉尼亚州因 AI 数据中心电价飙升 76%，要求企业全额承担专用电力基建

## 🔗 跨源共振
- Musk 多条高互动动态与 AI/新能源叙事共振
- AI 视频降本、机器人世界杯 — 具身智能与内容生成中英文圈同步
```

### 第 4 步：完成

daily 与 weekly 流程一致，均输出"抓取概要 + 分主题结构化总结"。不再追加独立的"本日/本周报告总结"标题——结构化总结本身就是汇报内容。

---

## 完成定义

**仅运行 pipeline 不算完成。** 必须完成全部 4 步：

1. ✅ pipeline 脚本运行完毕（exit code 0）
2. ✅ 读取了归档 Markdown 文件
3. ✅ 输出了抓取概要句（从 summary 字段转写）
4. ✅ 输出了分主题结构化总结（每主题每条 `- <总结>:<内容>` 形式，合并近似、去噪、剩余全保留，含跨源共振块）

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
