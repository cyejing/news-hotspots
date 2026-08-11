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

pipeline 是 15–30 分钟的长任务，用 **subagent 后台执行**，主会话不被阻塞、不退出，等 subagent 完成通知后再读取归档并输出。

**Subagent 职责**：运行 `run-pipeline.py` 到完成（exit code 0 或 1），归档 JSON/Markdown/meta 全部落盘，向主会话报告完成状态与归档路径。**不负责读取或翻译，但输出总结时必须严格遵循以下格式规范（见下方"分主题结构化总结"节），不得自创格式。**

完成条件（归档文件已生成）：
- `<WORKSPACE>/archive/news-hotspots/<DATE>/json/<mode>*.json`
- `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/<mode>*.md`
- `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/*.meta.json`

归档路径从 pipeline stdout 的 `✅ Done → <path>` 或 `pipeline.meta.json` 的 `outputs.markdown_output` 读取，不用 `ls` 猜目录。

**主会话职责**：启动 subagent 后**不退出**，等 subagent 完成通知，读归档 Markdown，翻译为 `<LANGUAGE>` 输出抓取概要 + 分主题结构化总结 + 热点总结。

> ⚠️ 主会话启动 subagent 后必须等其完成通知再收尾，不能 spawn 后提前结束当前 session——否则归档无人读取、报告丢失。

约束：同一机器不要并发运行多个热点任务（固定 debug/archive 路径会互相覆盖）。

---

## 脚本执行

subagent 运行以下命令：

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

### 长任务稳定性保证

- pipeline 在 subagent 后台运行，主会话不阻塞，不受单次工具调用 timeout 限制。
- 每个 step 的超时由 `runtime.json` 控制：`fetch_step_timeout_s=2000` / `merge_timeout_s=300` / `hotspots_timeout_s=120`。单 step 超时会被杀并记为 `timeout` 状态，不影响其他 step。
- 归档 JSON/Markdown/meta 全部落盘到 `<WORKSPACE>/archive/news-hotspots/<DATE>/`，部分 step 失败仍可从已有归档恢复（见失败处理）。

### 成败判定

- **exit 0**（`status=ok` 或 `partial`）：pipeline 成功或部分成功（部分源挂了但产出了 `merge-sources.json` 与归档）→ 主会话读归档输出。
- **exit 1**（完全失败，无 `merge-sources.json`）→ 走失败处理。

---

## 归档路径

- JSON: `<WORKSPACE>/archive/news-hotspots/<DATE>/json/<mode>.json`
- Markdown: `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/<mode>.md`
- Meta: `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/*.meta.json`

`<DATE>` 格式为 `YYYY-MM-DD`，以脚本实际产出的目录名为准（从 subagent 报告的归档路径中解析）。

---

## 主会话输出约束

**适用场景**：无论是 `run-pipeline.py` 还是 `merge-hotspots.py` 生成的归档 Markdown，主会话都必须遵循以下约束。

### 必须遵守

最终汇报输出三部分，不输出 markdown 正文：

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
- **跨源共振就地加亮**：被多个来源（不同 `source_type`）同时报道的条目，在 bullet 行首加 `🔥` 标记即可，**不再单列跨源共振块**——共振信息留在所属主题内、跟着条目走，便于在扫读该主题时一眼看到。判定依据是归档 Markdown 中每条 bullet 末尾的 `*source_label*` 来源标注（同一 `source_type` 的不同源不算）。
- 全文为标题 + bullet 结构，不输出连续叙述段落，确保可纵向快速扫读。

示例（结构示意，非真实内容）：

```
## 🤖 AI 与芯片
- 模型即芯片:AMD 收购 Taalas 将模型刻入硅片，对标 Cerebras wafer-scale 路线
- 价值定价转向:DeepSeek 大幅涨价，智谱提价 83% 后调用量反增 400%，阿里 Qwen 拟向大用户收收入分成
- Qwen3.8 Max:被 agentic 指数评为最佳综合模型，开源模型以 100 倍成本优势在检索任务上击败 GPT-5.6
- 🔥 Agent 标准化:Amazon/Cursor/Microsoft/OpenAI/Vercel 联合发布 Agent Plugins 1.0.0（API 与 RSS 同步报道）

## 🏗️ 基础设施
- 🔥 资本涌入:Firmus 获 Coatue+Nvidia 20 亿美元融资，香港 DC 租金因字节需求暴涨（RSS 与 API 同步报道）
- 供应链黑天鹅:iPhone 18 Pro 遭 10 亿美元 DRAM 短缺，芯片积压
- 能源成本转嫁:弗吉尼亚州因 AI 数据中心电价飙升 76%，要求企业全额承担专用电力基建

## 📌 热点总结
今日热点的主线是「AI 产业化进入定价与基建博弈期」：模型侧从拼参数转向拼商业回报（DeepSeek/Qwen 涨价或抽成），同时算力与能源需求反向施压供应链与地方电网，形成"模型涨价—算力扩张—能源承压"的闭环。开源模型以成本优势在部分任务反超闭源，预示企业采购将从单一旗舰模型向组合调度迁移。Agent 标准化与资本涌入被多源同步报道，是后续 1–2 周值得跟踪的趋势信号。
```

**3. 热点总结** —— 全部主题输出完毕后，追加一个独立的 `## 📌 热点总结` 块，对本期抓取的内容做整体收束而非罗列：

- 篇幅约一段（4–8 句），允许连续叙述，**这是全文唯一允许的叙述段落**，其余部分仍是标题 + bullet。
- 内容维度（按需取 2–4 项，不机械堆砌）：
  - **主线概括**：当日/当周的核心叙事是什么，用一句话点出主题间的内在联系（如"模型涨价—算力扩张—能源承压"的闭环）。
  - **热点走势**：与近期（可结合记忆/历史归档）对比，哪些在升温、哪些在退潮、哪些是新出现信号；指出方向而非罗列数字。
  - **共振与异常**：被多源同时报道、或跨主题反复出现的信号，点出值得后续 1–2 周跟踪的趋势。
  - **判断与留白**：如适用，给出一句倾向性判断或待观察的开放问题；无判断时省略，不要硬凑。
- 不再单列跨源共振块——共振信息已在条目级用 `🔥` 就地标注，此处只在高层维度提炼其含义。

### 完成定义

**仅运行 pipeline 不算完成。** 必须完成：
1. ✅ pipeline 脚本运行完毕（exit code 0）
2. ✅ 读取了归档 Markdown 文件
3. ✅ 输出了抓取概要句（从 summary 字段转写）
4. ✅ 输出了分主题结构化总结（跨源共振条目就地 `🔥` 加亮）
5. ✅ 输出了热点总结块（`## 📌 热点总结`）

---

## 失败处理

pipeline exit 1 或归档缺失时，先查看诊断：
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
