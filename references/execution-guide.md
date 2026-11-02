# 热点生成执行指南

> **执行流程** 已在 SKILL.md「统一执行流程」中描述，本文档只定义两件事：
> 1. 主会话发给用户的报告必须遵循的输出格式约束
> 2. pipeline 失败时的处理方式

## 占位符

| 占位符 | 说明 |
|--------|------|
| `<SKILL_DIR>` | skill 仓库根目录 |
| `<WORKSPACE>` | 工作区根目录 |
| `<DATE>` | 归档日期 `YYYY-MM-DD`，以脚本实际产出为准 |
| `<MODE>` | 运行模式，`daily` 或 `weekly` |
| `<RSS_HOURS>` | daily=48，weekly=168 |

## 输出约束

最终汇报**必须同时包含以下三部分**，不输出 markdown 正文，不混入其他过程信息：

### 固定模板骨架（每次严格照此结构输出）

```
# 📰 <DATE> 全球科技与 AI 热点日报
**数据规模**：{各源状态项，空格分隔}

## <emoji> <主题标题>
- **<总结词>**:<一句话归纳> _（<渠道·源名>）_
（按归档主题重复）

## 📌 热点总结
- **<共性短词>**:合并归纳
{1–2 句收束}
```

### 1. 渠道健康行（标题下第一行）

紧跟报告标题输出一行 `**数据规模**：`，逐源展示抓取健康度。每源固定格式 `{符号}{源名} {总条数}→{筛选后条数}`，源之间用空格分隔，按固定顺序：API、Google、RSS、TLDR、头条、推特、微博、雪球、知乎（frontmatter 中缺失的源跳过）；超长可拆两行：

**数据规模**：✅API 24→4 ✅Google 215→24 ✅RSS 639→23 ✅头条 19→2
✅推特 268→32 ✅微博 1→1 ✅雪球 20→3 ✅知乎 20→1

- 数据取自归档 Markdown 顶部 `summary` frontmatter 的 `源(fetched→selected,fail=N)`：箭头方向与 frontmatter 一致，左侧为抓取总条数，右侧为筛选后入选条数
- 符号语义：✅ = 抓到数据且无失败项（fail=0 且 selected>0）；⚠️ = 抓到数据但有失败项（fail>0）；❌ = 完全失败（selected=0 或源缺失）
- 除本行外不输出任何抓取过程信息；fail 明细不展开，渠道诊断走 source-health.py

### 2. 分主题结构化总结（标题 + bullet）

- 按归档 Markdown 的 `## 主题` 分组，每个主题一个二级标题（带 emoji），标题用源数据中的 topic title，不自拟
- **每条 bullet 格式**：`- **<总结词>**:<一句话归纳> _（<渠道·源名>）_`
  - 总结词是核心信息的短词/短语，必须**加粗**，用 `:` 分隔，扫读锚点
  - 内容提炼"谁做了什么、什么意义"，不抄标题原文
- **尾部来源标注**：每条 bullet 结尾加斜体来源标注 `_（<渠道·源名>）_`，与正文间留一个空格
  - 命名格式 `渠道·源名`；渠道取值：RSS / TLDR / Google / API / 头条 / 推特 / 微博 / 雪球 / 知乎
  - 推特账号用 handle：`_（推特·@karpathy）_`；X Explore 热点写 `_（推特·X Explore）_`；热榜类源名可省渠道直接写 `_（微博）_`/`_（知乎）_`；雪球写 `_（雪球·段永平）_`
  - 合并条目列出全部来源，用 ` + ` 连接，最多列 3 个、超出写 `等`：`_（RSS·SemiAnalysis + 推特·@elonmusk + 微博 等）_`
  - 📌 热点总结块是归纳段落，不加来源标注
- **合并近似**：同事件的多条报道合并成一条，信息互补
- **去除噪音**：促销码、纯营销、低信号杂项直接省略，不单独成条
- **去噪合并后有价值才输出**：每主题素材（数据层 Top 10）经合并近似、剔除噪音后，仅输出有信息价值的内容；条数不设上限，也不为凑数硬保留低信号条目
- **跨源共振就地加亮**：被不同 source_type 同时报道的条目，行首加 `🔥`
- 全篇标题 + bullet 结构，不出现连续叙述段落

### 3. 热点总结块（归纳式）

`## 📌 热点总结` 作为独立块，是对**全文内容的归纳总结**，以共性归纳为主线：

1. **先合并同类**：通读全部分主题内容，把同类事件、同一驱动因素或同一事件的多条新闻**合并成一条 bullet 归纳**（格式 `- **短词**:一句话归纳`）。一条 bullet 可覆盖多源多条目：先讲共同点，再补关键差异；能合并的必须合并，不逐事件单列
2. **无共性才单列**：确实独立、无法归入任何共性的内容，按「有价值 + 有热点」挑选后单独成条，宁缺毋滥
3. **收束 1–2 句**：bullet 之后追加 1–2 句话，每句必须点名具体事件或具体主体，只写两类内容：
   - **事件间联系**：共同驱动、因果或对比关系
   - **后续方向**：下一步值得跟踪的具体事
   不得写无具体对象的泛泛收束（如「影响面较广」「但非主线」「值得持续关注」）。不罗列数字流水账、不重复正文 bullet 原文。

示例（合并归纳式）：
```
## 📌 热点总结
- **AI 基建回报周期成定价主线**：Intel AI 芯片产能不足盘后跌 17%、亚马逊巨额 capex 压股价、德银警告 AI 崩盘将冲击美元——财报密集期市场集中质疑 AI 投入产出比
- **老旧软件栈一攻一守**：Binance 63 亿美元失守源头是一台被攻陷的 DevOps 笔记本，思科却用 AI 两个月重写 98% 遗留代码——AI 同时放大旧栈风险与翻新速度
- **美国中期选举冲刺**：马斯克单条 19.5 万赞催票、伊朗战争成核心辩题，科技巨头深度卷入政治议程
- **GPT-6 三维理解不及格**：ScienceVision 仅 0.53 分——"会建模"不等于"看得懂世界"
```

## 失败处理

### 诊断

```bash
uv run <SKILL_DIR>/scripts/source-health.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --input <WORKSPACE>/archive/news-hotspots/<DATE>/meta --verbose
```

### 失败类型与处理

| 失败类型 | 判断条件 | 处理方式 |
|----------|----------|----------|
| 完全失败 | 无 `merge-sources.json` | 用 subagent 重跑 pipeline |
| 部分失败 | 有 `merge-sources.json` 但无 markdown | 运行 merge-hotspots.py 补救 |
| 无法恢复 | 无归档 | 向用户如实报告，不伪造结果 |

**merge-hotspots.py 补救命令**：
```bash
uv run <SKILL_DIR>/scripts/merge-hotspots.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --input <WORKSPACE>/archive/news-hotspots/<DATE>/json/merge-sources.json \
  --archive <WORKSPACE>/archive/news-hotspots \
  --mode <MODE>
```

## 归档路径

- JSON: `<WORKSPACE>/archive/news-hotspots/<DATE>/json/<MODE>.json`
- Markdown: `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/<MODE>.md`
- Meta: `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/*.meta.json`

