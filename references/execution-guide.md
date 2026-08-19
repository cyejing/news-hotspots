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

### 1. 抓取概要句（一行）

从归档 Markdown 顶部 `summary` frontmatter 转写为自然语言，展示各源抓取情况（各源是否成功抓到数据、抓取量）：

```
本次热点汇总：今日共抓取 N 篇文章，主要来源包括 RSS（X→Y篇）、API（X→Y篇）、Twitter（X→Y篇）……生成时间 YYYY-MM-DD HH:MM。
YYYY-MM-DD daily 全球科技与 AI 热点
```

其中 `X→Y` 为该源"抓取→入选"篇数，`fail=` 非零时需点出该源有失败项。

### 2. 分主题结构化总结（标题 + bullet）

- 按归档 Markdown 的 `## 主题` 分组，每个主题一个二级标题（带 emoji），标题用源数据中的 topic title，不自拟
- **每条 bullet 格式**：`- <总结词>:<一句话归纳>`
  - 总结词是核心信息的短词/短语，用 `:` 分隔，扫读锚点
  - 内容提炼"谁做了什么、什么意义"，不抄标题原文
- **合并近似**：同事件的多条报道合并成一条，信息互补
- **去除噪音**：促销码、纯营销、低信号杂项直接省略，不单独成条
- **剩余全保留**：不设条数上限，不为精简而丢有效信息
- **跨源共振就地加亮**：被不同 source_type 同时报道的条目，行首加 `🔥`
- 全篇标题 + bullet 结构，不出现连续叙述段落

### 3. 热点总结块（模版 A：事件驱动 + 合并 + 高层次总结）

`## 📌 热点总结` 作为独立块，格式如下：

1. **事件驱动 bullet**：按报告实际内容，每条一个核心事件/主题，格式 `- **短词**:一句话归纳`（不填固定字段如主线/走势/共振，按内容自然总结）
2. **合并相似**：同一事件的多条报道、跨主题高度相似的内容合并成一条，信息互补
3. **高层次总结**：bullet 之后追加 1–2 句话，对全部内容做整体收束——点出事件间的内在联系或后续方向，不罗列数字、不重复 bullet

示例：
```
## 📌 热点总结
- **AI 开源浪潮**：Hugging Face 一周内连发两款开源模型，3B 参数击败百亿闭源、30B agentic 模型 24GB 显存可跑
- **人形机器人上岗**：中欧美三方同日将人形机器人部署进宝马产线，Physical AI 从概念走向落地
- **哥伦比亚地震**：7.4 级地震致 132 人死亡，该国数年来最大地震
- **Anthropic 水印落地**：Claude 文本嵌入隐形水印，AI 生成内容溯源成行业标配
- **腾讯 AI 助手逆袭**：WorkBuddy 月活破 2000 万，超越字节阿里成国内首位

整体来看，AI 从"能力竞赛"转向"开源普惠 + 硬件落地 + 治理框架"三线并进；非 AI 侧哥伦比亚地震、消费电子成本压力等事件影响面较广，但非主线。
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

