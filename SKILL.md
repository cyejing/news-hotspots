---
name: news-hotspots
description: 聚合 RSS、Twitter、Google News 等多源科技与 AI 新闻，生成每日/每周热点报告。支持生成今日新闻热点、创建每日定时任务、换一批新闻热点（重新抓取）、查看抓取健康诊断。
version: "5.3.0"
homepage: https://github.com/cyejing/news-hotspots
source: https://github.com/cyejing/news-hotspots
metadata:
  openclaw:
    requires:
      bins: [ "python3", "uv" ]
    optionalBins: [ "bb-browser" ]
files:
  read:
    - <SKILL_DIR>/config/defaults/: 默认配置
    - <SKILL_DIR>/references/: 参考文档
    - <SKILL_DIR>/scripts/: 管道脚本
    - <WORKSPACE>/config/: 工作区覆盖配置
    - <WORKSPACE>/archive/news-hotspots/: 历史热点归档
  write:
    - <WORKSPACE>/archive/news-hotspots/<DATE>/json/: 热点 JSON 归档
    - <WORKSPACE>/archive/news-hotspots/<DATE>/markdown/: Markdown 归档
    - <WORKSPACE>/archive/news-hotspots/<DATE>/meta/: 运行诊断元数据
---

# News Hotspots

聚合多源新闻，生成每日/每周热点报告。最终汇报只输出**抓取概要 + 分主题结构化总结 + 热点总结**，不逐条翻译标题；跨源共振条目就地 `🔥` 加亮，完整 Markdown 归档到磁盘供需要时查阅。

## 统一执行流程（指令 / 定时任务 共用）

无论是「生成今日热点」「换一批」「定时任务触发」，都用**同一条路径**完成：

```
主会话 → spawn subagent 跑 pipeline（15-30分钟）
       → subagent 返回格式化的报告文本
       → 主会话直接发给用户
```

### 主会话职责

1. **spawn subagent**（见下方 Subagent 职责），传参：模式（daily/weekly）、hours
2. **用 `sessions_yield` 结束当前回合**，等 subagent 完成事件
3. subagent 返回的报告文本直接发给用户

### bb-browser 前置检查（主流程启动前自动执行）

pipeline 启动时（`run-pipeline.py`）会自动执行 **bb-browser 可用性探测**：

1. **探测命令**（从各 fetch 脚本实际调用中抽取，均无需登录态、轻量超时）：
   - `bb-browser site zhihu/hot`（知乎热榜）
   - `bb-browser site google/news "OpenAI"`（Google News）
   - `bb-browser site twitter/search "AI"`（Twitter 搜索）
   - `bb-browser site weibo/hot`（微博热榜）
   - `bb-browser site toutiao/hot`（头条热榜）
   - `bb-browser site xueqiu/user 1247347556 3`（雪球用户动态，轻量取 3 条；依赖已有登录态）
   - 只要**任意一条成功**（返回条目数 > 0）即判定 bb-browser 可用
2. **全部失败 → 自动恢复**：参考 `browser-restart` 技能，调用其 `restart_browser.py` 重启 Chrome + 重建 bb-browser daemon
3. **重启后再次探测**：仍失败则继续主流程，但日志明确警告 bb-browser 源可能失败

探测逻辑与重启触发由 `scripts/check_bb_browser.py` 实现，可在 pipeline 外单独运行验证：

```bash
python3 <SKILL_DIR>/scripts/check_bb_browser.py
```

单独运行可查看完整探测日志与最终可用性判定。

### Subagent 职责

1. 运行 pipeline（daily 用 `--hours 48`，weekly 用 `--hours 168`）：
   ```bash
   uv run <SKILL_DIR>/scripts/run-pipeline.py \
     --defaults <SKILL_DIR>/config/defaults \
     --config <WORKSPACE>/config \
     --archive <WORKSPACE>/archive/news-hotspots \
     --mode <MODE> --hours <HOURS> --verbose --force
   ```
2. 从 pipeline stdout 的 `✅ Done → <path>` 或 `pipeline.meta.json` 的 `outputs.markdown_output` 取 markdown 路径
3. **读 daily.md，按 `references/execution-guide.md` 的输出约束生成完整中文报告**
4. 把报告作为 subagent 的最终文本返回给主会话

**Subagent 严禁输出过程信息**：exit code、抓取文章数、各源耗时、源成败明细——这些留在 `meta/*.meta.json` 诊断里，需要时由指令 4「健康诊断」专门读取。

### 指令路由

| 用户意图 | 动作 |
|----------|------|
| "生成今日热点" / "今天有什么新闻" / "看今天的科技热点" | 按统一流程跑 daily |
| "换一批" / "再看一批" / "刷新一下热点" | 重新跑 pipeline（同 daily 命令，`--force` 已保证重新抓取） |
| "设置每日定时任务" / "每周生成一次报告" | 参考 `references/automation-template.md` 组装 cron，payload 指向本流程 |
| "抓取情况怎么样" / "源健康吗" / "为什么没有 Twitter" | 运行 `source-health.py`（见指令 4） |
| "检查浏览器抓取状态" / "bb-browser 可用吗" / "抓取正常吗" | 运行 `python3 <SKILL_DIR>/scripts/check_bb_browser.py`，向用户汇报各探测命令结果与可用性判定（含是否触发自动重启），不跑 pipeline |

### 指令 4：查看抓取健康情况

```bash
uv run <SKILL_DIR>/scripts/source-health.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --input <WORKSPACE>/archive/news-hotspots/<DATE>/meta --verbose

# 最近 7 天历史诊断
uv run <SKILL_DIR>/scripts/source-health.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --input <WORKSPACE>/archive/news-hotspots --verbose
```

报告符号：✅ 健康 / ⚠️ 降级 / ⏭️ 跳过 / ⏱️ 超时 / 🚦 限流 / 🔌 连接 / 🐛 解析 / 📭 空结果 / 🔁 连续降级。

## 输出约束（详见 references/execution-guide.md）

主会话最终发给用户的报告必须包含三部分（固定模板骨架与格式细则见 `references/execution-guide.md`）：
1. **渠道健康行**（标题下 `**数据规模**：` 一行，每源 `{✅/⚠️/❌}{源名} {总条数}→{筛选后条数}`，按 API、Google、RSS、头条、推特、微博、雪球、知乎固定顺序，数据取自归档 `summary` frontmatter）
2. **分主题结构化总结**（标题 + bullet，每条 `- **总结词**:一句话归纳`，跨源共振就地 `🔥` 加亮，去除噪音）
3. **热点总结块**（`## 📌 热点总结`，归纳式：同类新闻合并成一条归纳，无共性才按「有价值+有热点」单列，唯一允许的叙述段落）

## 路径占位符

- `<SKILL_DIR>`: skill 仓库根目录
- `<WORKSPACE>`: 工作区根目录
- `<DATE>`: 归档日期 `YYYY-MM-DD`，以脚本实际产出为准

## 注意事项

- 主题体系（v6.0，2026-10-31重构）：7 分类 = 3 兴趣主题（🤖 AI 前沿 = 模型/智能体/芯片/算力全栈；💰 价值投资 = Buffett/股东信/估值，段永平由雪球源覆盖；🐦 推特圈 = 关注账号时间线）+ 2 领域主题（💻 科技数码；📈 商业财经）+ 2 雷达主题（🌍 国际时事；🔥 社会热点 = 纯热榜源，不做关键词分流）。抓取源整建制归属主题，不拆分路由
- 同一机器不要并发运行多个热点任务（固定 debug/archive 路径会互相覆盖）
- pipeline 部分源失败仍会产出 daily.md（status=partial），报告照常输出；源健康细节走指令 4
- 中文 RSS：爱范儿/少数派已复活，量子位已接入；36氪/机器之心无 RSS，未接入
- tldr 步骤（HTML 抓取型，非 RSS）：tldr.tech/ai → ai-frontier、tldr.tech/tech → technology；每日 issue 页 `<article>` 块解析，新旧两版页面结构兼容，Sponsor 条目过滤，当日无内容自动回退 4 天
- X Explore 热点（fetch-twitter 内置）：`twitter/news` 自定义适配器（Explore GraphQL 端点逆向）拿新闻卡片（标题+讨论量 posts），整建制归 world 主题，hot_score=posts 讨论数
- 内容过滤：工作区覆盖配置 `<WORKSPACE>/config/news-hotspots-runtime.json` 的 `content_filters.blocked_keywords`（标题/摘要关键词，不区分大小写）会在最终热点输出前剔除命中条目（如老板要求过滤 `["比特币", "bitcoin", "btc"]`）；过滤统计写入 daily JSON 的 `content_filter` 字段；`content_filters.junk_keywords` 可覆盖内置硬垃圾词表（18+ video、casino、promo code 等 SEO 垃圾模式，命中即剔除，默认内置 ~17 条）
- Google 噪音降权：`fetch.google.relevance_gate_enabled`（默认 true）对标题+摘要均未命中主题特征词的 google 结果打 `relevance_gate_pass=false` 标记并在 merge 阶段扣分降权（不硬丢弃，避免误杀正经新闻）；惩罚分值 `merge-sources.py` 的 `RELEVANCE_PENALTY_SCORE`
