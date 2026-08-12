---
name: news-hotspots
description: 聚合 RSS、Twitter、Google News 等多源科技与 AI 新闻，生成每日/每周热点报告。支持生成今日新闻热点、创建每日定时任务、换一批新闻热点（重新抓取）、查看抓取健康诊断。
version: "5.2.0"
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

主会话最终发给用户的报告必须包含三部分：
1. **抓取概要句**（从归档 `summary` frontmatter 转写）
2. **分主题结构化总结**（标题 + bullet，跨源共振就地 `🔥` 加亮，去除噪音）
3. **热点总结块**（`## 📌 热点总结`，唯一允许的叙述段落）

## 路径占位符

- `<SKILL_DIR>`: skill 仓库根目录
- `<WORKSPACE>`: 工作区根目录
- `<DATE>`: 归档日期 `YYYY-MM-DD`，以脚本实际产出为准

## 注意事项

- 同一机器不要并发运行多个热点任务（固定 debug/archive 路径会互相覆盖）
- pipeline 部分源失败仍会产出 daily.md（status=partial），报告照常输出；源健康细节走指令 4
