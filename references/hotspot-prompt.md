# 热点提示模板

使用前替换 `<...>` 占位符。严格按下面流程执行，不要自行改写脚本步骤。

## 占位符

| 占位符                | daily      | weekly      |
|--------------------|------------|-------------|
| `<MODE>`           | `daily`    | `weekly`    |
| `<RSS_HOURS>`      | `48`       | `168`       |
| `<EXTRA_SECTIONS>` | *(无)*      | `📊 每周趋势总结` |
| `<WORKSPACE>`      | 工作区路径      |             |
| `<SKILL_DIR>`      | skill 安装路径 |             |
| `<DATE>`           | YYYY-MM-DD |             |
| `<LANGUAGE>`       | `Chinese`  |             |

生成 **<DATE>** 的 <MODE> 全球科技与 AI 热点报告。使用 `<DATE>` 作为报告日期，不要推断。

## 执行步骤

1. 运行统一管道：
   ```bash
   uv run <SKILL_DIR>/scripts/run-pipeline.py \
     --defaults <SKILL_DIR>/config/defaults \
     --config <WORKSPACE>/config \
     --archive-dir <WORKSPACE>/archive/news-hotspots \
     --hours <RSS_HOURS> \
     --output /tmp/hotspots.json \
     --verbose --force
   ```
   - 这是明显的长耗时任务。执行前先判断当前环境是否支持 subagent
   - 如果支持 subagent，强烈要求优先并默认使用 subagent 运行这个命令，不要让主对话直接以前台同步方式阻塞执行
   - 只有在确认当前环境不支持 subagent 时，才允许查看当前环境适合哪种长任务运行方式，再选择替代方案
2. 只读取：
    - `/tmp/hotspots.json`
   - 不要读取 `meta/` 或任何 fetch 结果 JSON；诊断信息只在排障时才需要
3. 根据 `hotspots.json` 写 Markdown
4. 将 Markdown 保存到 `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/`
5. 不要并发运行多个热点任务；`/tmp/hotspots.json` 是固定路径，并发运行会互相覆盖

## 写作规则

- 只根据 `hotspots.json` 选材，不要自己重新打分或重排
- 使用 `hotspots.json` 中的 topic 顺序和 item 排序
- Markdown 按 topic 分类输出
- 每条至少包含：emoji、评分、标题、链接、来源
- 若 `metrics` 中有 likes/comments/replies/retweets/score，则一并展示
- 优先使用活跃感较强的 emoji，例如 `🔥` `🚀` `🧠` `⚠️` `💬`
- 分数可四舍五入到 1 位小数
- `<EXTRA_SECTIONS>` 只有在 weekly 时输出
- 使用 `<LANGUAGE>` 撰写全文

## 输出与归档

- 最终输出完整 Markdown，不要只给简版热点或只写几条精选
- 输出应与归档文件内容保持一致；如果平台单条消息长度受限，可分多段连续发送，但不要省略 topic 或条目
- 将 Markdown 保存到 `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/<MODE>.md`
- 如果同名文件已存在，改为 `<MODE>1.md`、`<MODE>2.md`

## 禁止事项

- 不要直接读取内部中间 JSON，也不要编写临时 Python 去重新解析它们
- 不要复制或改写脚本流程
