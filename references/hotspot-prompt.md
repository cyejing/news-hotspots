# 热点提示模板

使用前替换 `<...>` 占位符。严格按下面流程执行。

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
2. 只读取 `/tmp/hotspots.json`
3. 根据 `hotspots.json` 写 Markdown
4. 将 Markdown 保存到 `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/`
5. 不要并发运行多个热点任务；`/tmp/hotspots.json` 是固定路径，并发运行会互相覆盖

## 写作规则

- 只根据 `hotspots.json` 选材，不要自己重新打分或重排
- 使用 `hotspots.json` 中的 topic 顺序和 item 排序
- 每条尽量 2 行看完
- 使用 `<LANGUAGE>` 撰写全文

## 固定格式

按下面模板输出，不要自定义结构：

```markdown
# <DATE> <MODE> 全球科技与 AI 热点
## <Topic Title 1>
- 1. ⭐<Score> | [<Title>](<URL>)
  指标：likes=<n>, comments=<n>, replies=<n>, retweets=<n>, score=<n> | 来源：<Source Name>
## <Topic Title 2>
- 1. ⭐<Score> | [<Title>](<URL>)
  来源：<Source Name>
<EXTRA_SECTIONS>
```

- 第一行固定为：编号 + `⭐8.7` 这种评分 + 带链接标题
- 第二行固定为：`来源：...` 或 `指标：... | 来源：...`
- 标题必须使用 Markdown 超链接格式 `[标题](链接)`
- 不要省略 topic 标题
- 每条 item 尽量控制在 2 行内
- 不要额外输出摘要、导语或扩展说明

## 输出与归档

- 最终输出完整 Markdown，不要只给简版热点或只写几条精选
- 输出应与归档文件内容保持一致；如果平台单条消息长度受限，可分多段连续发送，但不要省略 topic 或条目
- 将 Markdown 保存到 `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/<MODE>.md`
- 如果同名文件已存在，改为 `<MODE>1.md`、`<MODE>2.md`

## 禁止事项

- 不要读取内部中间 JSON，也不要编写临时 Python 去重新解析它们
- 不要复制或改写脚本流程
- subagent 不能对返回结果进行摘要、节选或压缩，必须返回完整 Markdown 原文
