# 热点提示模板

这个文档是全球科技与 AI 热点任务的执行契约，负责定义执行门禁、运行流程和产物归档规则；它面向执行代理使用，不负责定义主会话如何向用户回复。

## 强制执行约束

- 固定说明：`run-pipeline.py` 是长耗时任务，通常需要 `15-30` 分钟，必须使用 subagent 运行；如果当前环境没有 subagent，再考虑其他后台代理或长任务执行方式
- 同一台机器上不要并发运行多个热点任务
- 不要手动改写脚本流程
- 不要返回摘要版、精选版或压缩版结果

## 任务目标

- 跑完统一管道
- 生成并归档最终热点 JSON 与 Markdown
- 返回完整归档 Markdown 原文，而不是摘要版

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

## 执行前确认

未满足以下任一项时，不要启动任务：

- 同一台机器上当前没有其他热点任务在运行
- `<WORKSPACE>`、`<SKILL_DIR>`、`<DATE>` 已经明确
- 输出语言使用 `<LANGUAGE>`

全部满足后，再开始执行“执行流程”。

## 执行流程

1. 运行统一管道：
   ```bash
   uv run <SKILL_DIR>/scripts/run-pipeline.py \
     --defaults <SKILL_DIR>/config/defaults \
     --config <WORKSPACE>/config \
     --archive <WORKSPACE>/archive/news-hotspots \
     --hours <RSS_HOURS> \
     --verbose --force
   ```
2. 不要手动改写脚本流程；`run-pipeline.py` 会调用 `merge-hotspots.py` 生成最终热点 JSON 和 Markdown。
3. 最终热点 JSON 归档到 `<WORKSPACE>/archive/news-hotspots/<DATE>/json/`。
4. 最终 Markdown 归档到 `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/`。
5. JSON 与 Markdown 使用同一组文件名后缀，例如 `hotspots1.json` 与 `hotspots1.md`。
6. 如需回传结果，必须读取归档后的最终 Markdown 原文，不要自行重写。

归档文件：
- `merge-hotspots.py` 生成 `<WORKSPACE>/archive/news-hotspots/<DATE>/json/hotspots*.json`
- `merge-hotspots.py` 生成 `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/hotspots*.md`
- `run-pipeline.py` 归档 `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/*.meta.json`

## 成功条件

满足以下全部条件，任务才算完成：

- 至少存在一个归档后的 `hotspots*.json`
- 至少存在一个归档后的 `hotspots*.md`
- JSON 与 Markdown 文件名后缀一致
- 返回内容与归档 Markdown 原文一致

## 写作规则

- 只根据 `merge-hotspots.py` 生成的最终热点 JSON 内容，不要自己重新打分或重排
- 使用最终热点 JSON 中的 topic 顺序和 item 排序
- 每条 item 必须控制在 2 行内
- 必须使用 `<LANGUAGE>` 撰写全文
- 不要新增导语、总结、编者按或额外说明

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
- 输出必须覆盖全部 topic 与条目，不要只保留精选

## 禁止事项

- 不要读取中间抓取结果 JSON，也不要编写临时 Python 去重新解析它们
- 不要复制或改写脚本流程
- 不要对最终 Markdown 做摘要、节选或压缩
- 不要并发运行多个热点任务
- 这个文档不负责定义最终如何向用户输出内容

## 失败时怎么做

出现以下任一情况时，不要把当前结果当成成功任务返回：

- `run-pipeline.py` 失败；优先查看 `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/` 下的 `*.meta.json`
- 如果最终 Markdown 没有成功归档，不要自行手写热点内容充当结果
- 如果只拿到部分内容，不要返回“精选版”或“简版”
