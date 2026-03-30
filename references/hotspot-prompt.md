# 热点提示模板

这个文档是全球科技与 AI 热点任务的执行契约。它只回答四件事：

1. 任务启动前必须满足什么条件
2. subagent 和主会话分别负责什么
3. `daily` 和 `weekly` 的最终输出各自要满足什么约束
4. 任务中断后如何恢复

## 输入参数

| 占位符           | daily        | weekly       |
|---------------|--------------|--------------|
| `<MODE>`      | `daily`      | `weekly`     |
| `<RSS_HOURS>` | `48`         | `168`        |
| `<WORKSPACE>` | 工作区路径        | 工作区路径        |
| `<SKILL_DIR>` | skill 安装路径   | skill 安装路径   |
| `<DATE>`      | `YYYY-MM-DD` | `YYYY-MM-DD` |
| `<LANGUAGE>`  | `Chinese`    | `Chinese`    |

## 全局规则

- `run-pipeline.py` 是长耗时任务，通常需要 `15-30` 分钟
- 必须优先使用 subagent 运行；如果当前环境没有 subagent，再改用其他长任务执行方式
- subagent 超时时间必须设置为 `30` 分钟
- 同一台机器上不要并发运行多个热点任务
- 启动前必须确认 `<WORKSPACE>`、`<SKILL_DIR>`、`<DATE>` 已经明确

## 执行分工

### 1. Subagent 负责什么

subagent 只负责运行统一管道并等待归档文件生成，不负责读取归档 Markdown，也不负责翻译和最终用户输出。

执行命令：

```bash
uv run <SKILL_DIR>/scripts/run-pipeline.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --archive <WORKSPACE>/archive/news-hotspots \
  --mode <MODE> \
  --hours <RSS_HOURS> \
  --verbose --force
```

subagent 完成条件：

- 至少存在一个 `<WORKSPACE>/archive/news-hotspots/<DATE>/json/<MODE>*.json`
- 至少存在一个 `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/<MODE>*.md`
- 至少存在一个 `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/*.meta.json`
- JSON 与 Markdown 文件名后缀一致

满足以上条件后，subagent 可以直接返回归档路径并结束。

### 2. 主会话负责什么

主会话只在归档 Markdown 已存在后继续处理，职责如下：

- 读取归档 Markdown
- 把正文完整翻译为 `<LANGUAGE>`
- 保持原 Markdown 结构输出
- 在正文结尾追加 AI 总结段落

## 主会话输出总约束

- 最终输出必须使用 `<LANGUAGE>`，不要保留英文正文作为最终交付
- 输出必须基于归档 Markdown 正文，先做等量翻译，再按原 Markdown 结构输出
- 翻译时只能转换语言，不能做摘要、精选、删减、改写结构或合并条目
- 每个 topic 不能减少
- 每个 topic 下的 item 不能减少
- Markdown 结构、标题层级、列表结构、链接格式不能变
- 每条 item 中，已有来源信息时必须保留 `来源：...`
- 每条 item 中，已有指标信息时必须保留 `指标：...`
- 如果原始归档确实没有来源或指标，可以省略对应字段，但不能伪造数据
- 如果平台单条消息长度受限，可以分段连续输出，但不能省略任何 topic 或 item

## 模式差异

### `daily`

- 必须把归档 Markdown 的全部文字内容完整翻译成 `<LANGUAGE>`
- 除语言转换外，不允许新增、删除或改写正文内容
- 正文之后必须追加标题为 `## 本日报告总结` 的 AI 总结段落
- 该总结必须基于已生成的归档 Markdown 正文内容，概括当天热点归纳、主要主题和信号变化

### `weekly`

- 必须先把归档 Markdown 正文完整翻译成 `<LANGUAGE>`
- 正文部分除语言转换外不能改动
- 正文之后必须追加标题为 `## 本周报告总结` 的 AI 总结段落
- 该总结必须基于已生成的归档 Markdown 正文内容和历史记录，概括本周热点变化、重复主题和来源趋势

## 成功标准

- subagent 成功：完成统一管道并产出匹配的 JSON、Markdown、meta 归档
- 主会话成功：基于归档 Markdown 完成最终输出，且满足上面的全部输出约束

## 失败与恢复

先查看诊断：

```bash
uv run <SKILL_DIR>/scripts/source-health.py \
  --input <WORKSPACE>/archive/news-hotspots/<DATE>/meta \
  --verbose
```

如果只是个别 fetch 步骤失败、超时或缺失结果，但已有部分抓取结果文件存在，可以继续补跑：

```bash
uv run <SKILL_DIR>/scripts/merge-sources.py \
  --archive <WORKSPACE>/archive/news-hotspots \
  --output <WORKSPACE>/.../debug/merge-sources.json \
  [仅传入当前已经存在的 fetch 输出文件]
```

```bash
uv run <SKILL_DIR>/scripts/merge-hotspots.py \
  --input <WORKSPACE>/.../debug/merge-sources.json \
  --archive <WORKSPACE>/archive/news-hotspots \
  --debug <WORKSPACE>/.../debug \
  --mode <MODE>
```

恢复后的处理规则：

- 如果已经拿到完整归档 Markdown，按上面的主会话输出约束完成最终输出
- 如果仍没有完整 Markdown，只能向用户报告实际完成情况和未完成步骤，不能伪造完整热点结果
