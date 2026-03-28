# 摘要提示模板

使用前替换 `<...>` 占位符。严格按下面流程执行，不要自行改写脚本步骤。

## 占位符

| 占位符 | daily | weekly |
|--------|-------|--------|
| `<MODE>` | `daily` | `weekly` |
| `<RSS_HOURS>` | `48` | `168` |
| `<EXTRA_SECTIONS>` | *(无)* | `📊 每周趋势总结` |
| `<WORKSPACE>` | 工作区路径 | |
| `<SKILL_DIR>` | skill 安装路径 | |
| `<DATE>` | YYYY-MM-DD | |
| `<LANGUAGE>` | `Chinese` | |

生成 **<DATE>** 的 <MODE> 全球科技与 AI 摘要。使用 `<DATE>` 作为报告日期，不要推断。

## 执行步骤

1. 运行统一管道：
   ```bash
   uv run <SKILL_DIR>/scripts/run-pipeline.py \
     --defaults <SKILL_DIR>/config/defaults \
     --config <WORKSPACE>/config \
     --hours <RSS_HOURS> \
     --output /tmp/summary.json \
     --verbose --force
   ```
2. 只读取：
   - `/tmp/summary.json`
3. 根据 `summary.json` 写 Markdown 摘要
4. 将 Markdown 保存到 `<WORKSPACE>/archive/news-digest/<DATE>/markdown/`

## 写作规则

- 只根据 `summary.json` 选材，不要自己重新打分或重排
- 使用 `summary.json` 中的 topic 顺序和 item 排序
- Markdown 按 topic 分类输出
- 每条至少包含：emoji、评分、标题、链接、来源
- 若 `metrics` 中有 likes/comments/replies/retweets/score，则一并展示
- 优先使用活跃感较强的 emoji，例如 `🔥` `🚀` `🧠` `⚠️` `💬`
- 分数可四舍五入到 1 位小数
- `<EXTRA_SECTIONS>` 只有在 weekly 时输出
- 使用 `<LANGUAGE>` 撰写全文

## 输出与归档

- 最终只输出 Markdown 摘要
- 将 Markdown 保存到 `<WORKSPACE>/archive/news-digest/<DATE>/markdown/<MODE>.md`
- 如果同名文件已存在，改为 `<MODE>1.md`、`<MODE>2.md`

## 禁止事项

- 不要直接读取运行目录中的内部 JSON 中间文件
- 不要编写临时 Python 去解析内部 JSON
- 不要复制或改写脚本流程
