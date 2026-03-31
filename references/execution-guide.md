# 热点生成执行指南

本文档说明如何执行 `run-pipeline.py` 生成热点新闻，以及 subagent 和主会话的职责分工。

---

## 占位符说明

| 占位符           | 说明                                 |
|---------------|------------------------------------|
| `<SKILL_DIR>` | 当前 skill 仓库根目录                     |
| `<WORKSPACE>` | 当前工作区根目录                           |
| `<DATE>`      | 归档日期，格式 `YYYY-MM-DD`，以脚本实际产出为准     |
| `<MODE>`      | 运行模式，`daily` 或 `weekly`            |
| `<RSS_HOURS>` | RSS 抓取时间窗口，daily 为 48，weekly 为 168 |
| `<LANGUAGE>`  | 用户使用的语言                            |

---

## 执行分工

### Subagent 职责

Subagent 只负责运行脚本并等待归档文件生成，**不负责读取 Markdown 或翻译输出**。

完成条件：

- 存在 `<WORKSPACE>/archive/news-hotspots/<DATE>/json/<mode>*.json`
- 存在 `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/<mode>*.md`
- 存在 `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/*.meta.json`

### 主会话职责

主会话在归档 Markdown 已存在后继续处理：

1. 读取归档 Markdown
2. 按 `<LANGUAGE>` 完整翻译
3. 不删减任何 topic 或 item
4. 结尾追加 AI 总结

---

## 脚本执行

### run-pipeline.py

**长耗时任务（15-30 分钟），必须使用 subagent 执行，超时设置 30 分钟以上。**

约束：

- 同一机器不要并发运行多个热点任务

```bash
uv run <SKILL_DIR>/scripts/run-pipeline.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --archive <WORKSPACE>/archive/news-hotspots \
  --mode <MODE> \
  --hours <RSS_HOURS> \
  --verbose --force
```

| 参数            | daily   | weekly   |
|---------------|---------|----------|
| `<MODE>`      | `daily` | `weekly` |
| `<RSS_HOURS>` | `48`    | `168`    |

---

## 归档路径

脚本执行完成后，产物归档在：

| 类型       | 路径                                                            |
|----------|---------------------------------------------------------------|
| JSON     | `<WORKSPACE>/archive/news-hotspots/<DATE>/json/<mode>.json`   |
| Markdown | `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/<mode>.md` |
| Meta     | `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/*.meta.json`   |

`<DATE>` 以脚本实际产出为准，格式为 `YYYY-MM-DD`。

---

## 主会话输出约束

### 必须遵守

1. **完整翻译**：将归档 Markdown 全文翻译为 `<LANGUAGE>`
2. **不删减**：每个 topic 和 item 都必须保留
3. **保持结构**：Markdown 结构、标题层级、链接格式不变
4. **保留来源**：每条 item 的"来源：..."和"指标：..."必须保留

### 必须追加

| 模式     | 追加内容             |
|--------|------------------|
| daily  | 结尾追加 `## 本日报告总结` |
| weekly | 结尾追加 `## 本周报告总结` |

AI 总结必须基于归档 Markdown 正文内容，概括热点归纳、主要主题和信号变化。

---

## 失败处理

任务失败时，先查看诊断：

```bash
uv run <SKILL_DIR>/scripts/source-health.py \
  --input <WORKSPACE>/archive/news-hotspots/<DATE>/meta \
  --verbose
```

### 判断失败类型

1. **完全失败**：没有产出 `merge-sources.json`
    - 处理：用 subagent 重新运行 `run-pipeline.py`

2. **部分失败**：有 `merge-sources.json` 但没有 markdown
    - 处理：直接运行 `merge-hotspots.py` 补跑
   ```bash
   uv run <SKILL_DIR>/scripts/merge-hotspots.py \
     --input <WORKSPACE>/archive/news-hotspots/<DATE>/json/merge-sources.json \
     --archive <WORKSPACE>/archive/news-hotspots \
     --debug /tmp/news-hotspots/debug \
     --mode <MODE>
   ```

3. **无法恢复**：以上方法都无法产出完整 Markdown
    - 处理：向用户报告实际完成情况，**不能伪造完整热点结果**
