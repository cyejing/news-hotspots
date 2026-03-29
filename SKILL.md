---
name: news-hotspots
description: 用于聚合 RSS、GitHub、Twitter、Reddit、Google News 等来源的全球科技与 AI 新闻内容，涵盖开发者工具、商业市场、宏观政策、国际局势和网络安全，生成每日热点新闻和每周热点新闻。
version: "3.21.2"
homepage: https://github.com/cyejing/news-hotspots
source: https://github.com/cyejing/news-hotspots
metadata:
  openclaw:
    requires:
      bins: ["python3", "uv"]
    optionalBins: ["gh", "bb-browser"]
env:
  - name: GITHUB_TOKEN
    required: false
    description: GitHub token，提高 GitHub API 速率限制
  - name: GH_APP_ID
    required: false
    description: GitHub App ID，用于自动生成安装 token
  - name: GH_APP_INSTALL_ID
    required: false
    description: GitHub App Installation ID，用于自动生成 token
  - name: GH_APP_KEY_FILE
    required: false
    description: GitHub App 私钥 PEM 文件路径
files:
  read:
    - <SKILL_DIR>/config/defaults/: 默认配置
    - <SKILL_DIR>/references/: 热点模板与参考文档
    - <SKILL_DIR>/scripts/: 管道脚本
    - <WORKSPACE>/config/: 工作区覆盖配置
    - <WORKSPACE>/archive/news-hotspots/: 历史热点归档
  write:
    - /tmp/: 临时 debug 目录
    - <WORKSPACE>/archive/news-hotspots/<DATE>/json/: hotspots JSON 归档
    - <WORKSPACE>/archive/news-hotspots/<DATE>/markdown/: Markdown 热点归档
    - <WORKSPACE>/archive/news-hotspots/<DATE>/meta/: 运行诊断元数据归档
---

# News Hotspots

这个 skill 是全球科技与 AI 热点任务的主会话入口契约，负责定义路由、强制约束和最终交付规则；具体执行流程由 `references/hotspot-prompt.md` 负责。

## 强制执行约束

- 固定说明：`run-pipeline.py` 是长耗时任务，通常需要 `15-30` 分钟，必须使用 subagent 运行；如果当前环境没有 subagent，再考虑其他后台代理或长任务执行方式
- 同一台机器上不要并发运行多个热点任务
- 主会话最终输出必须来自归档 Markdown 原文
- 不要让主会话自己重新解析中间抓取结果或重写脚本流程

## 必需路径

- `<WORKSPACE>`
  当前工作区根目录。
- `<SKILL_DIR>`
  当前 skill 仓库根目录。
- `<DATE>`
  日期目录格式固定为 `YYYY-MM-DD`。
- `<WORKSPACE>/archive/news-hotspots/<DATE>/json/`
  最终热点 JSON 归档目录。
- `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/`
  最终 Markdown 归档目录。
- `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/`
  当天运行诊断元数据目录。

## 适用场景

- 用户请求生成今日 / 本周热点
- 用户请求查看最近运行情况或历史失败诊断
- 用户请求创建每日 / 每周热点自动化任务

## 快速路由

- 生成今日 / 本周热点：读取 `references/hotspot-prompt.md`
- 查看当前运行或最近 7 天健康情况：运行 `source-health.py`
- 创建每日 / 每周自动化任务：读取 `references/automation-template.md`

## 主会话职责

1. 识别用户是要生成热点、看诊断，还是创建定时任务。
2. 生成热点时，将执行步骤交给 `references/hotspot-prompt.md`。
3. 生成热点前，先向用户说明并遵守上面的“强制执行约束”。
4. 任务完成后，只读取 `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/` 下的最终归档 Markdown。
5. 回复用户时以归档 Markdown 原文为准，不改写、不压缩、不重新组织。

### 入口约束

- 遵守上面的“强制执行约束”

## 成功条件

满足以下全部条件，任务才算完成：

- 最终热点 JSON 已归档到 `<WORKSPACE>/archive/news-hotspots/<DATE>/json/`
- 最终 Markdown 已归档到 `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/`
- 诊断元数据已归档到 `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/`
- 主会话向用户返回的是完整归档 Markdown 原文，而不是摘要或改写版本

## 生成热点任务

1. 按 `references/hotspot-prompt.md` 中的占位符和流程执行。
2. 执行时先满足上面的“强制执行约束”。
3. 运行完成后，确认最终产物已经归档到 `<WORKSPACE>/archive/news-hotspots/<DATE>/json/` 与 `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/`。
4. 如果执行代理返回的是摘要、节选或压缩版本，主会话必须回到归档目录读取完整 Markdown 原文。

## 失败处理

出现以下任一情况时，不要把当前结果当成成功任务返回给用户：

- `run-pipeline.py` 失败；先查看 `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/` 下对应的 `*.meta.json`
- 用户要的是运行诊断或历史诊断；统一使用 `source-health.py`，不要自己手工拼错误摘要
- 没有拿到完整 Markdown 原文；不要把不完整结果当成最终热点返回给用户

## 用户创建定时任务

1. 用户要求创建每日/每周热点定时任务。
2. 定时任务提示模板使用 `references/automation-template.md`。
3. 自动化提示词只负责填充占位符；具体执行规则仍以 `references/hotspot-prompt.md` 为准。

## 补充入口

当用户需要执行健康检查或查看历史任务执行情况时，使用以下脚本。

当前运行诊断：

```bash
uv run <SKILL_DIR>/scripts/source-health.py --input <WORKSPACE>/archive/news-hotspots/<DATE>/meta --verbose
```

最近 7 天历史诊断：

```bash
uv run <SKILL_DIR>/scripts/source-health.py --input <WORKSPACE>/archive/news-hotspots --verbose
```

配置检查：

```bash
uv run <SKILL_DIR>/scripts/validate-config.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --verbose
```

说明：

- `source-health.py` 只读取 `meta/` 下的诊断元数据，不回读抓取结果 JSON
- `run-pipeline.py` 归档 `meta/*.meta.json`，并记录最终热点产物路径
- 最终热点 JSON 与 Markdown 由 `merge-hotspots.py` 统一命名并归档
- 统一热点产物是后续所有消费流程的唯一上游，不要让主会话绕过它去拼装结果

## 参考文档

- `references/hotspot-prompt.md`
  执行热点任务时使用的统一流程模板
- `references/automation-template.md`
  创建每日 / 每周自动化任务时使用的占位符模板

归档文件：
- `merge-hotspots.py` 生成 `json/hotspots*.json` 与 `markdown/hotspots*.md`
- `run-pipeline.py` 归档 `meta/*.meta.json`
