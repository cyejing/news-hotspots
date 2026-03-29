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

这个 skill 是全球科技与 AI 热点任务的主会话入口契约，负责定义使用入口、强制约束和任务分流；执行细节以 `references/hotspot-prompt.md` 为准。

## 适用场景

- 用户请求生成今日热点或本周热点
- 用户请求查看当前运行情况或最近历史诊断
- 用户请求创建每日 / 每周热点自动化任务

## 强制执行约束

- `run-pipeline.py` 是长耗时任务，通常需要 `15-30` 分钟
- 必须使用 subagent 运行；如果当前环境没有 subagent，再考虑其他后台代理或长任务执行方式
- subagent 的超时时间必须设置为 `30` 分钟
- 同一台机器上不要并发运行多个热点任务

## 快速路由

- 生成热点任务：读取 `references/hotspot-prompt.md`
- 创建自动化任务：读取 `references/automation-template.md`
- 查看健康诊断：运行 `source-health.py`

## 必需路径

- `<WORKSPACE>`：当前工作区根目录
- `<SKILL_DIR>`：当前 skill 仓库根目录
- `<DATE>`：日期目录，格式固定为 `YYYY-MM-DD`
- `<WORKSPACE>/archive/news-hotspots/<DATE>/json/`：最终热点 JSON 归档目录
- `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/`：最终 Markdown 归档目录
- `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/`：运行诊断元数据目录

## 成功条件

满足以下全部条件，任务才算完成：

- 最终热点 JSON 已归档到 `<WORKSPACE>/archive/news-hotspots/<DATE>/json/`
- 最终 Markdown 已归档到 `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/`
- 诊断元数据已归档到 `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/`

## 失败处理

- 任务失败时，先查看 `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/` 下的 `*.meta.json`
- 需要诊断时，统一使用 `source-health.py`
- 详细执行约束、输出限制和超时恢复规则，统一以 `references/hotspot-prompt.md` 为准
