---
name: news-hotspots
description: 用于聚合 RSS、GitHub、Twitter、Reddit、Google News 等来源的全球科技与 AI 新闻内容，涵盖开发者工具、商业市场、宏观政策、国际局势和网络安全，生成每日热点和每周热点。
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
    - /tmp/: 默认 hotspots JSON 输出与临时 debug 目录
    - <WORKSPACE>/archive/news-hotspots/<DATE>/json/: hotspots JSON 归档
    - <WORKSPACE>/archive/news-hotspots/<DATE>/markdown/: Markdown 热点归档
    - <WORKSPACE>/archive/news-hotspots/<DATE>/meta/: 运行诊断元数据归档
---

# News Hotspots

这个 skill 用来稳定生成全球科技与 AI 日报 / 周报。标准流程只有两步：运行统一管道，然后只读取 `/tmp/hotspots.json` 生成最终 Markdown。

必需路径：

- `/tmp/hotspots.json`
  唯一主输出文件。大模型只读取这个文件。
- `<WORKSPACE>/archive/news-hotspots/<DATE>/json/`
  当天归档的 `hotspots.json`。
- `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/`
  当天 Markdown 热点归档目录。
- `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/`
  当天运行诊断元数据目录。
- `<DATE>`
  日期目录格式固定为 `YYYY-MM-DD`。

## 主流程

唯一推荐入口：

```bash
uv run <SKILL_DIR>/scripts/run-pipeline.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --archive-dir <WORKSPACE>/archive/news-hotspots \
  --hours 48 \
  --output /tmp/hotspots.json \
  --verbose --force
```

- 不要改写脚本步骤，不要额外解析内部 JSON
- 同一台机器上不要并发运行多个热点任务；`/tmp/hotspots.json` 是固定路径
- `run-pipeline.py` 是长耗时任务；如果环境支持 **subagent**，必须使用 **subagent** 执行
- 保持该任务独占本次热点运行；完成前不要启动第二个 `run-pipeline.py`
- 运行完成后只读取 `/tmp/hotspots.json`

## Subagent 结果处理

- 不要只信任 subagent 的总结
- 如果 subagent 返回的是摘要、节选或压缩总结，主会话必须读取 `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/` 下的最终归档文件
- 主会话回复用户时，以最终归档 Markdown 原文为准
- 只有确认 subagent 返回的已经是完整 Markdown 原文时，才可以直接转交

## 补充入口

诊断：

```bash
uv run <SKILL_DIR>/scripts/source-health.py --input-dir <WORKSPACE>/archive/news-hotspots/<DATE>/meta --verbose
```

```bash
uv run <SKILL_DIR>/scripts/source-health.py --input-dir <WORKSPACE>/archive/news-hotspots --verbose
```

配置检查：

```bash
uv run <SKILL_DIR>/scripts/validate-config.py --verbose
```

## 参考文档

- `references/hotspot-prompt.md`
  大模型执行热点报告时使用的固定提示模板
- `references/automation-template.md`
  每日 / 每周定时任务模板
