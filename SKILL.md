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

这个 skill 用来稳定生成全球科技与 AI 日报 / 周报。主流程是：运行统一管道，仅读取 `/tmp/hotspots.json`，再按提示模板输出最终 Markdown。

## 目录说明

- `<SKILL_DIR>`
  当前 skill 仓库根目录。`scripts/`、`config/`、`references/` 都相对于这个目录。
- `<WORKSPACE>`
  当前用户工作区根目录。工作区配置和归档目录都相对于这个目录。
- `<SKILL_DIR>/config/defaults/`
  默认 source 和 topic 配置目录。
- `<WORKSPACE>/config/`
  工作区覆盖配置目录。若不存在，会自动回退到默认配置。
- `/tmp/hotspots.json`
  本次运行的唯一主输出文件。大模型只读取这个文件。
- `<WORKSPACE>/archive/news-hotspots/<DATE>/json/`
  当天归档的 `hotspots.json`。
- `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/`
  当天 Markdown 热点归档目录。
- `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/`
  当天运行诊断元数据目录。

## 管道产物

- `/tmp/hotspots.json`
  本次运行给大模型消费的唯一输入。
- `<WORKSPACE>/archive/news-hotspots/<DATE>/json/`
  归档后的 `hotspots.json`。
- `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/`
  每个步骤的 `*.meta.json` 和 `pipeline.meta.json`。

step meta 的关键字段：

- `status`
- `items`
- `call_stats`
  包含 `total_calls / ok_calls / failed_calls`
- `failed_items`

健康诊断和历史检查都基于这些 meta 字段，不再回读原始抓取结果 JSON。

## 主流程

唯一推荐入口。不要改写步骤，不要额外解析内部 JSON：

```bash
uv run <SKILL_DIR>/scripts/run-pipeline.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --archive-dir <WORKSPACE>/archive/news-hotspots \
  --hours 48 \
  --output /tmp/hotspots.json \
  --verbose --force
```

- 同一台机器上不要并发运行多个热点任务；`/tmp/hotspots.json` 是固定路径，并发运行会互相覆盖
- `run-pipeline.py` 是明显的长耗时任务。执行前先判断当前环境是否支持 **subagent**
- 如果支持 **subagent**，强烈要求优先并默认使用 **subagent** 运行 `run-pipeline.py`，不要让主对话直接以前台同步方式执行这个命令
- 只有在确认当前环境不支持 **subagent** 时，才允许进一步查看当前环境适合哪种长任务运行方式，再选择替代方案
- 允许足够长的执行时间：
  - 单步骤 timeout 默认可到 `1800s`
  - 整体通常在 `10-30` 分钟内完成
  - 不要因为几分钟内没有新输出就中断或误判失败
- 使用 **subagent** 或替代长任务方案时，保持该任务独占本次热点运行；在它完成前不要再启动第二个 `run-pipeline.py`
- 运行完成后只读取 `/tmp/hotspots.json`

这里的 `<DATE>` 表示本次运行对应的日期目录，格式固定为 `YYYY-MM-DD`，例如 `2026-03-29`。

## 诊断与检查

查看当前运行诊断：

```bash
uv run <SKILL_DIR>/scripts/source-health.py --input-dir <WORKSPACE>/archive/news-hotspots/<DATE>/meta --verbose
```

查看最近 7 天历史诊断：

```bash
uv run <SKILL_DIR>/scripts/source-health.py --input-dir <WORKSPACE>/archive/news-hotspots --verbose
```

校验配置：

```bash
uv run <SKILL_DIR>/scripts/validate-config.py --verbose
```

## 定时任务

当用户要求添加定时任务时：
- 引用 `references/hotspot-prompt.md`
- 不要复制其中的脚本流程
- 使用 `references/automation-template.md` 作为模板

## 参考文档

- `references/hotspot-prompt.md`
  大模型执行热点报告时使用的固定提示模板
- `references/automation-template.md`
  每日 / 每周定时任务模板
