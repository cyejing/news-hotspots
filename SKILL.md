---
name: news-digest
description: 用于聚合 RSS、GitHub、Twitter、Reddit、Google News 等来源的全球科技与 AI 新闻内容，涵盖开发者工具、商业市场、宏观政策、国际局势和网络安全，生成每日摘要和每周摘要。
version: "3.21.2"
homepage: https://github.com/cyejing/news-digest
source: https://github.com/cyejing/news-digest
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
    - config/defaults/: 默认配置
    - references/: 摘要模板与参考文档
    - scripts/: 管道脚本
    - <workspace>/config/: 工作区覆盖配置
    - <workspace>/archive/news-digest/: 历史摘要归档
  write:
    - /tmp/: 默认 summary JSON 输出与临时 debug 目录
    - <workspace>/archive/news-digest/<DATE>/json/: summary JSON 归档
    - <workspace>/archive/news-digest/<DATE>/markdown/: Markdown 摘要归档
    - <workspace>/archive/news-digest/<DATE>/meta/: 运行诊断元数据归档
---

# News Digest

这个 skill 用来稳定生成全球科技与 AI 日报 / 周报。主流程已经脚本化，大模型只需要运行统一管道、读取 `/tmp/summary.json`，再按提示模板输出最终 Markdown。

## 目录说明

- `config/defaults/`
  skill 内置默认配置目录。`run-pipeline.py --defaults config/defaults` 会从这里读取默认 source 和 topic。
- `workspace/config/`
  工作区覆盖配置目录。若存在，会覆盖默认配置；若不存在，会自动回退到 `config/defaults/`。
- `/tmp/summary.json`
  本次运行的唯一主输出文件。大模型只读取这个文件来写摘要。
- `workspace/archive/news-digest/<DATE>/json/`
  当天归档的 `summary.json` 历史记录。
- `workspace/archive/news-digest/<DATE>/markdown/`
  当天最终用户可读的 Markdown 摘要归档目录。
- `workspace/archive/news-digest/<DATE>/meta/`
  当天运行产生的步骤诊断元数据目录，供健康检查读取。

## 适用场景

- 生成每日摘要
- 生成每周摘要
- 重跑新闻抓取与汇总流程
- 查看当前运行诊断或最近 7 天历史诊断
- 校验配置是否有效

## 主流程

唯一推荐入口：

```bash
uv run scripts/run-pipeline.py \
  --defaults config/defaults \
  --config workspace/config \
  --hours 48 \
  --output /tmp/summary.json \
  --verbose --force
```

- 大模型后续只读取 `/tmp/summary.json`
- 不要直接读取 debug 目录中的内部 JSON 中间文件
- `run-pipeline.py` 耗时较长；如果运行环境支持 **subagent** 后台代理或长任务执行，应优先用它来执行这个脚本，再等待结果返回
- 运行 `run-pipeline.py` 时，不要使用过短的等待时间。应允许与脚本当前配置匹配的长耗时执行：
  - 单步骤 timeout 默认可到 `1800s`
  - 整体运行通常在 `10-30` 分钟内完成
  - 不要因为几分钟内没有新输出就中断或误判失败
- `run-pipeline.py` 会自动：
  - 抓取当前启用的来源
  - 合并、评分、去重、分 topic
  - 输出 `summary.json`
  - 归档 JSON 和诊断元数据到 `workspace/archive/news-digest/<DATE>/`

这里的 `<DATE>` 表示本次运行对应的日期目录，格式固定为 `YYYY-MM-DD`，例如 `2026-03-29`。

## 大模型应如何使用

1. 先阅读 `references/digest-prompt.md`
2. 只执行一次 `run-pipeline.py`
3. 如果平台支持 subagent / 后台代理 / 长任务执行，优先让它执行 `run-pipeline.py`，并等待完成
4. 允许足够长的执行时间，不要提前中断
5. 只读取 `/tmp/summary.json`
6. 按 `digest-prompt.md` 写最终 Markdown 摘要

## 诊断与检查

查看当前运行诊断：

```bash
uv run scripts/source-health.py --input-dir workspace/archive/news-digest/<DATE> --verbose
```

查看最近 7 天历史诊断：

```bash
uv run scripts/source-health.py --input-dir workspace/archive/news-digest --verbose
```

校验配置：

```bash
uv run scripts/validate-config.py --verbose
```

## 定时任务

当用户要求添加定时任务时：
- 定时任务内容应引用 `references/digest-prompt.md`
- 不要把管道步骤复制进定时任务提示词
- 使用 `references/automation-template.md` 作为模板

## 参考文档

- `references/digest-prompt.md`
  大模型执行摘要时使用的固定提示模板
- `references/automation-template.md`
  每日 / 每周定时任务模板
