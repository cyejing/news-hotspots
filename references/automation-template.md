# 定时任务模板

这个文档是全球科技与 AI 热点自动化任务的占位符契约，负责定义自动化提示中的默认参数注入规则，不负责重新定义执行流程。

## 使用方式

- 具体执行流程、脚本步骤、输入输出约束，都以 `hotspot-prompt.md` 为准
- 定时任务提示词只需要引用 `hotspot-prompt.md`，不要复制其中的流程说明
- 定时任务最终也必须以归档 Markdown 原文作为返回内容，不要自行压缩或改写
- 定时任务只负责填占位符，不负责重新定义格式、流程或失败处理
- 触发执行时，直接遵守 `hotspot-prompt.md` 中的“强制执行约束”

## 每日热点

```text
阅读 <SKILL_DIR>/references/hotspot-prompt.md，并使用以下占位符执行每日热点任务：

- MODE = daily
- RSS_HOURS = 48
- EXTRA_SECTIONS = (无)
- WORKSPACE = <你的工作区路径>
- SKILL_DIR = <你的 skill 安装路径>
- DATE = YYYY-MM-DD
- LANGUAGE = Chinese
```

## 每周热点

```text
阅读 <SKILL_DIR>/references/hotspot-prompt.md，并使用以下占位符执行每周热点任务：

- MODE = weekly
- RSS_HOURS = 168
- EXTRA_SECTIONS = 📊 每周趋势总结
- WORKSPACE = <你的工作区路径>
- SKILL_DIR = <你的 skill 安装路径>
- DATE = YYYY-MM-DD
- LANGUAGE = Chinese
```
