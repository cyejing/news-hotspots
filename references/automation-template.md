# 定时任务模板

这个文档是全球科技与 AI 热点自动化任务的占位符模板，只负责定义定时任务提示中的默认参数，不负责解释执行逻辑。

## 使用方式

- 自动化任务提示词只需要读取 `hotspot-prompt.md`
- 自动化任务只负责填入占位符，不复制执行规则
- 具体执行、输出限制、失败处理、恢复流程，都以 `hotspot-prompt.md` 为准

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
- EXTRA_SECTIONS = ## 每周趋势总结
- WORKSPACE = <你的工作区路径>
- SKILL_DIR = <你的 skill 安装路径>
- DATE = YYYY-MM-DD
- LANGUAGE = Chinese
```
