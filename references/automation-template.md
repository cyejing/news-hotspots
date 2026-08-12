# 定时任务模板

## 设计原则

定时任务触发时，OpenClaw 会启动一个独立 session，**该 session 走与指令相同的统一执行流程**（SKILL.md「统一执行流程」），即：

```
session 启动 → spawn subagent 跑 pipeline
            → subagent 返回报告 → 直接发给用户
```

定时任务的 payload 只需描述这个流程的关键参数，不需要重复描述机制。

## Cron 配置

| 字段 | 建议值 | 说明 |
|------|--------|------|
| `sessionTarget` | `isolated` | 每次定时触发独立 session |
| `delivery.mode` | `announce` | 完成后推送报告到飞书 |
| `payload.timeoutSeconds` | `3600` | 覆盖 pipeline（15-30分钟）+ 输出全流程 |

## Payload 模板

### 每日热点（每天 9:00 触发）

```
📰 每日全球科技与AI热点报告

阅读 <SKILL_DIR>/references/execution-guide.md 并严格按其「输出约束」生成报告。

模式参数：
- MODE=daily
- RSS_HOURS=48
- LANGUAGE=Chinese

执行：
1. spawn subagent 跑 pipeline（参数见 SKILL.md）
2. subagent 返回的报告直接发给用户

约束：subagent 只输出报告正文，不输出任何过程信息（exit code / 抓取条数 / 源耗时）
```

### 每周热点（每周一 9:00 触发）

```
📰 每周全球科技与AI热点报告

阅读 <SKILL_DIR>/references/execution-guide.md 并严格按其「输出约束」生成报告。

模式参数：
- MODE=weekly
- RSS_HOURS=168
- LANGUAGE=Chinese

执行：同上，MODE=weekly，HOURS=168
```

## 示例 cron 条目

```
0 9 * * *  openclaw cron trigger daily-news --sessionTarget=isolated --delivery.mode=announce --payload.timeoutSeconds=3600
0 9 * * 1  openclaw cron trigger weekly-news --sessionTarget=isolated --delivery.mode=announce --payload.timeoutSeconds=3600
```
