# 定时任务模板

本文档提供每日/每周热点任务的 cron payload 模板。执行细节与占位符说明见 `execution-guide.md`。

---

## 设计原则

cron payload 必须确保 agent 在**当前 session 同步完成全流程**（启动 subagent → 等待完成 → 读取归档 → 输出），不能 spawn 后提前结束当前 session。

pipeline 是 15–30 分钟长任务，由 subagent 后台执行，主会话等通知后读归档输出——既不阻塞 session，又保证报告必然交付。

---

## 每日热点

```text
📰 每日全球科技与AI热点报告

阅读 <SKILL_DIR>/references/execution-guide.md 并严格按其「执行分工」与「输出约束」生成报告（不要自行定义输出格式）。

参数：MODE=daily，RSS_HOURS=48，LANGUAGE=Chinese

约束：
1. 当前 session 同步完成全流程，不要 spawn 后提前结束——subagent 跑完 pipeline 后主会话必须读取归档并输出，不能提前结束。
2. subagent 跑完后只回极简完成句（`✅ pipeline 完成，归档：<markdown 绝对路径>`），不报 exit code、不报抓取文章数、不报各源耗时与源成败——过程信息留在 meta/ 诊断里，需要时用指令 4 健康诊断查看。主会话从完成句取路径读归档，输出抓取概要 + 分主题结构化总结 + 热点总结，不输出抓取过程报告。
```

---

## 每周热点

```text
📰 每周全球科技与AI热点报告

阅读 <SKILL_DIR>/references/execution-guide.md 并严格按其「执行分工」与「输出约束」生成报告（不要自行定义输出格式）。

参数：MODE=weekly，RSS_HOURS=168，LANGUAGE=Chinese

约束：
1. 当前 session 同步完成全流程，不要 spawn 后提前结束——subagent 跑完 pipeline 后主会话必须读取归档并输出，不能提前结束。
2. subagent 跑完后只回极简完成句（`✅ pipeline 完成，归档：<markdown 绝对路径>`），不报 exit code、不报抓取文章数、不报各源耗时与源成败——过程信息留在 meta/ 诊断里，需要时用指令 4 健康诊断查看。主会话从完成句取路径读归档，输出抓取概要 + 分主题结构化总结 + 热点总结，不输出抓取过程报告。
```

---

## Cron 配置

| 字段 | 建议值 | 说明 |
|------|--------|------|
| sessionTarget | `isolated` | 每次定时触发独立 session |
| delivery.mode | `announce` | 完成后推送报告 |
| payload.timeoutSeconds | `3600` | session 级超时，覆盖 15–30 分钟 pipeline + 输出 |
