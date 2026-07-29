# 定时任务模板

本文档提供每日/每周热点任务的 cron payload 模板。执行细节见 `execution-guide.md`。

---

## 设计原则

cron payload 必须确保 agent 在**当前 session 同步完成全流程**（运行 → 等待 → 读取 → 输出），不能 spawn 子任务后提前结束。

---

## 每日热点

```text
📰 每日全球科技与AI热点报告

阅读 {SKILL_DIR}/references/execution-guide.md，按指南执行：
- MODE=daily, RSS_HOURS=48, LANGUAGE=Chinese
- 同步执行 pipeline（timeout ≥ 1800秒）
- 读取归档 Markdown 后，只输出"抓取概要句 + AI 浓缩总结"，不逐条翻译标题

⚠️ 必须在当前 session 同步完成全流程，不要 spawn 后提前结束。
```

---

## 每周热点

```text
📰 每周全球科技与AI热点报告

阅读 {SKILL_DIR}/references/execution-guide.md，按指南执行：
- MODE=weekly, RSS_HOURS=168, LANGUAGE=Chinese
- 同步执行 pipeline（timeout ≥ 1800秒）
- 读取归档 Markdown 后，只输出"抓取概要句 + AI 浓缩总结"，不逐条翻译标题

⚠️ 必须在当前 session 同步完成全流程，不要 spawn 后提前结束。
```

---

## Cron 配置

| 字段 | 建议值 |
|------|--------|
| sessionTarget | `isolated` |
| delivery.mode | `announce` |
| payload.timeoutSeconds | `3600` |
