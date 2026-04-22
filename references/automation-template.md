# 定时任务模板

本文档提供每日/每周热点任务的 cron payload 模板。执行细节见 `execution-guide.md`。

---

## 设计原则

cron payload 的核心要求：**agent 必须在当前 session 内同步完成全流程**（pipeline → 读取 → 翻译 → 输出），不能 spawn 子任务后提前结束。

`<SKILL_DIR>`  替换为 skill 仓库根目录

---

## 每日热点

```text
📰 每日全球科技与AI热点报告

执行步骤：
1. 阅读 {SKILL_DIR}/references/execution-guide.md
2. 按指南同步执行 pipeline（MODE=daily, RSS_HOURS=48, LANGUAGE=Chinese）
3. pipeline 完成后读取归档 Markdown，翻译为中文输出
4. 结尾追加 "## 本日报告总结"

⚠️ 关键：必须在当前 session 同步等待 pipeline 完成（约15-30分钟），然后读取并输出报告。不要 spawn 子任务后提前结束。
```

---

## 每周热点

```text
📰 每周全球科技与AI热点报告

执行步骤：
1. 阅读 {SKILL_DIR}/references/execution-guide.md
2. 按指南同步执行 pipeline（MODE=weekly, RSS_HOURS=168, LANGUAGE=Chinese）
3. pipeline 完成后读取归档 Markdown，翻译为中文输出
4. 结尾追加 "## 本周报告总结"

⚠️ 关键：必须在当前 session 同步等待 pipeline 完成（约15-30分钟），然后读取并输出报告。不要 spawn 子任务后提前结束。
```

---

## Cron 配置建议

| 字段 | 建议值 |
|------|--------|
| sessionTarget | `isolated` |
| delivery.mode | `announce` |
| delivery.channel | `feishu` |
| payload.timeoutSeconds | `3600`（pipeline 可能需要 30 分钟） |
