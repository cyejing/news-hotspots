# Agents

维护 `news-hotspots` 时，优先遵循这里的规则；用户向说明和大模型主流程以 [SKILL.md](/Users/chenyejing/project/news-hotspots/SKILL.md) 与 [hotspot-prompt.md](/Users/chenyejing/project/news-hotspots/references/hotspot-prompt.md) 为准。

## 项目心智

- 主入口是 [run-pipeline.py](/Users/chenyejing/project/news-hotspots/scripts/run-pipeline.py)
- 最终给大模型的唯一推荐输入是 `/tmp/hotspots.json`
- 合并、评分、去重、topic 分组都在 [merge-sources.py](/Users/chenyejing/project/news-hotspots/scripts/merge-sources.py)
- 精简热点 JSON 由 [merge-hotspots.py](/Users/chenyejing/project/news-hotspots/scripts/merge-hotspots.py) 生成
- 当前运行诊断看 `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/` 中的 `*.meta.json`
- 最近 7 天历史诊断看 `<WORKSPACE>/archive/news-hotspots/` 下最近 7 天的 `<DATE>/meta/`
- 文档和脚本中的路径语义统一使用 `<SKILL_DIR>` 与 `<WORKSPACE>`，不要依赖当前 shell 的相对路径
- `run-pipeline.py` 是长耗时脚本；如果运行环境支持 subagent、后台代理或长任务执行，应优先用这种方式运行，并允许和脚本默认 timeout 匹配的执行时间
- 同一台机器上不要并发运行多个热点任务；`/tmp/hotspots.json` 是固定路径，并发运行会互相覆盖

工作区覆盖顺序：

1. `<WORKSPACE>/config/news-hotspots-sources.json`
2. `<WORKSPACE>/config/news-hotspots-topics.json`
3. `<SKILL_DIR>/config/defaults/`

归档目录：

- `<WORKSPACE>/archive/news-hotspots/<DATE>/json/`
- `<WORKSPACE>/archive/news-hotspots/<DATE>/markdown/`
- `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/`

推荐主命令：

```bash
uv run <SKILL_DIR>/scripts/run-pipeline.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --archive-dir <WORKSPACE>/archive/news-hotspots \
  --hours 48 \
  --output /tmp/hotspots.json \
  --verbose --force
```

## 重构约束

后续继续维护时，优先保持这几条结构约束，不要再回退到“脚本能跑但职责混杂”的状态：

- `run-pipeline.py`
  - 保持“编排层 + 诊断层 + 归档层”分离
  - `main()` 只保留高层流程，不要重新把 step meta 组装、archive 复制、payload 解析塞回主函数
  - step 诊断统一通过：
    - `items`
    - `call_stats`
    - `failed_items`
  - 不要重新把 `count` 恢复成 pipeline / meta / 日志的主语义
- `source-health.py`
  - 保持“三层结构”：
    - meta 发现
    - 诊断归一
    - 文本渲染
  - 只读取 meta，不再回读原始抓取结果 JSON
  - 报告固定为：
    - `History report`
    - `Run details`
- `test-news-hotspots.sh`
  - 保持统一调度入口，不要重新扩散成多个测试脚本
  - step 脚本映射、输出路径映射、公共参数拼装应集中维护
- `merge-sources.py`
  - 保持“输入归一 → 评分/相似性/去重 → topic 分组 → 输出组装”的阶段化结构
  - 优先做单文件内部收口，不急着拆新模块
- fetch 脚本
  - 优先保持 CLI 一致：
    - `--defaults`
    - `--config`
    - `--output`
    - `--hours`
    - `--verbose`
    - `--force`
  - 顶层输出优先补统一字段：
    - `calls_total`
    - `calls_ok`
    - `calls_kind`
    - `items_total`
  - 单条结果优先补统一字段：
    - `status`
    - `items`
    - `count`
  - 旧字段可以保留做兼容，但新增逻辑优先消费统一语义字段

可以删除的旧逻辑原则：

- 删除已经没有业务价值的 backward compatibility 分支
- 删除只为旧 `count` 语义服务的展示或 fallback
- 删除空壳 helper 和只剩兼容含义的注释
- 如果 step meta 已有完整失败诊断，不要再新增第二套错误表达

## Topic 规则

默认 topic：

- `ai-models`
- `ai-agents`
- `ai-ecosystem`
- `technology`
- `developer-tools`
- `github`
- `markets-business`
- `macro-policy`
- `world-affairs`
- `cybersecurity`

维护时重点遵守：

- `ai-models / ai-agents / ai-ecosystem` 命中时，不再同时挂 `technology`
- `github` 是独立 topic；GitHub Releases 和 GitHub Trending 统一只产出 `github`
- `macro-policy` 与 `markets-business` 不重复挂载
- `world-affairs` 独立，不再使用泛化 `news`
- 每个 source 默认只挂 1 个 topic；必要时最多 2 个；尽量不要 3 个以上

RSS 默认池规则：

- 机构媒体、官方博客、公共机构和行业媒体优先默认启用
- 明显个人博客保留在 `sources.rss` 尾部作为候选源
- 候选源统一用 `"enabled": false`

## 评分与去重

- `source priority` 只是轻量基础信号
- fetch 内互动/热度只影响该 fetch 内局部排序
- 跨 `source_type` 热点有有限加分
- 历史相似内容会明显降权
- topic 输出阶段会做来源多样性重排

如果改评分、去重或 topic 行为，优先同步检查：

- [merge-sources.py](/Users/chenyejing/project/news-hotspots/scripts/merge-sources.py)
- [merge-hotspots.py](/Users/chenyejing/project/news-hotspots/scripts/merge-hotspots.py)
- [references/hotspot-prompt.md](/Users/chenyejing/project/news-hotspots/references/hotspot-prompt.md)

## 限流规则

- 同一网站、同一域名默认串行抓取
- 默认优先靠 cooldown 主动降频，避免触发限流
- 新抓取脚本应提供可覆盖的 cooldown 环境变量
- `run-pipeline.py` 应把 `cooldown_s` 写入 `pipeline.meta.json`
- 新 fetch 步骤无论成功、返回非 0、超时或结果文件缺失，都应保证输出对应的 `*.meta.json`
- step meta 的失败诊断统一使用 `failed_items`；不要再维护单独的 `error_messages` 作为主失败来源
- 除非确认目标站点稳定支持更高频率，否则不要加并发

## Fetch 输出协议

fetch 脚本顶层结果应尽量统一包含：

- `source_type`
- `calls_total`
- `calls_ok`
- `calls_kind`
- `items_total`

单个 source / topic / repo 结果应尽量统一包含：

- `status`
- `items`
- `count`

当前为了兼容 merge 和现有 fixture，旧字段仍允许保留，例如：

- `sources_total`
- `sources_ok`
- `total_articles`
- `total_posts`
- `subreddits`
- `topics`
- `repos`

但新增逻辑应优先消费统一语义字段：

- `items_total`
- `calls_total`
- `calls_ok`
- `failed_items`

## 诊断入口

配置检查：

```bash
uv run <SKILL_DIR>/scripts/validate-config.py --defaults <SKILL_DIR>/config/defaults --config <WORKSPACE>/config --verbose
```

当前运行诊断：

```bash
uv run <SKILL_DIR>/scripts/source-health.py --input-dir <WORKSPACE>/archive/news-hotspots/<DATE>/meta --verbose
```

最近 7 天历史诊断：

```bash
uv run <SKILL_DIR>/scripts/source-health.py --input-dir <WORKSPACE>/archive/news-hotspots --verbose
```

说明：

- `run-pipeline.py` 会在 debug 目录下写每个步骤的 `.meta.json`
- `run-pipeline.py` 也会把 `hotspots.json` 和 `meta/` 归档到 `<WORKSPACE>/archive/news-hotspots/<DATE>/`
- `source-health.py` 只读取 meta，不再回读原始结果 JSON
- step meta 的核心字段是：
  - `status`
  - `items`
  - `call_stats`
  - `failed_items`
- 如果 `--input-dir` 指向 archive 根目录，`source-health.py` 会自动聚合最近 7 天 `<DATE>/meta/` 下的元数据
- `source-health.py` 的报告结构固定为：
  - `History report`：步骤级汇总，不展开详细错误
  - `Run details`：按每次运行分组，详细显示 `failed_items`

## 测试入口

统一使用 [test-news-hotspots.sh](/Users/chenyejing/project/news-hotspots/scripts/test-news-hotspots.sh)。测试输出固定在 `/tmp/news-hotspots/`。

```bash
uv run <SKILL_DIR>/scripts/test-news-hotspots.sh full
uv run <SKILL_DIR>/scripts/test-news-hotspots.sh step rss
uv run <SKILL_DIR>/scripts/test-news-hotspots.sh step merge
uv run <SKILL_DIR>/scripts/test-news-hotspots.sh step hotspots
uv run <SKILL_DIR>/scripts/test-news-hotspots.sh health
uv run <SKILL_DIR>/scripts/test-news-hotspots.sh unit
```

补充：

- `full` 会产出 `/tmp/news-hotspots/hotspots.json`，并把步骤诊断元数据归档到 `<WORKSPACE>/archive/news-hotspots/<DATE>/meta/`
- 单步骤测试复用 `/tmp/news-hotspots/` 下固定文件名
- 历史去重默认读取 `<WORKSPACE>/archive/news-hotspots/` 下所有日期目录中的 `json/`
