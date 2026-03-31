---
alwaysApply: true
---
# News Hotspots 项目维护规则

维护 `news-hotspots` 时，优先遵循这里的规则。  
面向主会话和执行代理的入口与交付规则，以 SKILL.md 和 references/execution-guide.md 为准；本文件只负责项目级维护约束、脚本边界和同步要求。

## 项目定位

### 核心脚本职责

| 脚本 | 职责 | 输入 | 输出 |
|------|------|------|------|
| run-pipeline.py | 统一编排入口 | 配置文件 | pipeline.meta.json |
| merge-sources.py | 输入归一、评分、去重、topic 分组 | fetch 结果 JSON | merged.json |
| merge-hotspots.py | 生成最终热点 JSON / Markdown | merged.json | daily.json, daily.md |
| source-health.py | 读取 meta 做诊断 | meta/*.meta.json | 诊断报告 |

### 核心约束

- **路径表达**：统一使用 `<SKILL_DIR>` 与 `<WORKSPACE>` 表达路径语义，不依赖当前 shell 的相对路径
- **并发限制**：同一台机器上不要并发运行多个热点任务；固定 debug 和归档结构会互相覆盖
- **环境变量**：GitHub 相关环境变量只保留 `GITHUB_TOKEN`；不要重新引入 `GH_APP_ID`、`GH_APP_INSTALL_ID`、`GH_APP_KEY_FILE`

---

## 当前实现基线

### CLI 参数约束

| 脚本 | 当前参数 | 废弃参数（不要使用） |
|------|---------|---------------------|
| `run-pipeline.py` | `--archive`, `--mode`, `--hours` | `--archive-dir`, `--debug-dir`, `--output` |
| `source-health.py` | `--input` | `--input-dir` |

### 归档目录结构

```
<WORKSPACE>/archive/news-hotspots/
└── <DATE>/
    ├── json/      # merged.json, daily.json, weekly.json
    ├── markdown/  # daily.md, weekly.md
    └── meta/      # pipeline.meta.json, fetch-*.meta.json
```

### 推荐主命令

```bash
uv run <SKILL_DIR>/scripts/run-pipeline.py \
  --defaults <SKILL_DIR>/config/defaults \
  --config <WORKSPACE>/config \
  --archive <WORKSPACE>/archive/news-hotspots \
  --mode daily --hours 48 --verbose --force
```

---

## 脚本职责边界

### `run-pipeline.py`

**职责**：编排层 + 诊断层 + 归档层分离

**关键约束**：
- `main()` 只保留高层流程，不要把 step meta 组装、payload 解析、最终产物复制塞回主函数
- 最终热点 JSON 和 Markdown 的命名与归档交给 `merge-hotspots.py`
- `pipeline.meta.json` 必须记录：
  - `hotspots_output`
  - `markdown_output`
  - `cooldown_s`
  - 归档根目录与归档结果

**中断处理**：
外部超时或中断时，应尽量基于已完成步骤继续尝试 merge / hotspots，并写出可诊断的 `pipeline.meta.json`。

### `merge-sources.py`

**职责**：输入归一 → 评分 / 相似性 / 去重 → topic 分组 → 输出组装

**关键约束**：
- 保持阶段化结构，优先在单文件内部收口，不急着拆新模块
- 只要已有部分 fetch 结果文件，就应允许继续生成 `merged.json`，不要强依赖所有 fetch 全量成功

### `merge-hotspots.py`

**职责**：最终热点产物生成器（不只是 JSON 渲染器）

**关键约束**：
- 负责从 `merged.json` 生成最终热点 JSON 和 Markdown
- 负责按 `mode` 统一命名并归档到 `<DATE>/json/` 与 `<DATE>/markdown/`
- `ARCHIVED_JSON=` 与 `ARCHIVED_MARKDOWN=` 是上游读取最终产物路径的稳定契约
- debug 输出只用于调试，不要把 debug JSON 当成最终用户交付物

### `source-health.py`

**职责**：meta 发现 → 诊断归一 → 文本渲染

**报告结构**：
- `History report`：历史健康趋势
- `Run details`：每次运行的详细诊断

**关键约束**：
- 只负责读取 `meta/*.meta.json` 做诊断，不回读抓取结果 JSON
- 如果 `--input` 指向 archive 根目录，应自动聚合最近 7 天 `<DATE>/meta/` 下的元数据

### `test-news-hotspots.sh`

**职责**：统一调度入口

**关键约束**：
- 保持统一调度入口，不要重新扩散成多个测试脚本
- step 脚本映射、输出路径映射、公共参数拼装应集中维护
- 如果脚本参数变化，优先同步这里，再同步 README / SKILL / references

### fetch 脚本

**CLI 一致性**：
```bash
--defaults <path>    # 默认配置目录
--config <path>      # 工作区配置目录
--hours <n>          # 时间窗口
--verbose            # 详细日志
--force              # 强制运行
```

**抓取策略**：
- 同一网站、同一域名默认串行抓取
- 默认优先靠 cooldown 主动降频，避免触发限流
- 新抓取脚本应提供可覆盖的 cooldown 环境变量

---

## 数据与诊断契约

### step 诊断字段

**统一优先使用**：`items`、`call_stats`、`failed_items`

**废弃字段**：不要把 `count` 恢复成 pipeline / meta / 日志的主语义

---

## Topic 与排序规则

### 默认 topic 列表

按优先级排序（高到低）：

1. `github` - GitHub 原生动态
2. `ai-infra` - 芯片、算力、电力、机器人、智能驾驶
3. `ai-frontier` - 模型、agents、AI 应用、AI lab 发布
4. `technology` - 开发工具、工程实践、网络安全、通用科技媒体
5. `business` - 财经媒体、公司经营、资本市场、产业竞争
6. `world` - 国际新闻、战争、外交、地缘政治
7. `science` - 论文、实验室成果、研究博客
8. `social` - 教育、就业、未来工作、媒体传播

### 分类原则

- **先细分类，后大分类**：能准确命中 `ai-frontier`、`ai-infra`、`github` 时，不再回落到一级大分类
- **看长期主轴**：判断 source 的 topic 时看长期主轴，而非偶发内容
- **单 topic 约束**：source 配置统一使用单值 `topic`，每个 source / item 只保留 1 个主 topic

### 评分与去重规则

- `source priority`：轻量基础信号
- fetch 内互动 / 热度：只影响该 fetch 内局部排序
- 历史相似内容：明显降权
- topic 输出阶段：做来源多样性重排

### RSS 默认池规则

- **优先启用**：机构媒体、官方博客、公共机构和行业媒体
- **候选源**：个人博客保留在尾部，用 `"enabled": false`

---

## 诊断与测试入口

```bash
# 配置检查
uv run <SKILL_DIR>/scripts/validate-config.py --defaults <SKILL_DIR>/config/defaults --config <WORKSPACE>/config --verbose

# 运行诊断（当前或最近 7 天）
uv run <SKILL_DIR>/scripts/source-health.py --input <WORKSPACE>/archive/news-hotspots/<DATE>/meta --verbose
uv run <SKILL_DIR>/scripts/source-health.py --input <WORKSPACE>/archive/news-hotspots --verbose

# 统一测试入口
uv run <SKILL_DIR>/scripts/test-news-hotspots.sh full
uv run <SKILL_DIR>/scripts/test-news-hotspots.sh step rss|merge|hotspots
uv run <SKILL_DIR>/scripts/test-news-hotspots.sh health|unit
```

---

## 同步要求

### CLI 参数变更时

同步：SKILL.md、execution-guide.md、automation-template.md、README.md、README_CN.md、test-news-hotspots.sh

### 最终产物路径或命名变更时

同步：merge-hotspots.py 归档逻辑、run-pipeline.py meta 记录、execution-guide.md

### 诊断字段变更时

同步：run-pipeline.py、source-health.py、相关测试 fixture
