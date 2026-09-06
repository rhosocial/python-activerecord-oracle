# 支持的版本

## Oracle 数据库版本支持

| Oracle 版本 | 支持状态 | 说明 |
|----------------|----------------|-------|
| 11g | ❌ 已停止支持 | Oracle 不再提供支持 |
| 12c / 12.1 / 12.2 | ⚠️ 扩展支持 | 某些功能需要 12c+（如 FETCH FIRST） |
| 18c | ✅ 支持 | 推荐基线 |
| 19c | ✅ 支持 | 最新的长期支持（LTS）版本 |
| 21c | ✅ 支持 | 添加原生 JSON 类型支持 |
| 23ai | ✅ 支持 | 添加 AI VECTOR 类型、JSON 关系二元性 |

> **重要**：本后端专为 Oracle 数据库设计。方言行为与 Oracle 特定功能（PL/SQL、序列、DUAL、ROWNUM/FETCH FIRST）紧密耦合。**请勿将本后端用于其他 Oracle 兼容的数据库。**

⚠️ **注意**：

- Oracle 11g 已停止支持，不建议使用
- 12c+ 才支持 ANSI `FETCH FIRST` / `OFFSET` 分页；早期版本使用 `ROWNUM`
- 某些功能（原生 JSON、VECTOR）仅在某些版本中可用；请参阅功能文档

## Python 版本要求

| Python 版本 | 支持状态 | 说明 |
|---------------|----------------|-------|
| 3.8 | ✅ 支持 | |
| 3.9 | ✅ 支持 | |
| 3.10 | ✅ 支持 | |
| 3.11 | ✅ 支持 | |
| 3.12 | ✅ 支持 | |
| 3.13 | ✅ 支持 | 支持 free-threaded 构建（3.13t） |
| 3.14 | ✅ 支持 | 支持 free-threaded 构建（3.14t） |

**Free-Threaded Python**：从 Python 3.13 开始，可以使用无 GIL 的 free-threaded 构建，如 `python3.13t`、`python3.14t` 等。本后端与 free-threaded Python 兼容，不过某些线程相关功能可能表现不同。

## 依赖要求

| 依赖 | 版本 | 说明 |
|-----------|---------|-------|
| rhosocial-activerecord | >=1.0.0 | 核心库 |
| oracledb | >=3.4.2 | Oracle 驱动（thin 或 thick 模式） |

⚠️ **重要**：本后端仅支持 `oracledb` 驱动。异步后端使用 `oracledb` **thin 模式**（无需安装 Oracle Instant Client）；**thick 模式**需要 Oracle Instant Client，并提供额外的 OCI 功能。

## 连接模式

| 模式 | 需要 Oracle Client | 使用场景 |
|------|------------------------|----------|
| Thin（默认） | 否 | TCP/IP 直接连接，包含异步支持 |
| Thick | 是 | OCI 功能、某些 Oracle 特定协议 |

💡 *AI Prompt:* "在 oracledb 驱动中，Oracle 的 thin 模式与 thick 模式有何不同？"