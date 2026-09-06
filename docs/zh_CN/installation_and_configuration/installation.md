# 安装指南

## 前提条件

- Python 3.8+（包括 3.13t/3.14t 自由线程构建）
- Oracle 数据库 12c+（推荐：18c+）
- Oracle Instant Client（厚模式）或网络访问（thin 模式）

## 安装

### 基本安装

```bash
pip install rhosocial-activerecord-oracle
```

这将安装：
- `rhosocial-activerecord`（核心库）
- `oracledb`（Oracle 数据库驱动）

### 开发安装

```bash
pip install rhosocial-activerecord-oracle[dev]
```

### 包含测试依赖

```bash
pip install rhosocial-activerecord-oracle[test]
```

## 驱动模式

`oracledb` 驱动支持两种模式：

### Thin 模式（无需 Oracle Client）

默认模式。无需安装 Oracle Instant Client。通过 TCP/IP 直接连接到 Oracle 数据库。

```python
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig

config = OracleConnectionConfig(
    host="localhost",
    port=1521,
    database="ORCLPDB1",
    username="system",
    password="password"
)
```

### 厚模式（需要 Oracle Client）

需要安装 Oracle Instant Client。提供额外功能，如 Oracle 调用接口（OCI）和对某些 Oracle 特定协议的支持。

```python
config = OracleConnectionConfig(
    host="localhost",
    port=1521,
    database="ORCLPDB1",
    username="system",
    password="password",
    mode="thick"  # 使用厚模式
)
```

## 验证安装

```python
import oracledb
print(f"oracledb 版本: {oracledb.__version__}")

from rhosocial.activerecord.backend.impl.oracle import OracleBackend
print("Oracle 后端导入成功")
```

## 另请参阅

- [连接配置](configuration.md) — 连接参数
- [SSL/TLS 配置](ssl.md) — 安全连接
- [连接管理](pool.md) — 连接池
