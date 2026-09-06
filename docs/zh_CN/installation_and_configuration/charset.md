# 字符集/编码

## 概述

Oracle 支持多种字符编码。默认编码为 UTF-8。

## 配置

```python
config = OracleConnectionConfig(
    host="localhost",
    port=1521,
    database="ORCLPDB1",
    username="system",
    password="password",
    encoding="UTF-8",       # 数据库字符集
    nencoding="UTF-16"      # 国家字符集（可选）
)
```

## 常用编码

| 编码 | 描述 |
|------|------|
| `UTF-8` | 通用字符集（推荐） |
| `AL32UTF8` | Oracle 的 UTF-8 实现 |
| `WE8ISO8859P1` | 西欧 |
| `JA16EUC` | 日文 EUC |

## 最佳实践

- 对于新数据库，始终使用 `UTF-8` 或 `AL32UTF8`
- 确保客户端编码与数据库编码匹配
- 国家字符集（`nencoding`）是可选的，仅在使用 `NCHAR`/`NVARCHAR2` 列时需要

## 另请参阅

- [连接配置](configuration.md) — 连接参数
