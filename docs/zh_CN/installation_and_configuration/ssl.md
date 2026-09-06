# SSL/TLS 配置

## 概述

Oracle 支持 TLS 加密进行客户端-服务器连接。`oracledb` 驱动通过连接参数提供 SSL/TLS 配置。

## 基本 SSL 配置

```python
config = OracleConnectionConfig(
    host="secure-db.example.com",
    port=2484,
    database="ORCLPDB1",
    username="system",
    password="password",
    # SSL 参数通过 conn_param 传递
)
```

## SSL 连接参数

| 参数 | 描述 |
|------|------|
| `ssl_server_cert_dn` | 服务器证书的可分辨名称 |
| `ssl_allow_weak_dn` | 允许弱可分辨名称匹配 |
| `wallet_location` | Oracle 钱包目录路径 |
| `wallet_password` | Oracle 钱包密码 |

## 另请参阅

- [连接配置](configuration.md) — 所有连接参数
