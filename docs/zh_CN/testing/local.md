# 本地 Oracle 测试

## 概述

本节介绍如何搭建本地 Oracle 测试环境。

## 使用 Docker 运行 Oracle

使用官方 Oracle Free 镜像（例如 `gvenzl/oracle-free` 提供单容器 Free 版，启动快速）：

```bash
# 运行 Oracle Free 容器
docker run -d \
  --name oracle-test \
  -e ORACLE_PASSWORD=test \
  -e APP_USER=test \
  -e APP_USER_PASSWORD=test \
  -p 1521:1521 \
  gvenzl/oracle-free:23-slim

# 等待 Oracle 就绪
docker exec oracle-test bash -c 'until echo "SELECT 1 FROM dual" | sqlplus system/test@localhost:1521/FREEPDB1; do sleep 2; done'
```

## 使用 Docker Compose

```yaml
# docker-compose.yml
version: '3.8'

services:
  oracle:
    image: gvenzl/oracle-free:23-slim
    environment:
      ORACLE_PASSWORD: test
      APP_USER: test
      APP_USER_PASSWORD: test
    ports:
      - "1521:1521"
    volumes:
      - oracle_data:/opt/oracle/oradata

volumes:
  oracle_data:
```

```bash
docker-compose up -d
```

> **注意**：Oracle 容器比较庞大，首次启动可能需要 1-3 分钟才能就绪。请在 CI 中使用上述 `sqlplus` 健康检查或等待循环，然后再运行测试。

## 服务名与 SID

Oracle Free 数据库通常使用服务名（例如 `FREEPDB1`）。使用带服务名的 `OracleConnectionConfig`：

```python
config = OracleConnectionConfig(
    host='localhost',
    port=1521,
    database='FREEPDB1',   # 服务名
    username='test',
    password='test',
)
```

## 运行测试

```bash
# 设置环境变量
export ORACLE_HOST=localhost
export ORACLE_PORT=1521
export ORACLE_DATABASE=FREEPDB1
export ORACLE_USER=test
export ORACLE_PASSWORD=test

# 运行测试
pytest tests/
```

💡 *AI Prompt:* "Docker 和 Docker Compose 有什么区别？"