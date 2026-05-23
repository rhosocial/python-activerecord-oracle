# Oracle 容器故障排查

## Oracle 18c XE 启动失败：Break signaled / ExitCode 255

### 现象
- 容器反复重启，ExitCode 255
- 日志显示在 "uncompressing database data files" 阶段收到 `Break signaled`
- 健康检查超时

### 根因
持久化卷中残留了**不完整的数据库初始化数据**，导致容器每次启动时解压数据文件被中断。

### 解决方法
删除旧容器和对应的数据卷，让容器从全新状态重新初始化：

```bash
docker -H localhost:2375 rm -f rhosocial_oracle_18c
docker -H localhost:2375 volume rm python-activerecord-oracle_oracle_18c_data
```

然后重新创建容器。注意：
- `docker compose up` 在远程 daemon 模式下可能不支持 `-f` 或 `--project-directory`，建议用 `docker run` 手动创建（参考 docker-compose.yml 配置）
- 首次启动需要解压数据文件（约5秒），之后监听器自动启动
- 完整启动约需 2-3 分钟