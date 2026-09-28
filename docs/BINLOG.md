# binlog 直连失效（0.2.0+，覆盖"绕过应用的写"）

广播失效的前提是写走应用路径。DBA 改库、其他服务写同一张表时，cache-kit 无法感知——
binlog 直连失效模块补上这个盲区：以 MySQL replica 协议直连 binlog（ROW 格式），
任何来源对已缓存实体表的写入都会按行提取主键并触发失效。

**不需要部署 Canal Server**：类路径引入 `mysql-binlog-connector-java`（本 starter 中声明为 optional，
仅启用 binlog 的服务需要自行添加该依赖）+ 开启开关即可。**防呆**：`cache-kit.binlog.enabled=true`
但类路径缺 connector 时启动直接失败并提示补依赖——绝不静默跳过（0.3.1 起）。
MySQL 需开启 `log_bin`、`binlog_format=ROW`、`binlog_row_image=FULL`（MINIMAL 会让行镜像缺主键列，按行失效静默丢失），
且账号具备 `REPLICATION SLAVE` 权限。

```yaml
cache-kit:
  binlog:
    enabled: true        # 缺省关闭
    # host/port/database 缺省从 spring.datasource.url 解析，账号缺省用数据源账号
    # server-id 缺省自动生成随机值；同一 MySQL 上多副本部署建议显式配置
    server-id: 18365     # 同一 MySQL 上必须唯一（与 MySQL server-id 及其他副本不同）
```

注意：MySQL 8.4 移除了 `SHOW MASTER STATUS`，需 connector 0.30.0+（本 starter 默认 0.31.0）。

**断线重连语义**：connector 内部随事件流推进 binlog 位点，断线重连自动从最后位点续传（原生支持，无需配置），
断连窗口内的事件会被自动回放，重复回放的失效是幂等 DEL，无害。若记录位点对应的 binlog 文件在长断连期间
被服务端清理（binlog 过期 / `PURGE BINARY LOGS`），重连会报错秒断并无限循环——监听器检测到
"连上即秒断且未收到任何事件"连续 5 轮后，自动重置为最新位点继续监听并告警（断连窗口内的事件失效丢失由 L2 TTL 兜底）。

实测：直写 → 失效 → 下次读到新值，端到端传播延迟 **avg 10.3ms / p99 14.2ms / max 53.7ms**（300 次直写零漏失效，含 5ms 轮询测量粒度）。

## 测试覆盖

- `BinlogInvalidationIntegrationTest`：真实 MySQL（CI 预起容器）端到端——绕过应用直改库，binlog 必须失效
- `BinlogReconnectIntegrationTest`：Testcontainers 自管 mysql:8.4 + redis:7——断线重连回放、
  位点被 PURGE 后 5 轮重置兜底恢复失效（无 Docker 自动跳过）
- `BinlogLifecycleTest`：位点清理兜底的单元路径

## 常见问题

依次检查：MySQL 是否开启 `log_bin` 且 `binlog_format=ROW`；`binlog_row_image` 是否为 `FULL`
（MINIMAL 时行镜像缺主键列，按行失效会静默丢失）；账号是否有 `REPLICATION SLAVE` 权限；
`server-id` 是否与 MySQL 及其他副本冲突；MySQL 8.4 需要 connector 0.30.0+。
