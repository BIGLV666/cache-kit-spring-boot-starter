# 开发与构建

## 构建

```bash
mvn test          # 本地无 Redis/带 binlog 的 MySQL(3307) 时对应集成测试自动跳过
mvn verify deploy -Prelease   # 发布（打 v* 标签由 CI 触发）
```

## 本地依赖容器

```bash
# binlog 集成测试需要带 binlog 的 MySQL（3307）
docker run -d --name cache-kit-mysql -p 3307:3306 \
  -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=cachekit_test \
  mysql:8.4 --log-bin=mysql-bin --binlog-format=ROW --server-id=1

# 上下文/广播/benchmark 测试需要本地 Redis（6379）
docker run -d --name cache-kit-redis -p 6379:6379 redis:7
```

Testcontainers 用例（`BinlogReconnectIntegrationTest`、`InvalidationBroadcastMultiInstanceIntegrationTest`、
`CacheBenchmarkIntegrationTest`）自管容器，只要 Docker 可用即自动运行，否则跳过。

## 发布流程

1. 确认 `pom.xml` 版本号与 `docs/CHANGELOG.md` 最新段一致
2. 提交并打标签：`git tag v0.3.1 && git push origin v0.3.1`
3. GitHub Actions `publish.yml` 触发：测试 → GPG 签名 → 上传 Sonatype Central
   （需要仓库 secrets：`GPG_PRIVATE_KEY`、`GPG_PASSPHRASE`、`CENTRAL_USERNAME`、`CENTRAL_PASSWORD`，
   以及 Sonatype Central Portal 上完成 `io.github.biglv666` namespace 校验）

## 测试布局

| 范围 | 测试类 |
|---|---|
| 三级链/失效/指标单元 | `TieredEntityCacheTest`、`CacheMetricsTest` 等 |
| 装配与配置校验 | `CacheKitAutoConfigurationValidationTest`、`RedisCommandTimeoutCheckTest`、`DataSourcePropertiesReflectionTest` |
| binlog 端到端 | `BinlogInvalidationIntegrationTest`（CI 预起 MySQL）、`BinlogReconnectIntegrationTest`（Testcontainers） |
| 多实例广播 | `InvalidationBroadcastMultiInstanceIntegrationTest`（streams 3 实例 / pubsub 2 实例） |
| 对比 benchmark | `CacheBenchmarkIntegrationTest` |
| 预热/手动 API | `CacheWarmupRunnerTest`、`DelegatingEntityCacheBatchTest` |
