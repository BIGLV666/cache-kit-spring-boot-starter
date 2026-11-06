# 通用值缓存：@CacheObject / @CacheLoad / CacheStore

面向**任意可 JSON 序列化的值**的键值缓存，与实体缓存（`@CacheEntity` / `@CachedQuery`）平行。
复用同一套 L1（Caffeine）→ L2（Redis）→ loader 三级链、single-flight、null 占位、TTL 抖动、
延迟双删、L2 熔断降级与失效广播（pub/sub / streams / sharded-pubsub）。

## 与实体缓存的区别

| | 实体缓存 | 通用值缓存 |
|---|---|---|
| 值类型 | 实体 | 任意可 JSON 序列化的值（DTO、配置、`List<T>`、`Map<K,V>`） |
| 键 | 实体元数据自动推导（`表名:主键`） | 用户 SpEL 或 `CacheStore` 显式给出 |
| 失效来源 | 写方法 / MP 自动切面 / binlog | 业务调用 `CacheStore.evict` 或 TTL |
| binlog | 支持 | **不支持**（框架不知道任意值与表/行的关系） |
| 适用场景 | 按主键读写的数据库实体 | 远程配置、聚合结果、字典、计算结果等 |

## 快速开始

```java
@CacheObject(prefix = "app-config", ttl = 300)
public class AppConfig {
    private String name;
    private Map<String, Object> values;
    // getters / setters / no-arg constructor
}

@Component
public class AppConfigService {

    @CacheLoad(
        key = "#tenant + ':' + #name",
        condition = "#tenant != null",
        cacheNull = true
    )
    public AppConfig load(String tenant, String name) {
        return remoteConfigClient.get(tenant, name); // 仅缓存未命中时执行
    }
}
```

`@CacheLoad` 方法命中时方法体不执行；未命中时执行方法体，并把返回值写入 L1/L2。
`CacheKit.withDb(...)` 作用域内强制旁路缓存。

## 注解

### @CacheObject（标在值类型上）

| 属性 | 默认 | 说明 |
|---|---|---|
| `prefix` | 类名转蛇形 | 键前缀；不能为空、不能含 `:` 或保留段 `__gk__` |
| `ttl` | -1 | 默认 L2 TTL（秒），≤0 使用全局 `cache-kit.l2.ttl` |
| `cacheNull` | true | 是否缓存 null 结果；方法级 `@CacheLoad.cacheNull=false` 可关闭 |

同一 `prefix` 只能绑定一个值类型，冲突在启动/首次注册时 fail-fast。

值类型必须是可被 Jackson 实例化的具体类。接口/抽象类除非配置
`@com.fasterxml.jackson.annotation.JsonTypeInfo` 多态信息，否则启动/首次注册时 fail-fast——
否则会"写入成功、每次读取都解码失败"，表现为永远 miss。

### @CacheLoad（标在回填方法上）

| 属性 | 默认 | 说明 |
|---|---|---|
| `key` | 必填 | 缓存键 SpEL；可用参数名，如 `"#id"`、`"#tenant + ':' + #name"` |
| `value` | void | 元数据来源类型（`@CacheObject` 类型）；无法从返回类型解析出唯一值类型时必填，也是无法解析的泛型位置的兜底类型 |
| `ttl` | -1 | 本次读取的 L2 TTL 覆盖（秒） |
| `condition` | 空 | SpEL 条件，false 时直查方法体、不走缓存 |
| `cacheNull` | true | 是否缓存 null（需与 `@CacheObject.cacheNull` 同时为 true） |

**key 严格语义**：表达式结果 null/空字符串直接抛异常，绝不猜测 key。返回类型无法解析出
唯一 `@CacheObject` 类型时启动失败，需用 `value = Xxx.class` 指定。

**类型解析（按目标类）**：方法返回类型中的泛型变量按**实际实现类**解析——`T`、`List<T>`、
`Map<K,V>`、数组、`T[]` 都会替换为具体实参（如 `AppConfigService extends Base<AppConfig>`
中的 `T` → `AppConfig`），命中缓存时不会退化成 `LinkedHashMap`。仍无法解析的位置
（顶层 `Object`、未绑定类型变量、无界通配符）使用 `value = Xxx.class` 兜底；
裸 `List`/`Set` 用 `value` 作为元素类型，裸 `Map` / `Map<?, V>` 的键按 `String` 处理
（JSON 对象键的天然类型），显式声明的 `Map<String,Object>` 等嵌套 `Object` 保持不变。

**注解位置**：`@CacheLoad` 可以标在接口方法或泛型父类方法上，切面按目标类回溯查找
（方法 → 具体重写方法 → 实现接口 → 父类），实现类无需重复标注；启动期扫描同样覆盖
接口方法，前缀白名单与 fail-fast 不依赖首次调用。

**异常语义**：方法声明的受检异常原样传播给调用方（single-flight 等待者收到同一异常实例），
异常结果不写入缓存；运行时异常同样原样传播，不会被包装成 `CacheKitException`。

## 生命周期控制：CacheStore

```java
@CacheHandle(AppConfig.class)
private CacheStore<AppConfig> configStore;

configStore.get("t1:db");                     // 只窥探，不回源
configStore.get("t1:db", () -> load(...));    // 未命中回源并回填
configStore.put("t1:db", updated);            // 主动更新
configStore.put("t1:db", updated, ttl);       // 指定本次 TTL
configStore.evict("t1:db");                   // L2 + 本地 L1 + 广播 + 延迟双删
configStore.refresh("t1:db", () -> load(...));// 强制删除后重新回源
```

`CacheStore` 字段也可以是泛型容器，如 `CacheStore<List<AppConfig>>`（字段泛型决定反序列化类型，
`@CacheHandle.value()` 仍指向 `@CacheObject` 元数据类型）。字段声明在泛型父类时同样按具体子类
解析实参（`class Holder<T> { CacheStore<List<T>> store; }` + `Holder<AppConfig>` → `List<AppConfig>`）；
`CacheStore<Object>` 这类顶层 `Object` 用 `@CacheHandle.value()` 兜底。

事务语义与实体缓存一致：`evict` 处于活动事务时按 `cache-kit.tx.evict-after-commit` 延迟到
提交后执行；`refresh` 是显式强制刷新，立即删除后回源。

写入语义：`put(key, value)` 在序列化失败或值超过 `l2.max-value-kb` 时会**失效旧值**再返回，
不会静默保留旧值（否则调用方以为更新成功、读却仍拿到旧值直到 TTL）。`put(key, null)` 在
`cacheNull=false` 或 `l2.null-ttl` 非正（关闭负缓存）时同样按失效处理——包含 L2/L1 删除、
广播、延迟双删与 `evictions` 统计，与 `evict(key)` 一致。

## 键与广播

键格式：

```text
[CacheKeyCustomizer 自定义段:]namespace:__gk__:prefix:用户 key
```

- `__gk__` 是本组件保留段，用于和实体缓存键空间隔离。
- 用户 key 超过 `cache-kit.generic.max-key-length`（默认 512）时做 SHA-256 归一化，
  防止超长 key 撑大 Redis 与日志。
- 失效广播与实体缓存共用同一 topic/stream，消息带保留前缀；订阅端按已注册
  `@CacheObject` 前缀白名单校验后清除本实例 L1，未知前缀直接忽略。
- `pubsub`、`streams`、`sharded-pubsub` 三种模式均可用，无需额外配置。

## 配置

```yaml
cache-kit:
  generic:
    enabled: true       # false：@CacheLoad 直接执行方法体，CacheStore 注入空操作句柄
    max-key-length: 512 # 用户 key 最大长度，超过后 SHA-256 归一化
```

## 边界与注意事项

- **不接 binlog**：任意值可能是聚合/计算结果，框架无法建立到表行的映射。数据变化时请主动
  `evict`/`refresh`，否则最坏等 TTL 过期。
- **MVP 不支持 `evictAll`/按前缀清空**：避免 L2 `SCAN` 的生产风险；需要时可按业务设计
  版本号键。
- **方法自调用不生效**：与 Spring AOP 一致，`this.method()` 不会经过缓存切面。
- **prefix 冲突 fail-fast**：同一 prefix 绑定不同类型会互相反序列化成错数据，启动即拒绝。
- **值必须可 JSON 序列化**：序列化失败不缓存、返回原值；超过 `l2.max-value-kb` 的值两级都不写。
- **null 区分**：`CacheStore.get(key)` 无法区分“未命中”和“缓存了 null”，两者都返回 null；
  需要区分时用 `get(key, loader)` 或业务侧哨兵值。
- **类型演进**：Jackson 容忍未知字段；删除字段/改泛型后旧值可能反序列化失败，按未命中回源覆盖。
  大规模类型迁移可改 `key-namespace` 整体弃用旧键。
- **多租户**：`CacheKeyCustomizer.segment()` 会参与通用缓存键组装；binlog 不适用，但广播端
  不校验自定义段（其他租户的键在本地不存在，删除是无害空操作）。
