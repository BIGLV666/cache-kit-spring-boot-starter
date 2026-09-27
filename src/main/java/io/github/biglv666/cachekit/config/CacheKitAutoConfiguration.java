package io.github.biglv666.cachekit.config;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.biglv666.cachekit.aspect.CacheInvalidateAspect;
import io.github.biglv666.cachekit.aspect.CachedQueryAspect;
import io.github.biglv666.cachekit.aspect.MybatisPlusAutoCacheAspect;
import io.github.biglv666.cachekit.aspect.MybatisPlusServiceCacheAspect;
import io.github.biglv666.cachekit.BinlogLifecycle;
import io.github.biglv666.cachekit.binlog.BinlogInvalidationListener;
import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.channel.RedisChannel;
import io.github.biglv666.cachekit.core.DoubleDeleteScheduler;
import io.github.biglv666.cachekit.core.InvalidationPublisher;
import io.github.biglv666.cachekit.core.InvalidationSubscriber;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.core.RedisInvalidationPublisher;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.handle.CacheHandleBeanPostProcessor;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * cache-kit 自动装配：L1 恒定启用（依赖 Caffeine）；L2、广播、MP 适配按类路径条件启用。
 */
@AutoConfiguration
// 声明在 Redis 自动装配之后：让 @ConditionalOnBean(RedisConnectionFactory) 能看到它的 Bean
//（用 name 形式避免 spring-data-redis 不在类路径时的类加载依赖）
@AutoConfigureAfter(name = "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration")
@ConditionalOnClass(Caffeine.class)
@ConditionalOnProperty(prefix = "cache-kit", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(CacheKitProperties.class)
public class CacheKitAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(CacheKitAutoConfiguration.class);


    @Bean
    @ConditionalOnMissingBean
    public EntityMetadataRegistry entityMetadataRegistry(CacheKitProperties props,
                                                         Environment environment,
                                                         ObjectProvider<EntityMetadataRegistry.DefaultTableNameResolver> tableNameResolver) {
        // 启动期 fail-fast：L1 TTL 必须显著小于 L2 TTL，倒挂会造成"L2 已刷新、L1 永远旧"的顽疾。
        // l2.ttl 非正值 = 禁用 L2 写入（见下方告警分支），此时不存在倒装问题，跳过本校验——
        // 否则任何正值的 l1.ttl 都 >= 非正值，文档承诺的"非正值禁用 L2"配置将无法启动
        if (!props.getL2().getTtl().isZero() && !props.getL2().getTtl().isNegative()
                && !props.getL1().getTtl().isZero() && !props.getL1().getTtl().isNegative()
                && props.getL1().getTtl().compareTo(props.getL2().getTtl()) >= 0) {
            throw new CacheKitException("cache-kit.l1.ttl(" + props.getL1().getTtl()
                    + ") 必须小于 cache-kit.l2.ttl(" + props.getL2().getTtl() + ")："
                    + "L1 TTL 倒挂会导致 L2 刷新后本地仍返回旧值");
        }
        // 非正 L2 TTL = 禁用 L2 写入（组件不提供"永不过期"，脏数据安全模型依赖 TTL 上界）：
        // 历史上注释曾把非正值描述为"不过期语义"，这里显式告警防止误配置后 L2 静默失效
        if (props.getL2().getTtl().isZero() || props.getL2().getTtl().isNegative()) {
            log.warn("cache-kit.l2.ttl={} 为非正值：L2 将跳过全部写入（等效禁用 Redis 二级缓存），"
                    + "读取全部回源 DB；组件不提供'永不过期'语义", props.getL2().getTtl());
        }
        // 键命名空间缺省派生自应用名：多服务共享 Redis 时隔离键空间，防止同名实体互相命中返回错数据
        if (props.getKeyNamespace() == null || props.getKeyNamespace().isBlank()) {
            String appName = environment.getProperty("spring.application.name");
            if (appName != null && !appName.isBlank()) {
                props.setKeyNamespace(appName);
                log.info("cache-kit.key-namespace 未配置，自动使用 spring.application.name='{}' 作为键命名空间", appName);
            } else if (props.isRequireKeyNamespace()) {
                throw new CacheKitException("cache-kit.require-key-namespace=true，但 key-namespace 与 "
                        + "spring.application.name 均为空：多服务/多环境共享 Redis 时同名实体会键冲突"
                        + "（错数据而非未命中），请显式配置 cache-kit.key-namespace");
            } else {
                log.warn("cache-kit.key-namespace 未配置且 spring.application.name 为空："
                        + "多服务共享 Redis 时同名实体会键冲突（错数据而非未命中），建议显式配置；"
                        + "零容忍可配置 cache-kit.require-key-namespace=true 启动失败");
            }
        }
        EntityMetadataRegistry registry = new EntityMetadataRegistry();
        registry.setDefaultTableNameResolver(tableNameResolver.getIfAvailable());
        log.info("cache-kit 启动: L1(maxEntries={}, maxWeightKb={}, ttl={}) L2(ttl={}, jitter={}, nullTtl={}, doubleDeleteDelay={}) broadcast({}, '{}') namespace='{}'",
                props.getL1().getMaxEntries(), props.getL1().getMaxWeightKb(), props.getL1().getTtl(),
                props.getL2().getTtl(), props.getL2().getJitter(), props.getL2().getNullTtl(),
                props.getL2().getDoubleDeleteDelay(),
                props.getBroadcast().isEnabled(), props.getBroadcast().getTopic(),
                props.getKeyNamespace());
        return registry;
    }

    /**
     * L1 通道默认实现（Caffeine）。当前版本 L1 不支持替换为其他本地缓存；
     * L2 通道支持定义自定义 {@code RedisChannel} Bean 覆盖。
     */
    @Bean
    @ConditionalOnMissingBean
    public CaffeineChannel cacheKitL1Channel(CacheKitProperties props) {
        return new CaffeineChannel(props.getL1().getMaxEntries(), props.getL1().getMaxWeightKb());
    }

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    public DoubleDeleteScheduler cacheKitDoubleDeleteScheduler(CacheKitProperties props,
            ObjectProvider<io.github.biglv666.cachekit.core.CacheMetricsListener> metrics) {
        DoubleDeleteScheduler scheduler = new DoubleDeleteScheduler(props.getL2().getDoubleDeleteDelay());
        scheduler.setMetricsListener(metrics.getIfAvailable());
        return scheduler;
    }

    @Bean
    @ConditionalOnMissingBean
    public TieredEntityCache tieredEntityCache(CacheKitProperties props,
                                               EntityMetadataRegistry registry,
                                               CaffeineChannel l1,
                                               ObjectProvider<RedisChannel> l2,
                                               ObjectProvider<InvalidationPublisher> publisher,
                                               DoubleDeleteScheduler doubleDeleteScheduler,
                                               ObjectProvider<io.github.biglv666.cachekit.core.CacheKeyCustomizer> keyCustomizers,
                                               ObjectProvider<io.github.biglv666.cachekit.core.CacheMetricsListener> metrics) {
        TieredEntityCache cache = new TieredEntityCache(props, l1, l2.getIfAvailable(),
                publisher.getIfAvailable(NoopInvalidationPublisher::new), doubleDeleteScheduler,
                props.getKeyNamespace(), keyCustomizers.stream().toList());
        cache.setMetricsListener(metrics.getIfAvailable());
        return cache;
    }

    @Bean
    @ConditionalOnMissingBean
    public CachedQueryAspect cachedQueryAspect(TieredEntityCache tieredEntityCache,
                                               EntityMetadataRegistry registry) {
        return new CachedQueryAspect(tieredEntityCache, registry);
    }

    @Bean
    @ConditionalOnMissingBean
    public CacheInvalidateAspect cacheInvalidateAspect(TieredEntityCache tieredEntityCache,
                                                       EntityMetadataRegistry registry,
                                                       CacheKitProperties props) {
        return new CacheInvalidateAspect(tieredEntityCache, registry, props.getTx().isEvictAfterCommit());
    }

    @Bean
    public static CacheHandleBeanPostProcessor cacheHandleBeanPostProcessor(
            BeanFactory beanFactory,
            ObjectProvider<TieredEntityCache> tieredEntityCache,
            EntityMetadataRegistry registry,
            ObjectProvider<CacheInvalidateAspect> invalidationAspect) {
        return new CacheHandleBeanPostProcessor(beanFactory, tieredEntityCache, registry, invalidationAspect);
    }

    /**
     * Redis 条件装配：L2 通道、失效发布/订阅、监听容器。
     *
     * <p>条件是"类路径有 spring-data-redis 且容器中存在 RedisConnectionFactory Bean"——
     * 只有类没有 Bean（传递引入了 spring-data-redis、排除了 Redis 自动装配、没有客户端实现）
     * 时按设计降级为纯 L1，而不是启动失败。</p>
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(RedisConnectionFactory.class)
    @ConditionalOnBean(RedisConnectionFactory.class)
    static class CacheKitRedisConfiguration {

        @Bean
        @ConditionalOnMissingBean
        public RedisChannel cacheKitL2Channel(ObjectProvider<StringRedisTemplate> templateProvider,
                                              ObjectProvider<RedisConnectionFactory> factoryProvider) {
            StringRedisTemplate template = templateProvider.getIfAvailable();
            RedisConnectionFactory factory = template != null
                    ? template.getConnectionFactory()
                    : factoryProvider.getObject();
            if (template == null) {
                template = new StringRedisTemplate(factory);
                template.afterPropertiesSet();
            }
            warnIfCommandTimeoutLong(factory);
            return new RedisChannel(template);
        }

        /**
         * 命令超时检查：L2 降级以"单次 Redis 调用返回异常"为边界——宿主命令超时过长时
         * （Lettuce 默认 60s），Redis 抖动会先把业务读线程阻塞到超时才降级，
         * 违背"业务读写绝不因 L2 失败而失败"的降级承诺，故超阈值告警给出修正指引。
         */
        private static void warnIfCommandTimeoutLong(RedisConnectionFactory factory) {
            java.time.Duration commandTimeout = resolveCommandTimeout(factory);
            if (commandTimeout != null && commandTimeout.compareTo(java.time.Duration.ofSeconds(5)) > 0) {
                log.warn("Redis 命令超时为 {}（建议 1~5s）：L2 故障降级以单次调用失败为边界，"
                        + "命令超时过长时 Redis 抖动会阻塞业务读线程到超时才降级。"
                        + "可通过 spring.data.redis.timeout 调整",
                        commandTimeout);
            }
        }

        @Bean
        @ConditionalOnMissingBean(InvalidationPublisher.class)
        public InvalidationPublisher cacheKitInvalidationPublisher(CacheKitProperties props,
                                                                   RedisChannel l2Channel) {
            if (!props.getBroadcast().isEnabled()) {
                return new NoopInvalidationPublisher();
            }
            return new RedisInvalidationPublisher(l2Channel.template(), props.getBroadcast().getTopic());
        }

        @Bean
        @ConditionalOnProperty(prefix = "cache-kit.broadcast", name = "enabled",
                havingValue = "true", matchIfMissing = true)
        public RedisMessageListenerContainer cacheKitInvalidationContainer(RedisChannel l2Channel,
                                                                           CaffeineChannel l1Channel,
                                                                           EntityMetadataRegistry registry,
                                                                           CacheKitProperties props,
                                                                           ObjectProvider<io.github.biglv666.cachekit.core.CacheMetricsListener> metricsProvider) {
            RedisMessageListenerContainer container = new RedisMessageListenerContainer();
            container.setConnectionFactory(l2Channel.template().getConnectionFactory());
            InvalidationSubscriber subscriber = new InvalidationSubscriber(l1Channel, registry, props.getKeyNamespace());
            subscriber.setMetricsListener(metricsProvider.getIfAvailable());
            container.addMessageListener(subscriber,
                    new ChannelTopic(props.getBroadcast().getTopic()));
            return container;
        }
    }

    /**
     * 反射读取工厂的命令超时（Lettuce/Jedis 的 {@code getClientConfiguration().getCommandTimeout()}）：
     * lettuce/jedis 是可选依赖，直接引用其类会在缺依赖宿主上 NoClassDefFoundError；
     * 非标准工厂实现或读取失败返回 null（调用方按"无从判断"跳过告警）。
     */
    static java.time.Duration resolveCommandTimeout(RedisConnectionFactory factory) {
        if (factory == null) {
            return null;
        }
        try {
            Object clientConfiguration = factory.getClass().getMethod("getClientConfiguration").invoke(factory);
            if (clientConfiguration == null) {
                return null;
            }
            // setAccessible：客户端配置实现类（如 DefaultLettuceClientConfiguration）是包私有类，
            // 其公共方法直接 invoke 会抛 IllegalAccessException
            java.lang.reflect.Method method =
                    clientConfiguration.getClass().getMethod("getCommandTimeout");
            method.setAccessible(true);
            Object timeout = method.invoke(clientConfiguration);
            return timeout instanceof java.time.Duration d ? d : null;
        } catch (Exception ignored) {
            return null;
        }
    }

    /**
     * Micrometer 指标条件装配：micrometer-core 在类路径时注册 cache-kit.* 计数器。
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(io.micrometer.core.instrument.MeterRegistry.class)
    static class CacheKitMetricsConfiguration {

        @Bean
        @ConditionalOnMissingBean(io.github.biglv666.cachekit.core.CacheMetricsListener.class)
        io.github.biglv666.cachekit.core.CacheMetricsListener cacheKitMetricsListener(ObjectProvider<io.micrometer.core.instrument.MeterRegistry> registryProvider) {
            io.micrometer.core.instrument.MeterRegistry registry = registryProvider.getIfAvailable();
            // micrometer 在类路径但宿主未定义 MeterRegistry（无 actuator）时退回空实现
            return registry == null
                    ? new io.github.biglv666.cachekit.core.CacheMetricsListener() {
                    }
                    : new MicrometerCacheMetrics(registry);
        }
    }

    /**
     * MyBatis-Plus 条件装配：BaseMapper 内置方法自动缓存切面。
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(BaseMapper.class)
    static class CacheKitMpConfiguration {

        @Bean
        @ConditionalOnProperty(prefix = "cache-kit.mp", name = "auto-cache-base-methods",
                havingValue = "true", matchIfMissing = true)
        public MybatisPlusAutoCacheAspect mybatisPlusAutoCacheAspect(TieredEntityCache tieredEntityCache,
                                                                     EntityMetadataRegistry registry,
                                                                     CacheInvalidateAspect invalidationAspect) {
            log.info("cache-kit MP 自动缓存已启用：BaseMapper 六方法（selectById/selectBatchIds/"
                    + "updateById/deleteById/deleteByIds/insert）自动接入；"
                    + "条件写 update(Wrapper)/delete(Wrapper) 不带主键值无法精确失效"
                    + "（binlog 可覆盖，否则 TTL 兜底）");
            return new MybatisPlusAutoCacheAspect(tieredEntityCache, registry, invalidationAspect);
        }

        /**
         * IService 批量写失效切面：saveBatch/updateBatchById/saveOrUpdateBatch 在 MP 内部
         * 经 SqlSession 批量语句执行、绕过 mapper 代理，必须有独立切面才不脏到 L2 TTL。
         * 仅 mybatis-plus-extension（IService 存在）时装配；切面内部反射加载 extension 类。
         */
        @Bean
        @ConditionalOnClass(name = "com.baomidou.mybatisplus.extension.service.IService")
        @ConditionalOnProperty(prefix = "cache-kit.mp", name = "auto-cache-base-methods",
                havingValue = "true", matchIfMissing = true)
        public MybatisPlusServiceCacheAspect mybatisPlusServiceCacheAspect(TieredEntityCache tieredEntityCache,
                                                                           EntityMetadataRegistry registry,
                                                                           CacheInvalidateAspect invalidationAspect) {
            log.info("cache-kit IService 批量写失效已启用：saveBatch/updateBatchById/saveOrUpdateBatch"
                    + "（SqlSession 批量通道绕过 mapper 代理，独立切面覆盖）");
            return new MybatisPlusServiceCacheAspect(tieredEntityCache, registry, invalidationAspect);
        }

        /**
         * MP 全局表名感知的默认表名解析器：实体无 @TableName 时用全局 table-prefix/table-underline
         * 推导物理表名，保证缓存前缀与 binlog 行事件表名一致。SqlSessionFactory 惰性获取
         * （首次实体解析发生在上下文就绪后），解析器 Bean 本身创建零成本。
         */
        @Bean
        public EntityMetadataRegistry.DefaultTableNameResolver cacheKitMpTableNameResolver(
                ObjectProvider<org.apache.ibatis.session.SqlSessionFactory> sqlSessionFactoryProvider) {
            return type -> {
                org.apache.ibatis.session.SqlSessionFactory sqlSessionFactory =
                        sqlSessionFactoryProvider.getIfAvailable();
                if (sqlSessionFactory == null) {
                    return null;
                }
                com.baomidou.mybatisplus.core.config.GlobalConfig globalConfig =
                        com.baomidou.mybatisplus.core.toolkit.GlobalConfigUtils
                                .getGlobalConfig(sqlSessionFactory.getConfiguration());
                if (globalConfig == null || globalConfig.getDbConfig() == null) {
                    return null;
                }
                com.baomidou.mybatisplus.core.config.GlobalConfig.DbConfig dbConfig = globalConfig.getDbConfig();
                String tablePrefix = dbConfig.getTablePrefix() == null ? "" : dbConfig.getTablePrefix();
                String name = dbConfig.isTableUnderline()
                        ? io.github.biglv666.cachekit.support.NamingUtils.camelToSnake(type.getSimpleName())
                        : type.getSimpleName();
                return tablePrefix + name;
            };
        }
    }

    /**
     * binlog 条件装配：类路径有 mysql-binlog-connector 且 cache-kit.binlog.enabled=true 时，
     * 直连 MySQL 订阅行事件，任何来源的写都能触发失效（覆盖 DBA/其他服务改库的盲区）。
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(com.github.shyiko.mysql.binlog.BinaryLogClient.class)
    @ConditionalOnProperty(prefix = "cache-kit.binlog", name = "enabled", havingValue = "true")
    static class CacheKitBinlogConfiguration {

        @Bean
        public com.github.shyiko.mysql.binlog.BinaryLogClient cacheKitBinlogClient(
                CacheKitProperties props,
                ObjectProvider<org.springframework.boot.autoconfigure.jdbc.DataSourceProperties> dsPropsProvider,
                ObjectProvider<javax.sql.DataSource> dataSourceProvider,
                EntityMetadataRegistry registry,
                TieredEntityCache tieredEntityCache,
                ObjectProvider<io.github.biglv666.cachekit.core.CacheKeyCustomizer> keyCustomizers) {
            CacheKitProperties.Binlog binlog = props.getBinlog();
            org.springframework.boot.autoconfigure.jdbc.DataSourceProperties dsProps =
                    dsPropsProvider.getIfAvailable();

            String url = dsProps != null ? dsProps.determineUrl() : null;
            String host = binlog.getHost();
            Integer port = binlog.getPort();
            String database = binlog.getDatabase();
            if (host == null || port == null || database == null) {
                if (url == null) {
                    throw new CacheKitException(
                            "cache-kit.binlog 未配置 host/port/database，且无法从 spring.datasource.url 解析");
                }
                java.util.regex.Matcher m = java.util.regex.Pattern
                        .compile("jdbc:mysql[^:]*://([^:/?]+)(?::(\\d+))?/([^?]+)")
                        .matcher(url);
                if (!m.find()) {
                    throw new CacheKitException("无法从数据源 URL 解析 binlog 连接参数: " + url);
                }
                host = host != null ? host : m.group(1);
                port = port != null ? port : Integer.parseInt(m.group(2) == null ? "3306" : m.group(2));
                database = database != null ? database : m.group(3);
            }
            String username = binlog.getUsername() != null ? binlog.getUsername()
                    : (dsProps != null ? dsProps.determineUsername() : null);
            String password = binlog.getPassword() != null ? binlog.getPassword()
                    : (dsProps != null ? dsProps.determinePassword() : null);

            BinlogInvalidationListener listener = new BinlogInvalidationListener(
                    tieredEntityCache, registry, dataSourceProvider.getIfAvailable(), database);
            com.github.shyiko.mysql.binlog.BinaryLogClient client =
                    new com.github.shyiko.mysql.binlog.BinaryLogClient(host, port, username, password);
            // server-id 缺省自动生成随机值：固定默认值会让同库多副本/多服务互相踢掉复制连接
            client.setServerId(binlog.getServerId() != null
                    ? binlog.getServerId()
                    : java.util.concurrent.ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE));
            client.setKeepAlive(true);
            client.registerEventListener(listener);
            if (keyCustomizers.stream().findAny().isPresent()) {
                log.warn("cache-kit.binlog 与 CacheKeyCustomizer 同时启用存在已知限制：binlog 解析线程"
                        + "无法还原租户键段，租户键的 binlog 失效不会命中（仅 TTL/双删兜底）。"
                        + "多租户场景建议暂不启用 binlog。");
            }
            return client;
        }

        @Bean
        public BinlogLifecycle cacheKitBinlogLifecycle(com.github.shyiko.mysql.binlog.BinaryLogClient client,
                ObjectProvider<io.github.biglv666.cachekit.core.CacheMetricsListener> metrics) {
            BinlogLifecycle lifecycle = new BinlogLifecycle(client, binlogEventListener(client), "cache-kit-binlog");
            lifecycle.setMetricsListener(metrics.getIfAvailable());
            return lifecycle;
        }

        /**
         * 取出已注册到 client 的 {@link BinlogInvalidationListener}：BinlogLifecycle 需要
         * 其事件计数判定"连上即秒断"（位点被服务端清理的兜底）。
         */
        private BinlogInvalidationListener binlogEventListener(com.github.shyiko.mysql.binlog.BinaryLogClient client) {
            return client.getEventListeners().stream()
                    .filter(BinlogInvalidationListener.class::isInstance)
                    .map(BinlogInvalidationListener.class::cast)
                    .findFirst()
                    .orElse(null);
        }
    }
}
