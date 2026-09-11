package io.github.biglv666.cachekit.config;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.biglv666.cachekit.aspect.CacheInvalidateAspect;
import io.github.biglv666.cachekit.aspect.CachedQueryAspect;
import io.github.biglv666.cachekit.aspect.MybatisPlusAutoCacheAspect;
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
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * cache-kit 自动装配：L1 恒定启用（依赖 Caffeine）；L2、广播、MP 适配按类路径条件启用。
 */
@AutoConfiguration
@ConditionalOnClass(Caffeine.class)
@ConditionalOnProperty(prefix = "cache-kit", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(CacheKitProperties.class)
public class CacheKitAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(CacheKitAutoConfiguration.class);


    @Bean
    @ConditionalOnMissingBean
    public EntityMetadataRegistry entityMetadataRegistry(CacheKitProperties props) {
        // 启动期 fail-fast：L1 TTL 必须显著小于 L2 TTL，倒挂会造成"L2 已刷新、L1 永远旧"的顽疾
        if (!props.getL1().getTtl().isZero() && !props.getL1().getTtl().isNegative()
                && props.getL1().getTtl().compareTo(props.getL2().getTtl()) >= 0) {
            throw new CacheKitException("cache-kit.l1.ttl(" + props.getL1().getTtl()
                    + ") 必须小于 cache-kit.l2.ttl(" + props.getL2().getTtl() + ")："
                    + "L1 TTL 倒挂会导致 L2 刷新后本地仍返回旧值");
        }
        log.info("cache-kit 启动: L1(maxEntries={}, maxWeightKb={}, ttl={}) L2(ttl={}, jitter={}, nullTtl={}, doubleDeleteDelay={}) broadcast({}, '{}') namespace='{}'",
                props.getL1().getMaxEntries(), props.getL1().getMaxWeightKb(), props.getL1().getTtl(),
                props.getL2().getTtl(), props.getL2().getJitter(), props.getL2().getNullTtl(),
                props.getL2().getDoubleDeleteDelay(),
                props.getBroadcast().isEnabled(), props.getBroadcast().getTopic(),
                props.getKeyNamespace());
        return new EntityMetadataRegistry();
    }

    @Bean
    @ConditionalOnMissingBean
    public CaffeineChannel cacheKitL1Channel(CacheKitProperties props) {
        return new CaffeineChannel(props.getL1().getMaxEntries(), props.getL1().getMaxWeightKb());
    }

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    public DoubleDeleteScheduler cacheKitDoubleDeleteScheduler(CacheKitProperties props) {
        return new DoubleDeleteScheduler(props.getL2().getDoubleDeleteDelay());
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
            EntityMetadataRegistry registry) {
        return new CacheHandleBeanPostProcessor(beanFactory, tieredEntityCache, registry);
    }

    /**
     * Redis 条件装配：L2 通道、失效发布/订阅、监听容器。
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(RedisConnectionFactory.class)
    static class CacheKitRedisConfiguration {

        @Bean
        @ConditionalOnMissingBean
        public RedisChannel cacheKitL2Channel(ObjectProvider<StringRedisTemplate> templateProvider,
                                              ObjectProvider<RedisConnectionFactory> factoryProvider) {
            StringRedisTemplate template = templateProvider.getIfAvailable();
            if (template == null) {
                template = new StringRedisTemplate(factoryProvider.getObject());
                template.afterPropertiesSet();
            }
            return new RedisChannel(template);
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
            return new MybatisPlusAutoCacheAspect(tieredEntityCache, registry, invalidationAspect);
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
                TieredEntityCache tieredEntityCache) {
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
            client.setServerId(binlog.getServerId());
            client.setKeepAlive(true);
            client.registerEventListener(listener);
            return client;
        }

        @Bean
        public BinlogLifecycle cacheKitBinlogLifecycle(com.github.shyiko.mysql.binlog.BinaryLogClient client) {
            return new BinlogLifecycle(client, "cache-kit-binlog");
        }
    }
}
