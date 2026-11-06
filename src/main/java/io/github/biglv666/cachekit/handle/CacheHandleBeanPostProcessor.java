package io.github.biglv666.cachekit.handle;

import com.fasterxml.jackson.databind.JavaType;
import io.github.biglv666.cachekit.annotation.CacheHandle;
import io.github.biglv666.cachekit.aspect.CacheInvalidateAspect;
import io.github.biglv666.cachekit.config.CacheKitDisabledConfiguration;
import io.github.biglv666.cachekit.core.CacheStore;
import io.github.biglv666.cachekit.core.EntityCache;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.generic.DefaultCacheStore;
import io.github.biglv666.cachekit.generic.GenericCacheMetadata;
import io.github.biglv666.cachekit.generic.GenericCacheRegistry;
import io.github.biglv666.cachekit.generic.GenericTypeResolver;
import io.github.biglv666.cachekit.generic.TieredValueCache;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;

/**
 * {@code @CacheHandle} 注入处理器：按字段类型注入缓存句柄。
 *
 * <ul>
 *   <li>{@link EntityCache}：实体缓存句柄，{@link CacheHandle#value()} 为实体类型；</li>
 *   <li>{@link CacheStore}：通用值缓存句柄，{@link CacheHandle#value()} 为标注
 *       {@code @CacheObject} 的值类型；字段可声明为 {@code CacheStore<List<T>>} 等泛型容器，
 *       反序列化类型按字段声明与目标类解析。</li>
 * </ul>
 *
 * <p>在 postProcessBeforeInitialization 阶段注入（此时 bean 尚未被 AOP 代理包装），
 * 三级链通过 ObjectProvider 惰性获取，避免 BeanPostProcessor 引发过早初始化。
 * cache-kit 被关闭（enabled=false）时由 {@link CacheKitDisabledConfiguration} 注册本处理器
 * 的降级形态；通用缓存关闭（generic.enabled=false）时 registry 不存在，同样降级：
 * 注入空操作句柄（实体为 {@code NoopEntityCache}、通用为 {@link NoopCacheStore}）
 * 而非留空字段，避免业务首次调用 NPE。</p>
 */
public class CacheHandleBeanPostProcessor implements BeanPostProcessor {

    private final BeanFactory beanFactory;
    private final ObjectProvider<TieredEntityCache> cacheProvider;
    private final EntityMetadataRegistry registry;
    private final ObjectProvider<CacheInvalidateAspect> invalidationAspectProvider;
    private final ObjectProvider<TieredValueCache> valueCacheProvider;
    private final ObjectProvider<GenericCacheRegistry> genericRegistryProvider;
    private final boolean evictAfterCommit;
    private final boolean disabled;

    public CacheHandleBeanPostProcessor(BeanFactory beanFactory,
                                        ObjectProvider<TieredEntityCache> cacheProvider,
                                        EntityMetadataRegistry registry,
                                        ObjectProvider<CacheInvalidateAspect> invalidationAspectProvider) {
        this(beanFactory, cacheProvider, registry, invalidationAspectProvider, null, null, true);
    }

    public CacheHandleBeanPostProcessor(BeanFactory beanFactory,
                                        ObjectProvider<TieredEntityCache> cacheProvider,
                                        EntityMetadataRegistry registry,
                                        ObjectProvider<CacheInvalidateAspect> invalidationAspectProvider,
                                        ObjectProvider<TieredValueCache> valueCacheProvider,
                                        ObjectProvider<GenericCacheRegistry> genericRegistryProvider,
                                        boolean evictAfterCommit) {
        this.beanFactory = beanFactory;
        this.cacheProvider = cacheProvider;
        this.registry = registry;
        this.invalidationAspectProvider = invalidationAspectProvider;
        this.valueCacheProvider = valueCacheProvider;
        this.genericRegistryProvider = genericRegistryProvider;
        this.evictAfterCommit = evictAfterCommit;
        this.disabled = false;
    }

    /** 关闭态构造：注入空操作句柄（读直查 DB、失效空操作），依赖均不可用 */
    public CacheHandleBeanPostProcessor(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
        this.cacheProvider = null;
        this.registry = null;
        this.invalidationAspectProvider = null;
        this.valueCacheProvider = null;
        this.genericRegistryProvider = null;
        this.evictAfterCommit = true;
        this.disabled = true;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        ReflectionUtils.doWithFields(targetClass, field -> {
            CacheHandle anno = field.getAnnotation(CacheHandle.class);
            if (anno == null) {
                return;
            }
            if (CacheStore.class.isAssignableFrom(field.getType())) {
                injectCacheStore(bean, targetClass, field, anno);
                return;
            }
            if (!EntityCache.class.isAssignableFrom(field.getType())) {
                throw new CacheKitException("@CacheHandle 字段类型必须是 EntityCache 或 CacheStore: "
                        + targetClass.getName() + "." + field.getName());
            }
            ReflectionUtils.makeAccessible(field);
            if (disabled) {
                field.set(bean, new NoopEntityCache<>());
                return;
            }
            TieredEntityCache cache = cacheProvider.getObject();
            CacheInvalidateAspect invalidationAspect = invalidationAspectProvider.getIfAvailable();
            field.set(bean, new DelegatingEntityCache<>(anno.value(), registry, cache, invalidationAspect));
        });
        return bean;
    }

    private void injectCacheStore(Object bean, Class<?> targetClass, Field field, CacheHandle anno)
            throws IllegalAccessException {
        ReflectionUtils.makeAccessible(field);
        if (disabled || valueCacheProvider == null || genericRegistryProvider == null) {
            field.set(bean, new NoopCacheStore<>());
            return;
        }
        TieredValueCache valueCache = valueCacheProvider.getIfAvailable();
        GenericCacheRegistry genericRegistry = genericRegistryProvider.getIfAvailable();
        if (valueCache == null || genericRegistry == null) {
            // 通用缓存关闭（cache-kit.generic.enabled=false）：注入空操作句柄，避免业务 NPE
            field.set(bean, new NoopCacheStore<>());
            return;
        }
        GenericCacheMetadata metadata = genericRegistry.require(anno.value());
        JavaType valueType = GenericTypeResolver.fieldType(field, targetClass, metadata.valueType());
        field.set(bean, new DefaultCacheStore<>(metadata, valueType, valueCache, evictAfterCommit));
    }
}
