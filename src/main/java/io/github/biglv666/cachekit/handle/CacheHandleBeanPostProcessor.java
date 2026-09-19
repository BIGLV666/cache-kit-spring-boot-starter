package io.github.biglv666.cachekit.handle;

import io.github.biglv666.cachekit.annotation.CacheHandle;
import io.github.biglv666.cachekit.aspect.CacheInvalidateAspect;
import io.github.biglv666.cachekit.core.EntityCache;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;

/**
 * {@code @CacheHandle} 注入处理器：为标注字段创建绑定指定实体的 {@link EntityCache} 句柄。
 *
 * <p>在 postProcessBeforeInitialization 阶段注入（此时 bean 尚未被 AOP 代理包装），
 * 三级链通过 ObjectProvider 惰性获取，避免 BeanPostProcessor 引发过早初始化。
 * cache-kit 被关闭（enabled=false）时由 {@link CacheKitDisabledConfiguration} 注册本处理器
 * 的降级形态：注入空操作句柄而非留空字段，避免业务首次调用 NPE。</p>
 */
public class CacheHandleBeanPostProcessor implements BeanPostProcessor {

    private final BeanFactory beanFactory;
    private final ObjectProvider<TieredEntityCache> cacheProvider;
    private final EntityMetadataRegistry registry;
    private final ObjectProvider<CacheInvalidateAspect> invalidationAspectProvider;
    private final boolean disabled;

    public CacheHandleBeanPostProcessor(BeanFactory beanFactory,
                                        ObjectProvider<TieredEntityCache> cacheProvider,
                                        EntityMetadataRegistry registry,
                                        ObjectProvider<CacheInvalidateAspect> invalidationAspectProvider) {
        this.beanFactory = beanFactory;
        this.cacheProvider = cacheProvider;
        this.registry = registry;
        this.invalidationAspectProvider = invalidationAspectProvider;
        this.disabled = false;
    }

    /** 关闭态构造：注入空操作句柄（读直查 DB、失效空操作），依赖均不可用 */
    public CacheHandleBeanPostProcessor(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
        this.cacheProvider = null;
        this.registry = null;
        this.invalidationAspectProvider = null;
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
            if (!EntityCache.class.isAssignableFrom(field.getType())) {
                throw new CacheKitException("@CacheHandle 字段类型必须是 EntityCache: "
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
}
