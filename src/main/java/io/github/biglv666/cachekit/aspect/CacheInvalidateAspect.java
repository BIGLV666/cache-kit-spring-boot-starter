package io.github.biglv666.cachekit.aspect;

import io.github.biglv666.cachekit.annotation.CacheInvalidate;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@code @CacheInvalidate} 切面：写方法成功返回后失效缓存（先写 DB 后删缓存），
 * 由 {@link TieredEntityCache#evict} 负责广播与延迟双删。
 *
 * <p>实体类型解析：注解显式指定 &gt; 参数中的实体实例；两者都无法确定时打一次警告并跳过。
 */
@Aspect
public class CacheInvalidateAspect {

    private static final Logger log = LoggerFactory.getLogger(CacheInvalidateAspect.class);

    private final TieredEntityCache tieredCache;
    private final EntityMetadataRegistry registry;
    private final PrimaryKeyResolver primaryKeyResolver;

    private final Set<Method> unsupportedWarned = ConcurrentHashMap.newKeySet();

    public CacheInvalidateAspect(TieredEntityCache tieredCache, EntityMetadataRegistry registry) {
        this.tieredCache = tieredCache;
        this.registry = registry;
        this.primaryKeyResolver = new PrimaryKeyResolver(registry);
    }

    @AfterReturning(pointcut = "@annotation(cacheInvalidate)")
    public void afterReturning(JoinPoint jp, CacheInvalidate cacheInvalidate) {
        Method method = ((MethodSignature) jp.getSignature()).getMethod();
        EntityMetadata meta = resolveMeta(cacheInvalidate, jp.getArgs());
        if (meta == null) {
            if (unsupportedWarned.add(method)) {
                log.warn("@CacheInvalidate 无法确定实体类型（注解未指定 entity 且参数中无实体），缓存未失效: {}", method);
            }
            return;
        }
        Object id = primaryKeyResolver.resolve(method, jp.getArgs(), meta);
        tieredCache.evict(meta, id);
    }

    private EntityMetadata resolveMeta(CacheInvalidate cacheInvalidate, Object[] args) {
        if (cacheInvalidate.entity() != void.class && cacheInvalidate.entity() != Void.class) {
            return registry.find(cacheInvalidate.entity());
        }
        for (Object arg : args) {
            if (arg == null || arg instanceof String || arg instanceof Number) {
                continue;
            }
            EntityMetadata meta = registry.find(arg.getClass());
            if (meta != null) {
                return meta;
            }
        }
        return null;
    }
}
