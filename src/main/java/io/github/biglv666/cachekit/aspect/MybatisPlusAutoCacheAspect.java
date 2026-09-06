package io.github.biglv666.cachekit.aspect;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.github.biglv666.cachekit.core.BatchCacheResolver;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ResolvableType;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MyBatis-Plus BaseMapper 自动缓存切面：内置方法名单内的查询/写入零注解接入。
 *
 * <p>名单：selectById / updateById / deleteById（selectBatchIds 返回列表，属 P2，不缓存）。
 * 仅拦截声明于 BaseMapper 的方法——用户在子接口覆写的同名方法需自行加注解，避免双重处理。
 * 实体类型从 mapper 接口的 BaseMapper&lt;E&gt; 泛型解析。
 */
@Aspect
public class MybatisPlusAutoCacheAspect {

    private static final Logger log = LoggerFactory.getLogger(MybatisPlusAutoCacheAspect.class);

    private static final String SELECT_BY_ID = "selectById";
    private static final String UPDATE_BY_ID = "updateById";
    private static final String DELETE_BY_ID = "deleteById";
    private static final String SELECT_BATCH_IDS = "selectBatchIds";
    private static final Set<String> AUTO_METHODS =
            Set.of(SELECT_BY_ID, UPDATE_BY_ID, DELETE_BY_ID, SELECT_BATCH_IDS);

    private final TieredEntityCache tieredCache;
    private final EntityMetadataRegistry registry;
    private final Map<Class<?>, Optional<Class<?>>> entityClassCache = new ConcurrentHashMap<>();

    public MybatisPlusAutoCacheAspect(TieredEntityCache tieredCache, EntityMetadataRegistry registry) {
        this.tieredCache = tieredCache;
        this.registry = registry;
    }

    @Around("execution(* com.baomidou.mybatisplus.core.mapper.BaseMapper+.*(..))")
    public Object around(ProceedingJoinPoint pjp) throws Throwable {
        Method method = ((MethodSignature) pjp.getSignature()).getMethod();
        if (!AUTO_METHODS.contains(method.getName()) || method.getDeclaringClass() != BaseMapper.class) {
            return pjp.proceed();
        }
        Object[] args = pjp.getArgs();
        Class<?> entityClass = entityClassOf(pjp.getTarget());
        EntityMetadata meta = entityClass == null ? null : registry.find(entityClass);
        if (meta == null || args.length != 1 || args[0] == null) {
            return pjp.proceed();
        }
        switch (method.getName()) {
            case SELECT_BY_ID -> {
                if (isScalar(args[0])) {
                    return tieredCache.load(meta, args[0], null, true, () -> invoke(pjp));
                }
                return pjp.proceed();
            }
            case SELECT_BATCH_IDS -> {
                // per-ID 三级链：命中部分直接用，缺失集合才回源；不存在 ID 缓存 null 占位
                if (args[0] instanceof Collection<?> requested && !requested.isEmpty()) {
                    List<Object> ids = scalarsOf(requested);
                    if (!ids.isEmpty()) {
                        return BatchCacheResolver.resolve(tieredCache, meta, ids, true, null,
                                missing -> invokeList(pjp, new Object[]{missing}));
                    }
                }
                return pjp.proceed();
            }
            case UPDATE_BY_ID, DELETE_BY_ID -> {
                Object result = pjp.proceed();
                Object id = idFromArg(args[0], meta);
                if (id != null) {
                    tieredCache.evict(meta, id);
                }
                return result;
            }
            default -> {
                return pjp.proceed();
            }
        }
    }

    private List<Object> scalarsOf(Collection<?> requested) {
        List<Object> ids = new ArrayList<>();
        for (Object o : requested) {
            if (o == null) {
                continue;
            }
            if (!isScalar(o)) {
                return List.of(); // 含非标量元素，放弃缓存语义
            }
            ids.add(o);
        }
        return ids;
    }

    @SuppressWarnings("unchecked")
    private List<Object> invokeList(ProceedingJoinPoint pjp, Object[] replacedArgs) {
        return (List<Object>) invokeWithArgs(pjp, replacedArgs);
    }

    private Object invokeWithArgs(ProceedingJoinPoint pjp, Object[] replacedArgs) {
        try {
            return pjp.proceed(replacedArgs);
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new CacheKitException(t);
        }
    }

    private Object invoke(ProceedingJoinPoint pjp) {
        try {
            return pjp.proceed();
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new CacheKitException(t);
        }
    }

    private Object idFromArg(Object arg, EntityMetadata meta) {
        if (isScalar(arg)) {
            return arg;
        }
        if (meta.entityType().isInstance(arg)) {
            return meta.idOf(arg);
        }
        return null;
    }

    private boolean isScalar(Object value) {
        return value instanceof String || value instanceof Number;
    }

    /** 从 mapper 对象实现的 BaseMapper&lt;E&gt; 泛型解析实体类，结果按 mapper 类缓存 */
    private Class<?> entityClassOf(Object target) {
        return entityClassCache
                .computeIfAbsent(target.getClass(), clazz -> {
                    for (Class<?> ifc : clazz.getInterfaces()) {
                        if (BaseMapper.class.isAssignableFrom(ifc)) {
                            Class<?> entity = ResolvableType.forClass(ifc)
                                    .as(BaseMapper.class)
                                    .getGeneric(0)
                                    .resolve();
                            if (entity != null) {
                                return Optional.of(entity);
                            }
                        }
                    }
                    return Optional.empty();
                })
                .orElse(null);
    }
}
