package io.github.biglv666.cachekit.aspect;

import io.github.biglv666.cachekit.annotation.CachedQuery;
import io.github.biglv666.cachekit.core.BatchCacheResolver;
import io.github.biglv666.cachekit.core.BypassContext;
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
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.core.ResolvableType;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@code @CachedQuery} 切面：L1 → L2 → 方法体（查 DB）三级 read-through。
 *
 * <p>返回类型没有缓存元数据（如 List、DTO）时打一次警告后直查 DB，不抛错——
 * MVP 只支持 by-ID 实体查询，列表缓存是 P2。
 */
@Aspect
public class CachedQueryAspect {

    private static final Logger log = LoggerFactory.getLogger(CachedQueryAspect.class);

    private final TieredEntityCache tieredCache;
    private final EntityMetadataRegistry registry;
    private final PrimaryKeyResolver primaryKeyResolver;

    private final SpelExpressionParser spelParser = new SpelExpressionParser();
    private final ParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();
    private final Map<String, Expression> conditionCache = new ConcurrentHashMap<>();
    private final Set<Method> unsupportedWarned = ConcurrentHashMap.newKeySet();

    public CachedQueryAspect(TieredEntityCache tieredCache, EntityMetadataRegistry registry) {
        this.tieredCache = tieredCache;
        this.registry = registry;
        this.primaryKeyResolver = new PrimaryKeyResolver(registry);
    }

    @Around("@annotation(cachedQuery)")
    public Object around(ProceedingJoinPoint pjp, CachedQuery cachedQuery) throws Throwable {
        Method method = ((MethodSignature) pjp.getSignature()).getMethod();
        if (BypassContext.isActive()) {
            return pjp.proceed();
        }
        Duration ttl = cachedQuery.ttl() > 0 ? Duration.ofSeconds(cachedQuery.ttl()) : null;

        // 列表返回：要求 List<实体> 且参数中有唯一 ID 集合（selectBatchIds 语义）
        if (List.class.isAssignableFrom(method.getReturnType())) {
            return resolveList(pjp, cachedQuery, method, ttl);
        }

        EntityMetadata meta = registry.find(method.getReturnType());
        if (meta == null) {
            warnUnsupported(method);
            return pjp.proceed();
        }
        if (!conditionHolds(cachedQuery.condition(), method, pjp.getArgs())) {
            return pjp.proceed();
        }
        Object id = primaryKeyResolver.resolve(method, pjp.getArgs(), meta);
        if (id == null) {
            // 无法证明参数是主键（如条件字段查询）：猜测会造成键空间混淆且失效链路断裂，
            // 旁路直查 DB 并警告
            warnUnsupported(method);
            return pjp.proceed();
        }
        return tieredCache.load(meta, id, ttl, cachedQuery.cacheNull(), () -> invoke(pjp));
    }

    /** List<实体> 返回类型的批量解析：唯一集合参数按 ID 拆分，缺失部分替换参数后回源 */
    private Object resolveList(ProceedingJoinPoint pjp, CachedQuery cachedQuery, Method method, Duration ttl)
            throws Throwable {
        Class<?> entityType = ResolvableType.forMethodReturnType(method).getGeneric(0).resolve();
        EntityMetadata meta = entityType == null ? null : registry.find(entityType);
        if (meta == null) {
            warnUnsupported(method);
            return pjp.proceed();
        }
        Object[] args = pjp.getArgs();
        int collIdx = -1;
        for (int i = 0; i < args.length; i++) {
            if (args[i] instanceof Collection<?>) {
                if (collIdx >= 0) {
                    collIdx = -2;
                    break;
                }
                collIdx = i;
            }
        }
        if (collIdx < 0) {
            warnUnsupported(method);
            return pjp.proceed();
        }
        final int idx = collIdx;
        List<Object> ids = new ArrayList<>();
        for (Object o : (Collection<?>) args[collIdx]) {
            if (o == null) {
                continue;
            }
            if (!(o instanceof String || o instanceof Number)) {
                warnUnsupported(method);
                return pjp.proceed();
            }
            ids.add(o);
        }
        if (!conditionHolds(cachedQuery.condition(), method, args)) {
            return pjp.proceed();
        }
        return BatchCacheResolver.resolve(tieredCache, meta, ids, cachedQuery.cacheNull(), ttl,
                missing -> {
                    Object[] replaced = args.clone();
                    replaced[idx] = missing;
                    return invokeList(pjp, replaced);
                });
    }

    @SuppressWarnings("unchecked")
    private List<Object> invokeList(ProceedingJoinPoint pjp, Object[] replacedArgs) {
        return (List<Object>) invoke(pjp, replacedArgs);
    }

    private Object invoke(ProceedingJoinPoint pjp) {
        return invoke(pjp, null);
    }

    private Object invoke(ProceedingJoinPoint pjp, Object[] replacedArgs) {
        try {
            return replacedArgs == null ? pjp.proceed() : pjp.proceed(replacedArgs);
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new CacheKitException(t);
        }
    }

    private boolean conditionHolds(String condition, Method method, Object[] args) {
        if (condition == null || condition.isBlank()) {
            return true;
        }
        Expression expression = conditionCache.computeIfAbsent(condition, spelParser::parseExpression);
        MethodBasedEvaluationContext context =
                new MethodBasedEvaluationContext(null, method, args, parameterNameDiscoverer);
        Boolean holds = expression.getValue(context, Boolean.class);
        return holds == null || holds;
    }

    private void warnUnsupported(Method method) {
        if (unsupportedWarned.add(method)) {
            log.warn("@CachedQuery 返回类型 {} 没有缓存元数据或不是实体（列表/分页查询暂不支持），该方法将直查 DB: {}",
                    method.getReturnType().getSimpleName(), method);
        }
    }
}
