package io.github.biglv666.cachekit.aspect;

import io.github.biglv666.cachekit.annotation.CachedQuery;
import io.github.biglv666.cachekit.core.BatchCacheResolver;
import io.github.biglv666.cachekit.core.BypassContext;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.exception.IdMisfireException;
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
 * <p>单值返回：主键严格推导（实体实例参数 / 标量参数名=主键字段名），推导不出则警告 + 直查 DB。
 * List&lt;实体&gt; 返回：唯一集合参数按 ID 拆解（selectBatchIds 语义），并开启严格模式守卫——
 * 回源结果主键与请求值对不上（集合参数实为条件值，如手机号列表）时判定误用，
 * 警告 + 直查 DB 不缓存，绝不把条件值当主键回填占位。其余返回类型打一次警告后直查 DB。</p>
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
    private final Set<Method> misfireWarned = ConcurrentHashMap.newKeySet();

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
        try {
            return BatchCacheResolver.resolve(tieredCache, meta, ids, cachedQuery.cacheNull(), ttl,
                    missing -> {
                        Object[] replaced = args.clone();
                        replaced[idx] = missing;
                        return invokeList(pjp, replaced);
                    }, true);
        } catch (IdMisfireException e) {
            // 严格模式守卫：集合参数不是主键集合（回源结果主键与请求值不匹配）。
            // 该路径必然旁路缓存、不写任何占位——与单值路径"猜测式推导被拒绝"同源语义。
            // 统一用原始参数全参重查：回源子集只覆盖本线程赢得 single-flight 的 ID，
            // 并发重叠请求下直接返回子集会静默缺数据（0.3.1 修复），代价仅是一次误用路径的重查
            if (misfireWarned.add(method)) {
                log.warn("@CachedQuery 集合参数不是主键集合（回源结果主键与请求值不匹配），"
                        + "该方法将直查 DB 不缓存，请确认参数确为主键集合: {}", method);
            }
            return invokeList(pjp, args);
        }
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
