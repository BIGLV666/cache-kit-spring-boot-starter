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
 * 事务感知：方法处于活动事务时失效延迟到 afterCommit（消除"删除在提交前、并发读回填旧值"
 * 的窗口），由 {@code cache-kit.tx.evict-after-commit} 控制（默认开）。</p>
 */
@Aspect
public class CacheInvalidateAspect {

    private static final Logger log = LoggerFactory.getLogger(CacheInvalidateAspect.class);

    private static final boolean SPRING_TX_PRESENT = detectSpringTx();

    private final TieredEntityCache tieredCache;
    private final EntityMetadataRegistry registry;
    private final PrimaryKeyResolver primaryKeyResolver;
    private final boolean evictAfterCommit;

    private final Set<Method> unsupportedWarned = ConcurrentHashMap.newKeySet();

    public CacheInvalidateAspect(TieredEntityCache tieredCache, EntityMetadataRegistry registry,
                                 boolean evictAfterCommit) {
        this.tieredCache = tieredCache;
        this.registry = registry;
        this.primaryKeyResolver = new PrimaryKeyResolver(registry);
        this.evictAfterCommit = evictAfterCommit;
    }

    private static boolean detectSpringTx() {
        try {
            Class.forName("org.springframework.transaction.support.TransactionSynchronizationManager");
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    /** 失效入口：spring-tx 存在且开启配置时走事务感知路径，否则立即失效。供 MP 自动切面复用 */
    public void evictSmart(EntityMetadata meta, Object id) {
        Runnable action = () -> tieredCache.evict(meta, id);
        if (evictAfterCommit && SPRING_TX_PRESENT) {
            TransactionAwareEvictor.evict(action);
        } else {
            action.run();
        }
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
        if (id == null) {
            // 无法证明参数是主键：不猜（对应的读路径同样不会被缓存），打警告跳过
            if (unsupportedWarned.add(method)) {
                log.warn("@CacheInvalidate 无法证明方法参数是主键（参数名与主键字段不匹配且无实体参数），"
                        + "本次失效被跳过: {}", method);
            }
            return;
        }
        evictSmart(meta, id);
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
