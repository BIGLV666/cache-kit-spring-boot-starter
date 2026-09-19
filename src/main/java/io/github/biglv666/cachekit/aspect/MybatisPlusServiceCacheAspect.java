package io.github.biglv666.cachekit.aspect;

import io.github.biglv666.cachekit.core.TieredEntityCache;
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
 * MyBatis-Plus IService 批量写自动失效切面：saveBatch / updateBatchById / saveOrUpdateBatch
 * 在 MP 内部经 SqlSession 批量语句执行（MappedStatement 直调），不经过 mapper 接口代理，
 * {@link MybatisPlusAutoCacheAspect} 拦不到——缺了本切面，批量更新后缓存脏到 L2 TTL。
 *
 * <p>方法成功返回后按实体主键批量失效（evictBatch 内部去重、合并调度延迟双删），
 * 事务感知与 mapper 路径一致（活动事务内延迟到 afterCommit）。
 * 实体类型从 ServiceImpl&lt;M, T&gt; 第二泛型（或直接实现 IService&lt;T&gt; 的第一泛型）解析，
 * 解析不出（自定义 IService 实现）或参数中无实体实例时跳过。
 * 条件写 update(Wrapper)/delete(Wrapper) 不带主键值，不在覆盖范围（binlog/TTL 兜底）。</p>
 *
 * <p>依赖说明：IService/ServiceImpl 位于 mybatis-plus-extension（本 starter 编译期不依赖），
 * 通过反射按名加载，仅在装配层确认存在时才注册本切面；宿主只有 mybatis-plus-core 时静默不装。</p>
 */
@Aspect
public class MybatisPlusServiceCacheAspect {

    private static final Logger log = LoggerFactory.getLogger(MybatisPlusServiceCacheAspect.class);

    private static final String SAVE_BATCH = "saveBatch";
    private static final String UPDATE_BATCH_BY_ID = "updateBatchById";
    private static final String SAVE_OR_UPDATE_BATCH = "saveOrUpdateBatch";
    private static final Set<String> BATCH_WRITE_METHODS =
            Set.of(SAVE_BATCH, UPDATE_BATCH_BY_ID, SAVE_OR_UPDATE_BATCH);

    /** MP extension 类反射加载（装配层已确认存在；null 仅在异常构造场景防御） */
    private static final Class<?> SERVICE_IMPL_CLASS = loadClass(
            "com.baomidou.mybatisplus.extension.service.impl.ServiceImpl");
    private static final Class<?> SERVICE_CLASS = loadClass(
            "com.baomidou.mybatisplus.extension.service.IService");

    private final TieredEntityCache tieredCache;
    private final EntityMetadataRegistry registry;
    private final CacheInvalidateAspect invalidationDelegate;
    private final Map<Class<?>, Optional<Class<?>>> entityClassCache = new ConcurrentHashMap<>();

    public MybatisPlusServiceCacheAspect(TieredEntityCache tieredCache, EntityMetadataRegistry registry,
                                         CacheInvalidateAspect invalidationDelegate) {
        this.tieredCache = tieredCache;
        this.registry = registry;
        this.invalidationDelegate = invalidationDelegate;
    }

    @Around("execution(* com.baomidou.mybatisplus.extension.service.IService+.*(..))")
    public Object around(ProceedingJoinPoint pjp) throws Throwable {
        Method method = ((MethodSignature) pjp.getSignature()).getMethod();
        if (!BATCH_WRITE_METHODS.contains(method.getName())) {
            return pjp.proceed();
        }
        Object result = pjp.proceed();
        try {
            evictAffected(pjp);
        } catch (Exception e) {
            // 失效失败不影响业务结果（数据已写入），残余脏数据由双删/TTL 兜底
            log.warn("IService 批量写缓存失效执行异常: {}", method, e);
        }
        return result;
    }

    /** 收集批量写参数中的实体主键并批量失效；saveBatch 对新插入 id 的失效即清除既有 null 占位 */
    private void evictAffected(ProceedingJoinPoint pjp) {
        Class<?> entityClass = entityClassOf(pjp.getTarget());
        EntityMetadata meta = entityClass == null ? null : registry.find(entityClass);
        if (meta == null) {
            return;
        }
        List<Object> ids = new ArrayList<>();
        for (Object arg : pjp.getArgs()) {
            if (!(arg instanceof Collection<?> coll)) {
                continue;
            }
            for (Object element : coll) {
                if (element != null && meta.entityType().isInstance(element)) {
                    Object id = meta.idOf(element);
                    if (id != null) {
                        ids.add(id);
                    }
                }
            }
        }
        if (!ids.isEmpty()) {
            invalidationDelegate.evictSmartBatch(meta, ids);
        }
    }

    /** 从 ServiceImpl<M, T> 第二泛型（回退 IService<T> 第一泛型）解析实体类，按实现类缓存 */
    private Class<?> entityClassOf(Object target) {
        return entityClassCache
                .computeIfAbsent(target.getClass(), clazz -> {
                    Class<?> entity = null;
                    if (SERVICE_IMPL_CLASS != null) {
                        entity = ResolvableType.forClass(clazz)
                                .as(SERVICE_IMPL_CLASS)
                                .getGeneric(1)
                                .resolve();
                    }
                    if (entity == null && SERVICE_CLASS != null) {
                        entity = ResolvableType.forClass(clazz)
                                .as(SERVICE_CLASS)
                                .getGeneric(0)
                                .resolve();
                    }
                    return Optional.ofNullable(entity);
                })
                .orElse(null);
    }

    private static Class<?> loadClass(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException | LinkageError e) {
            return null;
        }
    }
}
