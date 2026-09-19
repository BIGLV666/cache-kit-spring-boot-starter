package io.github.biglv666.cachekit.aspect;

import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 主键推导（严格模式）：只接受语义上可证明是"这一行主键"的参数，其余返回 null
 * 由调用方旁路（直查 DB）——条件字段值被误当主键会造成缓存键空间混淆且失效链路断裂
 * （如 selectByPhone(String phone) 把手机号当主键回填 user:&lt;phone&gt;，行更新时无法失效）。
 *
 * <p>接受的两条路径：
 * ① 参数中有实体实例（{@code updateById(User)}）→ 直接读其主键字段；
 * ② 标量参数且参数名与主键字段同名（MyBatis {@code @Param} 值或 Java 参数名，
 * 需 {@code -parameters} 编译，Boot 父 POM 默认开启）。</p>
 */
public class PrimaryKeyResolver {

    private static volatile Class<? extends Annotation> mybatisParamClass;

    private final EntityMetadataRegistry registry;

    public PrimaryKeyResolver(EntityMetadataRegistry registry) {
        this.registry = registry;
    }

    /**
     * 从方法参数解析主键值；无法证明是主键时返回 null（调用方应旁路缓存）。
     */
    public Object resolve(Method method, Object[] args, EntityMetadata meta) {
        // ① 实体实例参数：主键就在行对象上
        for (Object arg : args) {
            if (arg != null && meta.entityType().isInstance(arg)) {
                return meta.idOf(arg);
            }
        }
        // ② 标量参数且参数名与主键字段同名
        String idFieldName = meta.idField().getName();
        Parameter[] params = method.getParameters();
        for (int i = 0; i < params.length && i < args.length; i++) {
            if (args[i] == null || !isScalar(args[i])) {
                continue;
            }
            if (paramName(params[i]).equals(idFieldName)) {
                return args[i];
            }
        }
        return null;
    }

    /**
     * 从方法参数收集全部主键值（批量失效用）：实体实例（含集合参数的元素）读其主键字段，
     * 标量参数且参数名与主键字段同名时取值。集合里的标量元素不收——无法证明是主键
     *（如 List&lt;Long&gt; 可能是任意条件值），保持"绝不猜测"的严格语义。
     */
    public List<Object> resolveAll(Method method, Object[] args, EntityMetadata meta) {
        List<Object> ids = new ArrayList<>();
        String idFieldName = meta.idField().getName();
        Parameter[] params = method.getParameters();
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if (arg == null) {
                continue;
            }
            if (meta.entityType().isInstance(arg)) {
                Object id = meta.idOf(arg);
                if (id != null) {
                    ids.add(id);
                }
            } else if (arg instanceof Collection<?> coll) {
                for (Object element : coll) {
                    if (element != null && meta.entityType().isInstance(element)) {
                        Object id = meta.idOf(element);
                        if (id != null) {
                            ids.add(id);
                        }
                    }
                }
            } else if (i < params.length && isScalar(arg) && paramName(params[i]).equals(idFieldName)) {
                ids.add(arg);
            }
        }
        return ids;
    }

    /** 参数名：MyBatis @Param 值优先（反射读取避免硬依赖），其次 Java 参数名 */
    private String paramName(Parameter parameter) {
        Class<? extends Annotation> paramClass = mybatisParamAnnotation();
        if (paramClass != null) {
            Annotation annotation = parameter.getAnnotation(paramClass);
            if (annotation != null) {
                try {
                    Object v = annotation.getClass().getMethod("value").invoke(annotation);
                    if (v instanceof String s && !s.isBlank()) {
                        return s;
                    }
                } catch (Exception ignored) {
                    // 反射读取失败则回退 Java 参数名
                }
            }
        }
        return parameter.getName();
    }

    @SuppressWarnings("unchecked")
    private Class<? extends Annotation> mybatisParamAnnotation() {
        if (mybatisParamClass == null) {
            synchronized (PrimaryKeyResolver.class) {
                if (mybatisParamClass == null) {
                    try {
                        mybatisParamClass = (Class<? extends Annotation>)
                                Class.forName("org.apache.ibatis.annotations.Param");
                    } catch (ClassNotFoundException | LinkageError e) {
                        // 非 MyBatis 宿主：仅靠 Java 参数名
                    }
                }
            }
        }
        return mybatisParamClass;
    }

    private boolean isScalar(Object value) {
        return value instanceof String || value instanceof Number;
    }
}
