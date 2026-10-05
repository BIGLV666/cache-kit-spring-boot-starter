package io.github.biglv666.cachekit.aspect;

import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * 主键推导（严格模式）：只接受语义上可证明是"这一行主键"的参数，其余返回 null
 * 由调用方旁路（直查 DB）——条件字段值被误当主键会造成缓存键空间混淆且失效链路断裂
 * （如 selectByPhone(String phone) 把手机号当主键回填 user:&lt;phone&gt;，行更新时无法失效）。
 *
 * <p>接受的两条路径：
 * ① 参数中有实体实例（{@code updateById(User)}）→ 直接读其主键字段（复合主键按声明序 join）；
 * ② 标量参数且参数名与主键字段同名（MyBatis {@code @Param} 值或 Java 参数名，
 * 需 {@code -parameters} 编译，Boot 父 POM 默认开启）——复合主键要求每个主键字段
 * 都能匹配到同名标量参数，缺一即旁路（部分主键的键是错键，绝不猜测）。</p>
 */
public class PrimaryKeyResolver {

    private static final Logger log = LoggerFactory.getLogger(PrimaryKeyResolver.class);

    private static volatile Class<? extends Annotation> mybatisParamClass;

    private final EntityMetadataRegistry registry;

    public PrimaryKeyResolver(EntityMetadataRegistry registry) {
        this.registry = registry;
    }

    /**
     * 从方法参数解析主键值；无法证明是主键时返回 null（调用方应旁路缓存）。
     */
    public Object resolve(Method method, Object[] args, EntityMetadata meta) {
        // ① 实体实例参数：主键就在行对象上（复合主键由 idOf 按声明序 join）
        for (Object arg : args) {
            if (arg != null && meta.entityType().isInstance(arg)) {
                return meta.idOf(arg);
            }
        }
        // ② 标量参数且参数名与主键字段同名；复合主键按声明序全匹配才成立
        List<Field> idFields = meta.idFields();
        Parameter[] params = method.getParameters();
        if (idFields.size() == 1) {
            String idFieldName = idFields.get(0).getName();
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
        Object[] values = new Object[idFields.size()];
        for (int f = 0; f < idFields.size(); f++) {
            String fieldName = idFields.get(f).getName();
            Object matched = null;
            for (int i = 0; i < params.length && i < args.length; i++) {
                if (args[i] != null && isScalar(args[i]) && paramName(params[i]).equals(fieldName)) {
                    matched = args[i];
                    break;
                }
            }
            if (matched == null) {
                // 复合主键任一字段无同名参数：无法证明是完整主键，旁路直查 DB
                return null;
            }
            values[f] = matched;
        }
        try {
            return EntityMetadata.joinSegments(Arrays.asList(values));
        } catch (CacheKitException e) {
            // 段含 ':' 的复合值不可能已被缓存（load 同样拒绝），旁路直查 DB 并警告
            log.warn("复合主键值无法成键，该方法将直查 DB: {}", method, e);
            return null;
        }
    }

    /**
     * 从方法参数收集全部主键值（批量失效用）：实体实例（含集合参数的元素）读其主键字段
     * （复合主键为 join 串），标量参数且参数名与主键字段同名时取值（仅单主键——复合主键
     * 的标量参数携带不了完整联合键）。集合里的标量元素不收——无法证明是主键
     *（如 List&lt;Long&gt; 可能是任意条件值），保持"绝不猜测"的严格语义。
     */
    public List<Object> resolveAll(Method method, Object[] args, EntityMetadata meta) {
        List<Object> ids = new ArrayList<>();
        List<Field> idFields = meta.idFields();
        boolean composite = idFields.size() > 1;
        String idFieldName = composite ? null : idFields.get(0).getName();
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
            } else if (!composite && i < params.length && isScalar(arg)
                    && paramName(params[i]).equals(idFieldName)) {
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
