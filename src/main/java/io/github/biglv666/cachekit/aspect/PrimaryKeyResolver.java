package io.github.biglv666.cachekit.aspect;

import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * 主键推导：从方法参数解析缓存键所需的主键值。
 *
 * <p>推导优先级：① 参数中有实体实例（{@code updateById(User)}）→ 读其主键字段；
 * ② 参数名与主键字段同名（需 {@code -parameters} 编译，Boot 父 POM 默认开启）；
 * ③ 唯一非空标量参数（{@code selectById(Long id)}）；④ 唯一非空对象参数且其类型有缓存元数据。
 * 全部失败抛 {@link CacheKitException}。
 */
public class PrimaryKeyResolver {

    private final EntityMetadataRegistry registry;

    public PrimaryKeyResolver(EntityMetadataRegistry registry) {
        this.registry = registry;
    }

    public Object resolve(Method method, Object[] args, EntityMetadata meta) {
        // ① 实体实例参数
        for (Object arg : args) {
            if (arg != null && meta.entityType().isInstance(arg)) {
                return meta.idOf(arg);
            }
        }
        Parameter[] params = method.getParameters();
        // ② 参数名与主键字段同名
        String idFieldName = meta.idField().getName();
        for (int i = 0; i < params.length && i < args.length; i++) {
            if (args[i] != null && params[i].isNamePresent()
                    && params[i].getName().equals(idFieldName)
                    && isScalar(args[i])) {
                return args[i];
            }
        }
        // ③/④ 唯一非空参数：标量直接用；对象尝试读取其元数据的主键
        Object onlyArg = null;
        int nonNull = 0;
        for (Object arg : args) {
            if (arg != null) {
                nonNull++;
                onlyArg = arg;
            }
        }
        if (nonNull == 1) {
            if (isScalar(onlyArg)) {
                return onlyArg;
            }
            EntityMetadata argMeta = registry.find(onlyArg.getClass());
            if (argMeta != null) {
                return argMeta.idOf(onlyArg);
            }
        }
        throw new CacheKitException("无法从方法参数推导主键: " + method
                + "，参数应包含实体实例、与主键字段同名的参数，或唯一标量主键参数");
    }

    private boolean isScalar(Object value) {
        return value instanceof String || value instanceof Number;
    }
}
