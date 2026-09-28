package io.github.biglv666.cachekit.config;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;

/**
 * GraalVM native-image 提示：注册本组件自身被反射/AOP 访问的类型。
 *
 * <p>使用方实体类的 Jackson 序列化反射由宿主应用自行注册（如
 * {@code @RegisterReflectionForBinding(UserEntity.class)}），组件在构建期无法枚举宿主实体。
 * binlog 需 mysql-binlog-connector 的 native 支持数据，详见 README「GraalVM Native」章节。</p>
 */
public class CacheKitRuntimeHints implements RuntimeHintsRegistrar {

    @Override
    public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
        MemberCategory[] categories = {
                MemberCategory.DECLARED_FIELDS,
                MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
                MemberCategory.INVOKE_DECLARED_METHODS
        };
        // @ConfigurationProperties 绑定与注解元数据（元注解读取）
        hints.reflection().registerType(CacheKitProperties.class, categories);
        hints.reflection().registerType(io.github.biglv666.cachekit.annotation.CacheEntity.class, categories);
        hints.reflection().registerType(io.github.biglv666.cachekit.annotation.CacheId.class, categories);
        hints.reflection().registerType(io.github.biglv666.cachekit.annotation.CacheHandle.class, categories);
        hints.reflection().registerType(io.github.biglv666.cachekit.annotation.CacheWarmup.class, categories);
        // 手动句柄与回调接口的代理
        hints.proxies().registerJdkProxy(io.github.biglv666.cachekit.core.EntityCache.class);
        hints.reflection().registerType(io.github.biglv666.cachekit.exception.CacheKitException.class, categories);
    }
}
