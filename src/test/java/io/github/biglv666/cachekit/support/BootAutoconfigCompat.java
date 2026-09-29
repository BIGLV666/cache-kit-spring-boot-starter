package io.github.biglv666.cachekit.support;

/**
 * 测试用 Boot 3/4 自动配置类解析：Boot 4 把功能模块的 autoconfigure 挪了包
 * （data.redis/jdbc 等，且 Redis 的类名从 RedisAutoConfiguration 改为 DataRedisAutoConfiguration），
 * 测试代码统一经反射双 FQN 探测，使同一测试套件在 Boot 3.5 与 Boot 4 依赖树下都能运行。
 */
public final class BootAutoconfigCompat {

    private BootAutoconfigCompat() {
    }

    public static Class<?> redis() {
        return load(
                "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration",
                "org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration");
    }

    public static Class<?> dataSource() {
        return load(
                "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
                "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration");
    }

    public static Class<?> jdbcTemplate() {
        return load(
                "org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration",
                "org.springframework.boot.jdbc.autoconfigure.JdbcTemplateAutoConfiguration");
    }

    public static Class<?> aop() {
        return load(
                "org.springframework.boot.autoconfigure.aop.AopAutoConfiguration",
                "org.springframework.boot.aop.autoconfigure.AopAutoConfiguration");
    }

    private static Class<?> load(String boot3Fqn, String boot4Fqn) {
        for (String fqn : new String[]{boot3Fqn, boot4Fqn}) {
            try {
                return Class.forName(fqn);
            } catch (ClassNotFoundException ignored) {
                // 尝试下一个候选
            }
        }
        throw new IllegalStateException("Boot 3/4 自动配置类均不存在: " + boot3Fqn + " / " + boot4Fqn);
    }
}
