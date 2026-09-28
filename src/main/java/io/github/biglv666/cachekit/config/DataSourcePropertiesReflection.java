package io.github.biglv666.cachekit.config;

/**
 * 数据源属性解析（Boot 3/4 兼容）：Spring Boot 4 将 {@code DataSourceProperties}
 * 从 {@code org.springframework.boot.autoconfigure.jdbc} 挪至 {@code org.springframework.boot.jdbc.autoconfigure}，
 * 直接类型引用会在 Boot 4 宿主上抛 NoClassDefFoundError——按候选 FQN 依次反射解析。
 */
final class DataSourcePropertiesReflection {

    private static final String[] CANDIDATE_FQNS = {
            "org.springframework.boot.autoconfigure.jdbc.DataSourceProperties",       // Boot 3
            "org.springframework.boot.jdbc.autoconfigure.DataSourceProperties"        // Boot 4
    };

    private DataSourcePropertiesReflection() {
    }

    /** 从容器按候选类型取数据源属性 Bean；两类 FQN 都不存在或取失败返回 null */
    static Object resolve(org.springframework.beans.factory.BeanFactory beanFactory) {
        for (String fqn : CANDIDATE_FQNS) {
            Class<?> type;
            try {
                type = Class.forName(fqn, false, DataSourcePropertiesReflection.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                continue;
            }
            try {
                return beanFactory.getBeanProvider(type).getIfAvailable();
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    /** 反射调用 determineUrl/determineUsername/determinePassword（属性类为 public，直接 invoke 即可） */
    static String determine(Object dsProps, String method) {
        if (dsProps == null) {
            return null;
        }
        try {
            Object result = dsProps.getClass().getMethod(method).invoke(dsProps);
            return result instanceof String s ? s : null;
        } catch (Exception e) {
            return null;
        }
    }
}
