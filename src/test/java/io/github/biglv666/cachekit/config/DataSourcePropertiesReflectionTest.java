package io.github.biglv666.cachekit.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 数据源属性反射解析（Boot 3/4 双包名兼容）：
 * 测试本身也用反射构造属性对象——Boot 3 下命中 org.springframework.boot.autoconfigure.jdbc，
 * Boot 4 下命中 org.springframework.boot.jdbc.autoconfigure，同一用例两侧都能跑。
 */
class DataSourcePropertiesReflectionTest {

    /** 以 classpath 上实际存在的 FQN 反射构造 DataSourceProperties 并设值 */
    private static Object newDataSourceProperties(String url, String username, String password) throws Exception {
        Class<?> type = null;
        for (String fqn : new String[]{
                "org.springframework.boot.autoconfigure.jdbc.DataSourceProperties",
                "org.springframework.boot.jdbc.autoconfigure.DataSourceProperties"}) {
            try {
                type = Class.forName(fqn);
                break;
            } catch (ClassNotFoundException ignored) {
                // 尝试下一个候选
            }
        }
        assertThat(type).as("Boot 3/4 DataSourceProperties 必须存在一个").isNotNull();
        Object props = type.getDeclaredConstructor().newInstance();
        type.getMethod("setUrl", String.class).invoke(props, url);
        type.getMethod("setUsername", String.class).invoke(props, username);
        type.getMethod("setPassword", String.class).invoke(props, password);
        return props;
    }

    @Test
    void shouldResolveAndReflectWhenClassPresent() throws Exception {
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        Object props = newDataSourceProperties("jdbc:mysql://localhost:3306/db", "root", "x");
        beanFactory.registerSingleton("dsProps", props);

        Object resolved = DataSourcePropertiesReflection.resolve(beanFactory);
        assertThat(resolved).isSameAs(props);
        assertThat(DataSourcePropertiesReflection.determine(resolved, "determineUrl"))
                .isEqualTo("jdbc:mysql://localhost:3306/db");
        assertThat(DataSourcePropertiesReflection.determine(resolved, "determineUsername")).isEqualTo("root");
    }

    @Test
    void shouldReturnNullWhenAbsent() {
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        assertThat(DataSourcePropertiesReflection.resolve(beanFactory)).isNull();
        assertThat(DataSourcePropertiesReflection.determine(null, "determineUrl")).isNull();
    }
}
