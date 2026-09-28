package io.github.biglv666.cachekit.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 数据源属性反射解析（Boot 3/4 双包名兼容）：Boot 3 类路径上按首个候选 FQN 命中。
 */
class DataSourcePropertiesReflectionTest {

    @Test
    void shouldResolveAndReflectWhenClassPresent() {
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        DataSourceProperties props = new DataSourceProperties();
        props.setUrl("jdbc:mysql://localhost:3306/db");
        props.setUsername("root");
        props.setPassword("x");
        beanFactory.registerSingleton("dsProps", props);

        Object resolved = DataSourcePropertiesReflection.resolve(beanFactory);
        assertThat(resolved).isInstanceOf(DataSourceProperties.class);
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
