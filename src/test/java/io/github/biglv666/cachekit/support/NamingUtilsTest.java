package io.github.biglv666.cachekit.support;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NamingUtilsTest {

    @Test
    void camelToSnakeShouldConvertClassName() {
        assertThat(NamingUtils.camelToSnake("User")).isEqualTo("user");
        assertThat(NamingUtils.camelToSnake("UserLogin")).isEqualTo("user_login");
        assertThat(NamingUtils.camelToSnake("OrderItemDetail")).isEqualTo("order_item_detail");
    }

    @Test
    void camelToSnakeShouldHandleEdgeCases() {
        assertThat(NamingUtils.camelToSnake("")).isEmpty();
        assertThat(NamingUtils.camelToSnake(null)).isNull();
        assertThat(NamingUtils.camelToSnake("already_snake")).isEqualTo("already_snake");
    }
}
