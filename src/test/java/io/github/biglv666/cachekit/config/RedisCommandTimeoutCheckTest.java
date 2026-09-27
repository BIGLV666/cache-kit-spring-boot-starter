package io.github.biglv666.cachekit.config;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * L2 装配的命令超时检查：反射读取工厂的 command timeout 供超长告警判定。
 */
class RedisCommandTimeoutCheckTest {

    @Test
    void customTimeoutShouldBeResolved() {
        LettuceConnectionFactory factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration("localhost", 6379),
                LettuceClientConfiguration.builder()
                        .commandTimeout(Duration.ofMillis(500))
                        .build());
        try {
            Duration timeout = CacheKitAutoConfiguration.resolveCommandTimeout(factory);
            assertThat(timeout).isEqualTo(Duration.ofMillis(500));
        } finally {
            factory.stop();
        }
    }

    @Test
    void unknownFactoryShouldYieldNullInsteadOfThrowing() {
        // 非标准工厂实现（无 getClientConfiguration 方法）：反射读取失败应返回 null，
        // 装配处按"无从判断"跳过告警
        RedisConnectionFactory unknown = Mockito.mock(RedisConnectionFactory.class);
        assertThat(CacheKitAutoConfiguration.resolveCommandTimeout(unknown)).isNull();

        assertThat(CacheKitAutoConfiguration.resolveCommandTimeout(null)).isNull();
    }
}
