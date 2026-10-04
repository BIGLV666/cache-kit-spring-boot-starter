package io.github.biglv666.cachekit.channel;

import io.github.biglv666.cachekit.core.CacheMetricsListener;
import io.github.biglv666.cachekit.core.L2CircuitBreaker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * RedisChannel × 熔断器单元测试：连续失败后短路（不再触达 Redis）、
 * 降级语义（get 未命中 / evictAll false / multiGet 全未命中）、降级计数。
 * 不依赖真实 Redis（模板 mock），CI 与本地均确定执行。
 */
class RedisChannelCircuitBreakerTest {

    private final StringRedisTemplate template = mock(StringRedisTemplate.class);
    private final ValueOperations<String, String> valueOps = mock(ValueOperations.class);

    @BeforeEach
    void setUp() {
        when(template.opsForValue()).thenReturn(valueOps);
    }

    private static class RecordingListener implements CacheMetricsListener {
        final AtomicInteger fallbacks = new AtomicInteger();
        final AtomicInteger opened = new AtomicInteger();

        @Override
        public void l2Fallback(String op) {
            fallbacks.incrementAndGet();
        }

        @Override
        public void l2CircuitOpened() {
            opened.incrementAndGet();
        }
    }

    @Test
    void consecutiveFailuresShouldOpenCircuitAndStopTouchingRedis() {
        when(valueOps.get(anyString())).thenThrow(new IllegalStateException("connection refused"));
        RedisChannel channel = new RedisChannel(template);
        RecordingListener listener = new RecordingListener();
        // 与生产装配一致：通道与熔断器挂同一个监听器
        L2CircuitBreaker breaker = new L2CircuitBreaker(2, Duration.ofSeconds(30));
        breaker.setMetricsListener(listener);
        channel.setMetricsListener(listener);
        channel.setCircuitBreaker(breaker);

        // 前两次失败真实触达 Redis
        assertThat(channel.get("k")).isEqualTo(CacheEntry.miss());
        assertThat(channel.get("k")).isEqualTo(CacheEntry.miss());
        verify(valueOps, times(2)).get(anyString());

        // 达到阈值熔断：短路返回未命中，Redis 零调用
        assertThat(channel.get("k")).isEqualTo(CacheEntry.miss());
        verify(valueOps, times(2)).get(anyString());
        verifyNoMoreInteractions(valueOps);

        // evictAll 短路必须返回 false（失效路径据此安排重试），同样不触达 Redis
        assertThat(channel.evictAll(List.of("a", "b"))).isFalse();
        verify(template, times(0)).delete(anyCollection());

        // multiGet 短路返回全未命中映射（而非 null），避免调用方逐键重试放大计数
        Map<String, CacheEntry> res = channel.multiGet(List.of("a", "b"));
        assertThat(res).containsEntry("a", CacheEntry.miss()).containsEntry("b", CacheEntry.miss());

        // 降级计数：2 次真实失败 + get/evictAll/multiGet 三次短路
        assertThat(listener.fallbacks.get()).isEqualTo(5);
        assertThat(listener.opened.get()).isEqualTo(1);
    }

    @Test
    void successShouldKeepCircuitClosedAndRecordNothing() {
        when(valueOps.get(anyString())).thenReturn("v");
        RedisChannel channel = new RedisChannel(template);
        RecordingListener listener = new RecordingListener();
        channel.setMetricsListener(listener);
        channel.setCircuitBreaker(new L2CircuitBreaker(2, Duration.ofSeconds(30)));

        assertThat(channel.get("k")).isEqualTo(CacheEntry.of("v"));
        assertThat(listener.fallbacks.get()).isZero();
        assertThat(listener.opened.get()).isZero();
    }

    @Test
    void putShortCircuitShouldSkipRedis() {
        when(valueOps.get(anyString())).thenThrow(new IllegalStateException("down"));
        RedisChannel channel = new RedisChannel(template);
        channel.setCircuitBreaker(new L2CircuitBreaker(1, Duration.ofSeconds(30)));

        channel.get("k"); // 触发熔断
        channel.put("k", "v", Duration.ofSeconds(30));

        verify(valueOps, times(1)).get(anyString());
        verify(valueOps, times(0)).set(anyString(), anyString(), org.mockito.ArgumentMatchers.any(Duration.class));
    }
}
