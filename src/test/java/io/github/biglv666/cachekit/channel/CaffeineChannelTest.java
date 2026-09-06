package io.github.biglv666.cachekit.channel;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class CaffeineChannelTest {

    private final CaffeineChannel channel = new CaffeineChannel(128);

    @Test
    void putGetEvictShouldWork() {
        assertThat(channel.get("k")).isEqualTo(CacheEntry.miss());

        channel.put("k", "{\"a\":1}", Duration.ofSeconds(5));
        assertThat(channel.get("k")).isEqualTo(CacheEntry.of("{\"a\":1}"));

        channel.evict("k");
        assertThat(channel.get("k")).isEqualTo(CacheEntry.miss());
    }

    @Test
    void entryShouldExpireAfterTtl() throws InterruptedException {
        channel.put("k", "v", Duration.ofMillis(80));
        assertThat(channel.get("k").hit()).isTrue();

        Thread.sleep(300);
        assertThat(channel.get("k")).isEqualTo(CacheEntry.miss());
    }

    @Test
    void nonPositiveTtlShouldNotStore() {
        channel.put("k", "v", Duration.ZERO);
        channel.put("k2", "v", Duration.ofSeconds(-1));
        assertThat(channel.get("k")).isEqualTo(CacheEntry.miss());
        assertThat(channel.get("k2")).isEqualTo(CacheEntry.miss());
    }
}
