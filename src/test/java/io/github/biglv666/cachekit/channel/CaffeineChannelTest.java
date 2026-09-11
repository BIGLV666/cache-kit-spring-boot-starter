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

    @Test
    void maxWeightShouldEvictLargeEntries() {
        // 1KB 权重上限：每条 2KB 的值权重为 2，存不下 50 条
        CaffeineChannel weighted = new CaffeineChannel(1000, 1);
        for (int i = 0; i < 50; i++) {
            weighted.put("big" + i, "x".repeat(2048), Duration.ofMinutes(5));
        }
        int hits = 0;
        for (int i = 0; i < 50; i++) {
            if (weighted.get("big" + i).hit()) {
                hits++;
            }
        }
        assertThat(hits).as("权重上限下大条目被淘汰，驻留数远小于 50").isLessThan(10);

        // 对比：条目数上限模式下 50 条小值全部驻留
        CaffeineChannel entries = new CaffeineChannel(1000);
        for (int i = 0; i < 50; i++) {
            entries.put("s" + i, "v", Duration.ofMinutes(5));
        }
        assertThat(entries.get("s0").hit()).isTrue();
    }
}
