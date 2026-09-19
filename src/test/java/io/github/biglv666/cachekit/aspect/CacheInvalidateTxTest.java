package io.github.biglv666.cachekit.aspect;

import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.DoubleDeleteScheduler;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 事务感知失效：活动事务内 @CacheInvalidate 的失效延迟到 afterCommit，
 * 消除"删除在提交前、并发读回填旧值"的窗口；回滚不失效；关闭配置退回立即失效。
 */
class CacheInvalidateTxTest {

    private TieredEntityCache cache;
    private EntityMetadata meta;

    @BeforeEach
    void setUp() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        cache = new TieredEntityCache(props,
                new CaffeineChannel(1024), null,
                new NoopInvalidationPublisher(),
                new DoubleDeleteScheduler(Duration.ofMillis(50)));
        meta = new EntityMetadataRegistry().require(UserEntity.class);
        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "lv"));
    }

    @AfterEach
    void cleanupTxState() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
        TransactionSynchronizationManager.setActualTransactionActive(false);
    }

    private CacheInvalidateAspect aspect(boolean evictAfterCommit) {
        return new CacheInvalidateAspect(cache, new EntityMetadataRegistry(), evictAfterCommit);
    }

    @Test
    void evictShouldBeDeferredUntilAfterCommit() {
        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.initSynchronization();
        try {
            aspect(true).evictSmart(meta, 1L);
            // 提交前：缓存尚未失效
            assertThat(cache.peek(meta, 1L).state())
                    .as("事务提交前失效不得执行").isEqualTo(TieredEntityCache.CachePeek.State.HIT);

            for (TransactionSynchronization sync : TransactionSynchronizationManager.getSynchronizations()) {
                sync.afterCommit();
            }
            assertThat(cache.peek(meta, 1L).state())
                    .as("afterCommit 后必须失效").isEqualTo(TieredEntityCache.CachePeek.State.MISS);
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    void rollbackShouldNotEvict() {
        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.initSynchronization();
        try {
            aspect(true).evictSmart(meta, 1L);
            // 回滚：不触发 afterCommit，缓存保持原样（数据未变）
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
        assertThat(cache.peek(meta, 1L).state())
                .as("事务回滚不失效").isEqualTo(TieredEntityCache.CachePeek.State.HIT);
    }

    @Test
    void noActiveTransactionShouldEvictImmediately() {
        aspect(true).evictSmart(meta, 1L);
        assertThat(cache.peek(meta, 1L).state())
                .as("无事务立即失效").isEqualTo(TieredEntityCache.CachePeek.State.MISS);
    }

    @Test
    void syncWithoutActualTransactionShouldEvictImmediately() {
        // 仅同步器、无真实事务（手工 initSynchronization、部分消息监听容器宿主）：
        // afterCommit 永远不会触发，必须立即失效而不是注册上去被静默吞掉
        TransactionSynchronizationManager.initSynchronization();
        try {
            aspect(true).evictSmart(meta, 1L);
            assertThat(cache.peek(meta, 1L).state())
                    .as("无真实事务时必须立即失效")
                    .isEqualTo(TieredEntityCache.CachePeek.State.MISS);
        } finally {
            if (TransactionSynchronizationManager.isSynchronizationActive()) {
                TransactionSynchronizationManager.clearSynchronization();
            }
        }
    }

    @Test
    void handleEvictShouldAlsoBeTransactionAware() {
        // 手动句柄（@CacheHandle 注入的 EntityCache）与注解路径共用事务感知钩子
        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.initSynchronization();
        try {
            io.github.biglv666.cachekit.core.EntityCache<UserEntity> handle =
                    new io.github.biglv666.cachekit.handle.DelegatingEntityCache<>(
                            UserEntity.class, new EntityMetadataRegistry(), cache, aspect(true));
            handle.evict(1L);
            assertThat(cache.peek(meta, 1L).state())
                    .as("事务内句柄失效同样延迟到 afterCommit")
                    .isEqualTo(TieredEntityCache.CachePeek.State.HIT);
            for (TransactionSynchronization sync : TransactionSynchronizationManager.getSynchronizations()) {
                sync.afterCommit();
            }
            assertThat(cache.peek(meta, 1L).state()).isEqualTo(TieredEntityCache.CachePeek.State.MISS);
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    void disabledConfigShouldEvictImmediatelyEvenInTx() {
        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.initSynchronization();
        try {
            aspect(false).evictSmart(meta, 1L);
            assertThat(cache.peek(meta, 1L).state())
                    .as("关闭 evict-after-commit 后事务内也立即失效")
                    .isEqualTo(TieredEntityCache.CachePeek.State.MISS);
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }
}
