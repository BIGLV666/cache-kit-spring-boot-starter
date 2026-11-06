package io.github.biglv666.cachekit.generic;

import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.CacheStore;
import io.github.biglv666.cachekit.core.DoubleDeleteScheduler;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.support.JsonCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultCacheStoreTest {

    private CacheStore<TestValue> store;
    private TieredValueCache cache;
    private GenericCacheMetadata meta;
    private AtomicInteger loads;

    @BeforeEach
    void setUp() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        cache = new TieredValueCache(props, new CaffeineChannel(1024), null,
                new NoopInvalidationPublisher(), null, "", List.of());
        GenericCacheRegistry registry = new GenericCacheRegistry();
        meta = registry.require(TestValue.class);
        store = new DefaultCacheStore<>(meta, JsonCodec.constructType(TestValue.class), cache, false);
        loads = new AtomicInteger();
    }

    @Test
    void shouldGetPutEvictAndRefresh() {
        assertThat(store.get("k")).isNull();

        store.put("k", new TestValue("v1", 1));
        assertThat(store.get("k")).isEqualTo(new TestValue("v1", 1));

        TestValue loaded = store.get("k", () -> {
            loads.incrementAndGet();
            return new TestValue("v2", 2);
        });
        assertThat(loaded).isEqualTo(new TestValue("v1", 1));
        assertThat(loads.get()).isZero();

        store.evict("k");
        assertThat(store.get("k")).isNull();

        TestValue refreshed = store.refresh("k", () -> {
            loads.incrementAndGet();
            return new TestValue("v3", 3);
        });
        assertThat(refreshed).isEqualTo(new TestValue("v3", 3));
        assertThat(store.get("k")).isEqualTo(new TestValue("v3", 3));
        assertThat(loads.get()).isEqualTo(1);
    }

    @Test
    void refreshShouldNotBeDeletedByDelayedDoubleDelete() throws Exception {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        DoubleDeleteScheduler scheduler = new DoubleDeleteScheduler(Duration.ofMillis(20));
        try {
            TieredValueCache schedulerCache = new TieredValueCache(props, new CaffeineChannel(1024), null,
                    new NoopInvalidationPublisher(), scheduler, "", List.of());
            CacheStore<TestValue> schedulerStore =
                    new DefaultCacheStore<>(meta, JsonCodec.constructType(TestValue.class), schedulerCache, false);
            schedulerStore.put("refresh", new TestValue("old", 1));

            TestValue refreshed = schedulerStore.refresh("refresh", () -> new TestValue("new", 2));
            assertThat(refreshed).isEqualTo(new TestValue("new", 2));

            Thread.sleep(150);
            assertThat(schedulerStore.get("refresh"))
                    .as("refresh 写入的新值不能被延迟双删删除")
                    .isEqualTo(new TestValue("new", 2));
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    void evictShouldBeDeferredUntilTransactionCommit() {
        CacheStore<TestValue> txStore =
                new DefaultCacheStore<>(meta, JsonCodec.constructType(TestValue.class), cache, true);
        txStore.put("tx", new TestValue("tx", 1));

        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            txStore.evict("tx");
            assertThat(txStore.get("tx")).as("事务提交前不应失效").isEqualTo(new TestValue("tx", 1));

            TransactionSynchronizationManager.getSynchronizations()
                    .forEach(TransactionSynchronization::afterCommit);
            assertThat(txStore.get("tx")).as("afterCommit 后应失效").isNull();
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    void syncWithoutActualTransactionShouldEvictImmediately() {
        CacheStore<TestValue> txStore =
                new DefaultCacheStore<>(meta, JsonCodec.constructType(TestValue.class), cache, true);
        txStore.put("sync", new TestValue("sync", 1));

        TransactionSynchronizationManager.initSynchronization();
        try {
            txStore.evict("sync");
            assertThat(txStore.get("sync"))
                    .as("仅同步器无真实事务：afterCommit 永不触发，必须立即失效")
                    .isNull();
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void readOnlyTransactionShouldEvictImmediately() {
        CacheStore<TestValue> txStore =
                new DefaultCacheStore<>(meta, JsonCodec.constructType(TestValue.class), cache, true);
        txStore.put("readonly", new TestValue("readonly", 1));

        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.setCurrentTransactionReadOnly(true);
        try {
            txStore.evict("readonly");
            assertThat(txStore.get("readonly")).as("只读事务不做写缓存语义，立即失效").isNull();
        } finally {
            TransactionSynchronizationManager.setCurrentTransactionReadOnly(false);
            TransactionSynchronizationManager.clearSynchronization();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    void disabledConfigShouldEvictImmediatelyEvenInTx() {
        CacheStore<TestValue> txStore =
                new DefaultCacheStore<>(meta, JsonCodec.constructType(TestValue.class), cache, false);
        txStore.put("disabled", new TestValue("disabled", 1));

        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.initSynchronization();
        try {
            txStore.evict("disabled");
            assertThat(txStore.get("disabled")).as("关闭 evict-after-commit 后事务内也立即失效").isNull();
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    void getWithLoaderShouldFillOnMiss() {
        TestValue value = store.get("k", () -> {
            loads.incrementAndGet();
            return new TestValue("v", 1);
        });

        assertThat(value).isEqualTo(new TestValue("v", 1));
        assertThat(store.get("k")).isEqualTo(new TestValue("v", 1));
        assertThat(loads.get()).isEqualTo(1);
    }
}
