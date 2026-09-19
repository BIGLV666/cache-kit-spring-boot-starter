package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.DefaultMessage;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 失效广播订阅端：键解析必须与 TieredEntityCache.key() 的组装规则对齐——
 * 含 CacheKeyCustomizer 段 / namespace / 含冒号的主键段都要能匹配。
 * 历史缺陷：带自定义段的广播被订阅端 100% 丢弃，跨实例 L1 静默脏读直达 L1 TTL。
 */
class InvalidationSubscriberTest {

    private final CaffeineChannel l1 = new CaffeineChannel(128);
    private final EntityMetadataRegistry registry = new EntityMetadataRegistry();
    private final List<Boolean> applied = new ArrayList<>();
    private final InvalidationSubscriber subscriber = create("");

    private InvalidationSubscriber create(String namespace) {
        InvalidationSubscriber s = new InvalidationSubscriber(l1, registry, namespace);
        s.setMetricsListener(new CacheMetricsListener() {
            @Override
            public void broadcastReceived(boolean appliedResult) {
                applied.add(appliedResult);
            }
        });
        return s;
    }

    private void prime(String key) {
        registry.find(UserEntity.class);
        l1.put(key, "v", Duration.ofSeconds(30));
    }

    private void broadcast(String key) {
        subscriber.onMessage(new DefaultMessage(new byte[0], key.getBytes(StandardCharsets.UTF_8)), null);
    }

    private void broadcastTo(InvalidationSubscriber target, String key) {
        target.onMessage(new DefaultMessage(new byte[0], key.getBytes(StandardCharsets.UTF_8)), null);
    }

    @Test
    void plainKeyShouldEvictLocalL1() {
        prime("user_entity:1");
        broadcast("user_entity:1");
        assertThat(l1.get("user_entity:1").hit()).isFalse();
        assertThat(applied).containsExactly(true);
    }

    @Test
    void customizerSegmentKeyShouldEvictLocalL1() {
        // 键形如 tenantA:user_entity:1（自定义段在最前）：修复前在这里被静默丢弃
        prime("tenantA:user_entity:1");
        broadcast("tenantA:user_entity:1");
        assertThat(l1.get("tenantA:user_entity:1").hit()).isFalse();
        assertThat(applied).containsExactly(true);
    }

    @Test
    void namespaceKeyShouldEvictWhenSegmentMatches() {
        prime("tenantA:db1:user_entity:1");
        broadcastTo(create("db1"), "tenantA:db1:user_entity:1");
        assertThat(l1.get("tenantA:db1:user_entity:1").hit()).isFalse();
        assertThat(applied).containsExactly(true);
    }

    @Test
    void otherNamespaceKeyShouldBeIgnored() {
        prime("tenantA:db1:user_entity:1");
        broadcastTo(create("other"), "tenantA:db1:user_entity:1");
        assertThat(l1.get("tenantA:db1:user_entity:1").hit()).isTrue();
        assertThat(applied).containsExactly(false);
    }

    @Test
    void idContainingColonShouldStillMatch() {
        // String 主键含 ':'：不能按最后一个冒号切前缀
        prime("user_entity:a:b");
        broadcast("user_entity:a:b");
        assertThat(l1.get("user_entity:a:b").hit()).isFalse();
    }

    @Test
    void namespaceWithColonIdShouldMatch() {
        prime("tenantA:db1:user_entity:a:b");
        broadcastTo(create("db1"), "tenantA:db1:user_entity:a:b");
        assertThat(l1.get("tenantA:db1:user_entity:a:b").hit()).isFalse();
    }

    @Test
    void unknownPrefixShouldBeIgnored() {
        // 安全边界：前缀匹配不到已知实体的广播必须忽略（防任意客户端清缓存）
        prime("attacker:1");
        broadcast("attacker:1");
        assertThat(l1.get("attacker:1").hit()).isTrue();
        assertThat(applied).containsExactly(false);
    }

    @Test
    void prefixMustBeWholeSegment() {
        // 前缀必须按完整段匹配："myuser_entity:1" 不应命中前缀 "user_entity"
        prime("myuser_entity:1");
        broadcast("myuser_entity:1");
        assertThat(l1.get("myuser_entity:1").hit()).isTrue();
        assertThat(applied).containsExactly(false);
    }
}
