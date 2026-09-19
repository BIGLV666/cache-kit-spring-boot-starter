package io.github.biglv666.cachekit.model;

import io.github.biglv666.cachekit.annotation.CacheEntity;
import io.github.biglv666.cachekit.annotation.CacheId;

/** 测试实体：实体级 TTL（2s）小于全局 L1 TTL（30s）——验证 L1 TTL 取较小值不倒装 */
@CacheEntity(ttl = 2)
public class TtlEntity {

    @CacheId
    private Long userId;
    private String userName;

    public TtlEntity() {
    }

    public TtlEntity(Long userId, String userName) {
        this.userId = userId;
        this.userName = userName;
    }

    public Long getUserId() {
        return userId;
    }

    public String getUserName() {
        return userName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TtlEntity that)) {
            return false;
        }
        return java.util.Objects.equals(userId, that.userId)
                && java.util.Objects.equals(userName, that.userName);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(userId, userName);
    }
}
