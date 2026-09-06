package io.github.biglv666.cachekit.model;

import io.github.biglv666.cachekit.annotation.CacheEntity;
import io.github.biglv666.cachekit.annotation.CacheId;

/** 测试实体：自有注解元数据 */
@CacheEntity(ttl = 60)
public class UserEntity {

    @CacheId
    private Long userId;
    private String userName;

    public UserEntity() {
    }

    public UserEntity(Long userId, String userName) {
        this.userId = userId;
        this.userName = userName;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UserEntity that)) {
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
