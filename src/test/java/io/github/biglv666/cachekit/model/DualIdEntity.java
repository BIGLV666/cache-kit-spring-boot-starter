package io.github.biglv666.cachekit.model;

import io.github.biglv666.cachekit.annotation.CacheId;

/** 测试实体：声明了两个 @CacheId（0.3.3+ 组成复合主键，声明顺序 join） */
public class DualIdEntity {

    @CacheId
    private Long userId;
    @CacheId
    private String code;

    public Long getUserId() {
        return userId;
    }

    public String getCode() {
        return code;
    }
}
