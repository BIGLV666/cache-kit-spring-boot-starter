package io.github.biglv666.cachekit.model;

import io.github.biglv666.cachekit.annotation.CacheId;

/** 测试实体：声明了两个 @CacheId，主键歧义必须 fail-fast */
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
