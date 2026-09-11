package io.github.biglv666.cachekit.model;

import io.github.biglv666.cachekit.annotation.CacheEntity;
import io.github.biglv666.cachekit.annotation.CacheId;

/**
 * 对 Jackson 不可序列化的实体：self 字段自引用，序列化触发无限递归
 * （JsonMappingException）——用于验证"序列化失败不丢业务数据"。
 */
@CacheEntity
public class UnserializableEntity {

    @CacheId
    private Long id;

    private UnserializableEntity self;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public UnserializableEntity getSelf() {
        return self;
    }

    public void setSelf(UnserializableEntity self) {
        this.self = self;
    }
}
