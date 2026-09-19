package io.github.biglv666.cachekit.model;

import io.github.biglv666.cachekit.annotation.CacheEntity;

/** 测试实体：无主键注解、仅有一个名为 id 的字段——兜底主键解析路径 */
@CacheEntity
public class PlainIdEntity {

    private Long id;
    private String name;

    public PlainIdEntity() {
    }

    public PlainIdEntity(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}
