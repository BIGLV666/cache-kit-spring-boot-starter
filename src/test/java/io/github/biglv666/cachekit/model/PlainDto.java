package io.github.biglv666.cachekit.model;

/** 测试实体：无任何缓存元数据，应判定为不可缓存 */
public class PlainDto {

    private Long someId;

    public Long getSomeId() {
        return someId;
    }

    public void setSomeId(Long someId) {
        this.someId = someId;
    }
}
