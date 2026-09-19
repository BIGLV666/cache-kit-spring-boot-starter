package io.github.biglv666.cachekit.exception;

import java.util.List;

/**
 * 严格模式误判守卫：批量请求值与回源结果的主键对不上，说明请求集合不是主键集合
 * （如把手机号/条件值列表当主键拆解）。异常携带回源的原始结果，
 * 调用方按条件查询旁路处理（警告 + 不缓存 + 原样返回），绝不返回空数据或写错键占位。
 */
public class IdMisfireException extends CacheKitException {

    private final transient List<Object> loadedEntities;

    public IdMisfireException(Class<?> entityType, List<Object> loadedEntities) {
        super("请求值集合与回源结果主键不匹配（实体 " + entityType.getName()
                + "）：集合参数不是主键集合，已按条件查询旁路处理");
        this.loadedEntities = loadedEntities;
    }

    /** 回源的原始结果（业务应直接返回这份数据，不进缓存链） */
    public List<Object> getLoadedEntities() {
        return loadedEntities;
    }
}
