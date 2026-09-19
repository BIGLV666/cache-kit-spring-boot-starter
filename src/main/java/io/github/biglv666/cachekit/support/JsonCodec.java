package io.github.biglv666.cachekit.support;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.biglv666.cachekit.exception.CacheKitException;

/**
 * JSON 编解码：两个缓存通道（L1/L2）统一存 JSON 字符串。
 *
 * <p>反序列化容忍未知字段（{@code FAIL_ON_UNKNOWN_PROPERTIES} 关闭），实体加字段后旧缓存
 * 仍可容错读取；解析失败由调用方按"缓存失效"处理而非抛错。
 */
public final class JsonCodec {

    /** null 占位符：缓存空结果防穿透，与任何合法 JSON 串不会冲突 */
    public static final String NULL_SENTINEL = "__cache_kit_null__";

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            // Object/Map 字段的整数统一反序列化为 Long：否则小数值写缓存时是 Long、读回变 Integer，
            // 用户侧 (Long) map.get(...) 仅在缓存命中路径抛 ClassCastException（DB 直查不复现，极难排查）
            .enable(DeserializationFeature.USE_LONG_FOR_INTS)
            // java.time 等模块由宿主类路径注册（Boot 应用自带 jsr310）；日期序列化为 ISO-8601 字符串
            .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .findAndRegisterModules();

    private JsonCodec() {
    }

    /**
     * 序列化对象为 JSON 字符串。
     *
     * @throws CacheKitException 序列化失败（不可缓存的类型结构等）
     */
    public static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new CacheKitException("缓存值序列化失败: " + value.getClass(), e);
        }
    }

    /**
     * 反序列化 JSON 为指定类型，失败返回 null 由调用方按未命中处理。
     */
    public static <T> T read(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            return null;
        }
    }
}
