package io.github.biglv666.cachekit.core;

/**
 * 通用缓存键协议常量与广播消息封装。
 *
 * <p>键格式：{@code [自定义段:]namespace:__gk__:prefix:用户key}。
 * {@code __gk__} 是本组件保留的通用缓存段，用于和实体缓存键空间隔离；
 * 广播消息在完整键前再加一个保留前缀，使同一 topic/stream 上的实体失效消息
 * 与通用失效消息可以无歧义路由。</p>
 *
 * <p>保留值由组件内部使用，业务前缀不得包含 {@code :} 或 {@code __gk__}。</p>
 */
public final class GenericKeyCodec {

    /** 通用缓存键中的保留段 */
    public static final String SEGMENT = "__gk__";

    /**
     * 广播消息保留前缀：使用控制字符包裹，避免与正常 namespace/表名前缀混淆
     *（普通实体键不会以该字符开头；Redis 消息按 UTF-8 传输）。
     */
    public static final String MESSAGE_PREFIX = "\u0001cache-kit:generic\u0001";

    private GenericKeyCodec() {
    }

    /** 将通用缓存完整键封装为广播消息 */
    public static String wrap(String key) {
        return MESSAGE_PREFIX + key;
    }

    /** 是否为通用缓存广播消息 */
    public static boolean isGenericMessage(String payload) {
        return payload != null && payload.startsWith(MESSAGE_PREFIX);
    }

    /** 解开通用缓存广播消息 */
    public static String unwrap(String payload) {
        return payload.substring(MESSAGE_PREFIX.length());
    }
}
