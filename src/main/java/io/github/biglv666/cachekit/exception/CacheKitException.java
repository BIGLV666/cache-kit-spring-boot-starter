package io.github.biglv666.cachekit.exception;

/**
 * cache-kit 运行时异常：主键推导失败、实体未注册缓存元数据、序列化异常等。
 */
public class CacheKitException extends RuntimeException {

    public CacheKitException(String message) {
        super(message);
    }

    public CacheKitException(String message, Throwable cause) {
        super(message, cause);
    }

    public CacheKitException(Throwable cause) {
        super(cause);
    }
}
