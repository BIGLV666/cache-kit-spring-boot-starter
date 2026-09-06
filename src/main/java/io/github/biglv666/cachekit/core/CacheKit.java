package io.github.biglv666.cachekit.core;

import java.util.function.Supplier;

/**
 * cache-kit 门面：提供作用域级强一致读（旁路缓存直查 DB）。
 *
 * <pre>{@code
 * User u = CacheKit.withDb(() -> mapper.selectById(1L));
 * }</pre>
 */
public final class CacheKit {

    private CacheKit() {
    }

    /**
     * 在指定作用域内跳过缓存直查 DB（含 BaseMapper 自动拦截），失效操作不受影响。
     *
     * @param supplier 业务读取逻辑
     * @return 逻辑返回值
     */
    public static <T> T withDb(Supplier<T> supplier) {
        BypassContext.enter();
        try {
            return supplier.get();
        } finally {
            BypassContext.exit();
        }
    }

    /** {@link #withDb(Supplier)} 的无返回值版本 */
    public static void withDb(Runnable action) {
        BypassContext.enter();
        try {
            action.run();
        } finally {
            BypassContext.exit();
        }
    }
}
