package io.github.biglv666.cachekit.config;

import io.github.biglv666.cachekit.handle.CacheHandleBeanPostProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * cache-kit 关闭态装配（enabled=false 时生效）：仍注册 {@code @CacheHandle} 处理器，
 * 向标注字段注入空操作句柄（读直查 DB、失效空操作）——避免字段留 null，
 * 业务首次调用时在远离配置错误的位置抛 NPE。
 */
@AutoConfiguration
@ConditionalOnProperty(prefix = "cache-kit", name = "enabled", havingValue = "false")
public class CacheKitDisabledConfiguration {

    private static final Logger log = LoggerFactory.getLogger(CacheKitDisabledConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    public static CacheHandleBeanPostProcessor cacheKitDisabledHandleBeanPostProcessor(BeanFactory beanFactory) {
        log.warn("cache-kit.enabled=false：@CacheHandle 字段将注入空操作句柄（读直查 DB、失效为空操作），"
                + "如需启用缓存请移除该配置");
        return new CacheHandleBeanPostProcessor(beanFactory);
    }
}
