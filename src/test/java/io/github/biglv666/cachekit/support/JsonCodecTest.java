package io.github.biglv666.cachekit.support;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class JsonCodecTest {

    @Test
    void objectFieldLongsShouldSurviveRoundTrip() {
        // Object/Map 字段的整数必须保持 Long：否则小数值写缓存时是 Long、读回变 Integer，
        // 用户侧 (Long) map.get(...) 仅在缓存命中路径抛 ClassCastException（DB 直查不复现）
        Map<String, Object> src = new HashMap<>();
        src.put("count", 5L);
        String json = JsonCodec.write(src);

        Map<String, Object> back = JsonCodec.read(json, Map.class);
        assertThat(back.get("count")).isInstanceOf(Long.class).isEqualTo(5L);
    }

    @Test
    void typedIntegerFieldsShouldStillBindAsInteger() {
        // USE_LONG_FOR_INTS 只影响无类型（Object/Map）绑定，实体声明的 Integer 字段不受影响
        Holder holder = JsonCodec.read("{\"age\": 25}", Holder.class);
        assertThat(holder.getAge()).isEqualTo(25);
    }

    public static class Holder {
        private Integer age;
        private Map<String, Object> extra = new HashMap<>();

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public Map<String, Object> getExtra() {
            return extra;
        }

        public void setExtra(Map<String, Object> extra) {
            this.extra = extra;
        }
    }
}
