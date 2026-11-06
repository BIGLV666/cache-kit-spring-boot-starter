package io.github.biglv666.cachekit.generic;

import com.fasterxml.jackson.databind.JavaType;
import io.github.biglv666.cachekit.core.CacheStore;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 类型解析回归：泛型方法/字段必须按具体实现类解析，未解析位置才用显式值类型兜底。
 * 这些用例直接对应"命中缓存后变成 LinkedHashMap"的生产问题。
 */
class GenericTypeResolverTest {

    static class Base<T> {
        T load() {
            return null;
        }

        List<T> loadList() {
            return null;
        }

        T[] loadArray() {
            return null;
        }

        Map<T, String> loadMap() {
            return null;
        }
    }

    static class TestValueSub extends Base<TestValue> {
    }

    interface ParameterizedApi<T> {
        Map<T, String> loadMap();
    }

    static class ListApi implements ParameterizedApi<List<TestValue>> {
        @Override
        public Map<List<TestValue>, String> loadMap() {
            return null;
        }
    }

    static class UntypedApi {
        Object loadRaw() {
            return null;
        }

        @SuppressWarnings("rawtypes")
        List loadRawList() {
            return null;
        }

        @SuppressWarnings("rawtypes")
        Map loadRawMap() {
            return null;
        }

        Map<String, Object> loadNestedObjectMap() {
            return null;
        }

        Map<?, TestValue> loadWildcardKeyMap() {
            return null;
        }
    }

    static class BaseHolder<T> {
        CacheStore<List<T>> store;
    }

    static class TestValueHolder extends BaseHolder<TestValue> {
    }

    static class ObjectHolder {
        CacheStore<Object> store;
    }

    @Test
    void shouldResolveTypeVariableAgainstConcreteSubclass() throws Exception {
        JavaType type = GenericTypeResolver.methodReturnType(
                Base.class.getDeclaredMethod("load"), TestValueSub.class, null);

        assertThat(type.getRawClass()).isEqualTo(TestValue.class);
    }

    @Test
    void shouldResolveListTypeVariableAgainstConcreteSubclass() throws Exception {
        JavaType type = GenericTypeResolver.methodReturnType(
                Base.class.getDeclaredMethod("loadList"), TestValueSub.class, null);

        assertThat(type.getRawClass()).isEqualTo(List.class);
        assertThat(type.getContentType().getRawClass()).isEqualTo(TestValue.class);
    }

    @Test
    void shouldResolveArrayOfTypeVariableAgainstConcreteSubclass() throws Exception {
        JavaType type = GenericTypeResolver.methodReturnType(
                Base.class.getDeclaredMethod("loadArray"), TestValueSub.class, null);

        assertThat(type.isArrayType()).isTrue();
        assertThat(type.getContentType().getRawClass()).isEqualTo(TestValue.class);
    }

    @Test
    void shouldPreserveParameterizedActualTypeArgument() throws Exception {
        // Api<T> 的 T = List<TestValue>：不能退化成裸 List
        JavaType type = GenericTypeResolver.methodReturnType(
                ParameterizedApi.class.getDeclaredMethod("loadMap"), ListApi.class, null);

        assertThat(type.getRawClass()).isEqualTo(Map.class);
        assertThat(type.getKeyType().getRawClass()).isEqualTo(List.class);
        assertThat(type.getKeyType().getContentType().getRawClass()).isEqualTo(TestValue.class);
        assertThat(type.getContentType().getRawClass()).isEqualTo(String.class);
    }

    @Test
    void shouldUseExplicitValueForTopLevelObject() throws Exception {
        JavaType type = GenericTypeResolver.methodReturnType(
                UntypedApi.class.getDeclaredMethod("loadRaw"), UntypedApi.class, TestValue.class);

        assertThat(type.getRawClass()).isEqualTo(TestValue.class);
    }

    @Test
    void shouldUseExplicitValueForRawList() throws Exception {
        JavaType type = GenericTypeResolver.methodReturnType(
                UntypedApi.class.getDeclaredMethod("loadRawList"), UntypedApi.class, TestValue.class);

        assertThat(type.getRawClass()).isEqualTo(List.class);
        assertThat(type.getContentType().getRawClass()).isEqualTo(TestValue.class);
    }

    @Test
    void shouldUseStringKeyAndExplicitValueForRawMap() throws Exception {
        JavaType type = GenericTypeResolver.methodReturnType(
                UntypedApi.class.getDeclaredMethod("loadRawMap"), UntypedApi.class, TestValue.class);

        assertThat(type.getRawClass()).isEqualTo(Map.class);
        assertThat(type.getKeyType().getRawClass()).isEqualTo(String.class);
        assertThat(type.getContentType().getRawClass()).isEqualTo(TestValue.class);
    }

    @Test
    void shouldUseStringKeyForWildcardKeyMap() throws Exception {
        JavaType type = GenericTypeResolver.methodReturnType(
                UntypedApi.class.getDeclaredMethod("loadWildcardKeyMap"), UntypedApi.class, TestValue.class);

        assertThat(type.getKeyType().getRawClass()).isEqualTo(String.class);
        assertThat(type.getContentType().getRawClass()).isEqualTo(TestValue.class);
    }

    @Test
    void shouldNotReplaceNestedExplicitObject() throws Exception {
        // 显式 Map<String,Object> 是用户意图，不能被 @CacheLoad.value 覆盖
        JavaType type = GenericTypeResolver.methodReturnType(
                UntypedApi.class.getDeclaredMethod("loadNestedObjectMap"), UntypedApi.class, TestValue.class);

        assertThat(type.getContentType().getRawClass()).isEqualTo(Object.class);
    }

    @Test
    void shouldResolveGenericHandleFieldAgainstConcreteSubclass() throws Exception {
        Field field = BaseHolder.class.getDeclaredField("store");

        JavaType type = GenericTypeResolver.fieldType(field, TestValueHolder.class, TestValue.class);

        assertThat(type.getRawClass()).isEqualTo(List.class);
        assertThat(type.getContentType().getRawClass()).isEqualTo(TestValue.class);
    }

    @Test
    void shouldUseExplicitValueForObjectHandleField() throws Exception {
        Field field = ObjectHolder.class.getDeclaredField("store");

        JavaType type = GenericTypeResolver.fieldType(field, ObjectHolder.class, TestValue.class);

        assertThat(type.getRawClass()).isEqualTo(TestValue.class);
    }
}
