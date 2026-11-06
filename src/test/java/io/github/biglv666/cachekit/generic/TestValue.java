package io.github.biglv666.cachekit.generic;

import io.github.biglv666.cachekit.annotation.CacheObject;

import java.util.Objects;

@CacheObject(prefix = "app-config", ttl = 60)
public class TestValue {

    public String name;
    public int value;

    public TestValue() {
    }

    public TestValue(String name, int value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TestValue testValue)) {
            return false;
        }
        return value == testValue.value && Objects.equals(name, testValue.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public String toString() {
        return "TestValue{" + name + "=" + value + '}';
    }
}
