package com.alibaba.canal.canal.listen.util;

public class ReflectionUtils {
    public static<T> T getInstance(String className, Class<T> type) {
        try {
            Class<?> clazz = Class.forName(className);
            return (T)clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
