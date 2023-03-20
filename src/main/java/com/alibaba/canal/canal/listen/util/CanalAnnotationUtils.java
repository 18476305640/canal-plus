package com.alibaba.canal.canal.listen.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public class CanalAnnotationUtils {

    public static <T> T getAnnotationAttrValue(String className, Class<? extends Annotation> annotationType,Class<T> attrType, String attrName) {
        try {
            Class<?> clazz = Class.forName(className);
            Annotation annotation = clazz.getAnnotation(annotationType);
            if (annotation != null) {
                Method method = annotationType.getDeclaredMethod(attrName);
                return (T)method.invoke(annotation);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

}
