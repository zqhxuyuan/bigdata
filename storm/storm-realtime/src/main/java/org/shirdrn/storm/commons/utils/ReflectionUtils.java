package org.shirdrn.storm.commons.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ReflectionUtils {
	

	private static final ClassLoader DEFAULT_CLASSLOADER = ReflectionUtils.class.getClassLoader();

	public static Object newInstance(String className) {
		return newInstance(className, DEFAULT_CLASSLOADER);
	}

	public static Object newInstance(String className, ClassLoader classLoader) {
		Object instance = null;
		try {
			Class<?> clazz = Class.forName(className, true, getClassLoader(classLoader));
			instance = clazz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return instance;
	}
	
	public static <R, T> T newInstance(Class<R> clazz, Class<T> baseClass, Object... parameters) {
		T instance = null;
		try {
			instance = construct(clazz, baseClass, parameters);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return instance;
	}
	
	@SuppressWarnings("unchecked")
	private static <R, T> T construct(Class<R> clazz, Class<T> baseClass, Object... parameters)
			throws InstantiationException, IllegalAccessException, InvocationTargetException {
		T instance = null;
		Constructor<R>[] constructors = (Constructor<R>[]) clazz.getConstructors();
		for (Constructor<R> c : constructors) {
			if (c.getParameterTypes().length == parameters.length) {
				for (int i = 0; i < c.getParameterTypes().length; i++) {
					if(!parameters[i].getClass().equals(c.getParameterTypes()[i])) {
						break;
					}
				}
				instance = (T) c.newInstance(parameters);
				break;
			}
		}
		return instance;
	}

	public static <T> T newInstance(Class<T> clazz) {
		T instance = null;
		try {
			instance = clazz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return instance;
	}

	private static ClassLoader getClassLoader(ClassLoader classLoader) {
		if (classLoader == null) {
			classLoader = DEFAULT_CLASSLOADER;
		}
		return classLoader;
	}
}