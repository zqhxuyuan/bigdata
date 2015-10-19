package org.shirdrn.storm.spring.utils;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.shirdrn.storm.spring.ContextFactory;
import org.shirdrn.storm.spring.SpringContextFactory;
import org.springframework.context.ApplicationContext;

import com.google.common.collect.Maps;

public class SpringFactory {

	private static final Map<String, Object> CACHED = Maps.newHashMap();
	private static final ReentrantLock LCK = new ReentrantLock();
	
	@SuppressWarnings("unchecked")
	public static ContextFactory<ApplicationContext> getContextFactory(String contextID, String... configs) {
		try {
			LCK.lock();
			if(!CACHED.containsKey(contextID)) {
				SpringContextFactory cf = new SpringContextFactory();
				cf.register(contextID, configs);
				CACHED.put(contextID, cf);
			}
		} finally {
			LCK.unlock();
		}
		return (ContextFactory<ApplicationContext>) CACHED.get(contextID);
	}
}
