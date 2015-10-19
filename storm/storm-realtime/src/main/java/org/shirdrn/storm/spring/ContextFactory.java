package org.shirdrn.storm.spring;

/**
 * A context object can be held by a {@link ContextFactory} object,
 * which has a map container to manage multiple context
 * objects based on the context <code>name</code>.
 *
 * 一个上下文对象可以被ContextFactory对象持有.
 * 上下文对象的工厂对象用一个map容器管理多个基于名称的上下文对象
 * @author yanjun
 * @param <T> Context type
 */
public interface ContextFactory<T> {

	/**
	 * Register a context identified by <code>name</code> 
	 * related with <code>configs</code>.
     *
     * 用名称标识符来注册一个上下文对象
	 * @param name
	 * @param configs
	 */
	void register(String name, String... configs);
	
	/**
	 * Obtain a context object by retrieving the given context <code>name</code>.
     *
     * 通过名称得到上下文对象
	 * @param name
	 * @return
	 */
	T getContext(String name);
	
}
