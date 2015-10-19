package org.shirdrn.storm.api;

import java.io.Serializable;

/**
 * Timeout cache interface to cache object with a fix expire time.
 * Usually a cache maybe a Memcached, or Redis, etc.
 * 
 * @author Yanjun
 *
 * @param <CONNECTION> Connection used to put object into cache.
 * @param <KEY> Key name for cached object.
 * @param <VALUE> Cached value data.
 */
public interface TimeoutCache<CONNECTION, KEY, VALUE> extends Serializable {

	/**
	 * Add object to the cache, with <code>key</code> and <code>value</code>, 
	 * as well as expire time.
	 * @param connection
	 * @param key
	 * @param value
	 * @param expredSecs
	 */
	void put(CONNECTION connection, KEY key, VALUE value, int expredSecs);
	
	/**
	 * Given a key, obtain a object from cache.
	 * @param connection
	 * @param key
	 * @return
	 */
	VALUE get(CONNECTION connection, KEY key);
}
