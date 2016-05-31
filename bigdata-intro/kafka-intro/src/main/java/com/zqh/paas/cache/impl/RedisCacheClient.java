package com.zqh.paas.cache.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.zqh.paas.util.SerializeUtil;

/**
 * redis的客户端实现
 *
 */
public class RedisCacheClient {
	private static final Logger log = Logger.getLogger(RedisCache.class);
	private JedisPool pool;
	private JedisPoolConfig config;
	private static final String HOST_KEY = "host";
	private static final String PORT_KEY = "port";
	private static final String TIMEOUT_KEY = "timeOut";
	private static final String MAXACTIVE_KEY = "maxActive";
	private static final String MAXIDLE_KEY = "maxIdle";
	private static final String MAXWAIT_KEY = "maxWait";
	private static final String TESTONBORROW_KEY = "testOnBorrow";
	private static final String TESTONRETURN_KEY = "testOnReturn";

	public RedisCacheClient(String parameter) {
		try {
			JSONObject json = JSONObject.fromObject(parameter);
			if (json != null) {
				config = new JedisPoolConfig();
				//config.setMaxActive(json.getInt(MAXACTIVE_KEY));
				//config.setMaxActive(json.getInt(MAXIDLE_KEY));
				//config.setMaxWait(json.getLong(MAXWAIT_KEY));
				config.setTestOnBorrow(json.getBoolean(TESTONBORROW_KEY));
				config.setTestOnReturn(json.getBoolean(TESTONRETURN_KEY));
				pool = new JedisPool(config, json.getString(HOST_KEY),
						json.getInt(PORT_KEY), json.getInt(TIMEOUT_KEY));
			}
		} catch (Exception e) {
			log.error("",e);
		}
	}

	public void addItemToList(int dbIndex, String key, Object object) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.connect();
			jedis.select(dbIndex);
			jedis.lpush(key.getBytes(), SerializeUtil.serialize(object));
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List getItemFromList(int dbIndex, String key) {
		Jedis jedis = null;
		List<byte[]> ss = null;
		List data = new ArrayList();
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			long len = jedis.llen(key);
			if (len == 0)
				return null;
			ss = jedis.lrange(key.getBytes(), 0, (int) len - 1);
			for (int i = 0; i < len; i++) {
				data.add(SerializeUtil.unserialize(ss.get(i)));
			}
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return data;

	}

	public void addItem(int dbIndex, String key, Object object) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			jedis.set(key.getBytes(), SerializeUtil.serialize(object));
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}

	public String flushDB(int dbIndex) {
		Jedis jedis = null;
		String result = "";
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			result = jedis.flushDB();
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return result;
	}

	public void addItem(int dbIndex, String key, Object object, int seconds) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			jedis.setex(key.getBytes(), seconds,
					SerializeUtil.serialize(object));
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	public Object getItem(int dbIndex, String key) {
		Jedis jedis = null;
		byte[] data = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			data = jedis.get(key.getBytes());
			return SerializeUtil.unserialize(data);
		} catch (Exception e) {
			log.error("",e);
			return null;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}

	public void delItem(int dbIndex, String key) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			jedis.del(key.getBytes());
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}

	public long getIncrement(int dbIndex, String key) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			return jedis.incr(key);
		} catch (Exception e) {
			log.error("",e);
			return 0L;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}

	/**
	 * 
	 * 
	 * @param key
	 * @param map
	 */
	public void addMap(int dbIndex, String key, Map<String, String> map) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			if (map != null && !map.isEmpty()) {
				for (String mapkey:map.keySet()) {
					jedis.hset(key, mapkey, map.get(mapkey));
				}

			}
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}

	public Map<String, String> getMap(int dbIndex, String key) {
		Map<String, String> map = null;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			map = jedis.hgetAll(key);
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return map;

	}
	public String getMapItem(int dbIndex,String key,String field) {
		String value = null;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			value = jedis.hget(key, field);
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return value;

	}

	/**
	 * 
	 * 
	 * @param key
	 * @param set
	 */
	public void addSet(int dbIndex, String key, Set<String> set) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			if (set != null && !set.isEmpty()) {
				for (String value : set) {
					jedis.sadd(key, value);
				}
			}
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	public Set<String> getSet(int dbIndex, String key) {
		Set<String> sets = new HashSet<String>();
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			sets = jedis.smembers(key);
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

		return sets;
	}
	
	/**
	 * 
	 * 
	 * @param key
	 * @param list
	 */
	public void addList( int dbIndex,String key, List<String> list) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			if (list != null && !list.isEmpty()) {
				for (String value : list) {
					jedis.lpush(key, value);
				}
			}
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}
	
	public List<String> getList(int dbIndex, String key) {
		List<String> lists = null;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			lists = jedis.lrange(key, 0, -1);
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

		return lists;
	}
	
	public void destroyPool() {
		if(null!=pool) {
			pool.destroy();
		}
	}
	
	
	public void addItem(String key, Object object) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.set(key.getBytes(), SerializeUtil.serialize(object));
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}


	public void addItem(String key, Object object, int seconds) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.setex(key.getBytes(), seconds,
					SerializeUtil.serialize(object));
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	public Object getItem(String key) {
		Jedis jedis = null;
		byte[] data = null;
		try {
			jedis = pool.getResource();
			data = jedis.get(key.getBytes());
			return SerializeUtil.unserialize(data);
		} catch (Exception e) {
			log.error("",e);
			return null;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}

	public void delItem(String key) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.del(key.getBytes());
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}
	
	public long getIncrement(String key) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			return jedis.incr(key);
		} catch (Exception e) {
			log.error("",e);
			return 0L;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}
	public void addItemFile(String key, byte[] file) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.set(key.getBytes(), file);
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}
	
	
	
	/**
	 * 
	 * 
	 * @param key
	 * @param map
	 */
	public void addMap(String key, Map<String, String> map) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			if (map != null && !map.isEmpty()) {
				for (String mapkey:map.keySet()) {
					jedis.hset(key, mapkey, map.get(mapkey));
				}
			}
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}

	public Map<String, String> getMap(String key) {
		Map<String, String> map = null;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			map = jedis.hgetAll(key);
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return map;

	}
	public String getMapItem(String key,String field) {
		String value = null;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			value = jedis.hget(key, field);
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return value;

	}

	/**
	 * 
	 * 
	 * @param key
	 * @param set
	 */
	public void addSet( String key, Set<String> set) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			if (set != null && !set.isEmpty()) {
				for (String value : set) {
					jedis.sadd(key, value);
				}
			}
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}
	public Set<String> getSet(String key) {
		Set<String> sets = new HashSet<String>();
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			sets = jedis.smembers(key);
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

		return sets;
	}
	/**
	 * 
	 * 
	 * @param key
	 * @param list
	 */
	public void addList( String key, List<String> list) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			if (list != null && !list.isEmpty()) {
				for (String value : list) {
					jedis.rpush(key, value);
				}
			}
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}
	public List<String> getList(String key) {
		List<String> lists = null;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			lists = jedis.lrange(key, 0, -1);
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

		return lists;
	}
	public void addMapItem(String key, String field,String value) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.hset(key, field, value);
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	public void delMapItem(String key, String field) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.hdel(key, field);
		} catch (Exception e) {
			log.error("",e);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

}
