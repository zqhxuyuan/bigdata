package com.zqh.paas.cache.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.zqh.paas.PaasException;
import com.zqh.paas.cache.ICache;
import com.zqh.paas.config.ConfigurationCenter;
import com.zqh.paas.config.ConfigurationWatcher;
import com.zqh.paas.util.JSONValidator;

/**
 * 基于统一配置的redis缓存实现
 *
 */
public class RedisCache implements ConfigurationWatcher, ICache {
	private static final Logger log = Logger.getLogger(RedisCache.class);
	private String confPath = "";
	private static final String HOST_KEY = "host";
	private static final String PORT_KEY = "port";
	private static final String TIMEOUT_KEY = "timeOut";
	private static final String MAXACTIVE_KEY = "maxActive";
	private static final String MAXIDLE_KEY = "maxIdle";
	private static final String MAXWAIT_KEY = "maxWait";
	private static final String TESTONBORROW_KEY = "testOnBorrow";
	private static final String TESTONRETURN_KEY = "testOnReturn";
	private static final String DBINDEX_KEY = "dbIndex";
	private static final String TWEMPROXY_KEY = "twemproxy";
	private String host = null;
	private String port = null;
	private String timeOut = null;
	private String maxActive = null;
	private String maxIdle = null;
	private String maxWait = null;
	private String testOnBorrow = null;
	private String testOnReturn = null;
	private RedisCacheClient cacheClient = null;
	private ConfigurationCenter confCenter = null;
	private int dbIndex = 0;
	private String twemproxy = null;

	public RedisCache() {
	}

	public void init() {
		try {
			process(confCenter.getConfAndWatch(confPath, this));
		} catch (PaasException e) {
			log.error("", e);
		}
	}

    /**
     * 有两种情景会调用到该方法. 一个是服务类初始化时,通过paasContext的init-method自动调用
     * 第二种是: 节点数据发生变化时(比如服务集群增删节点),要根据更新后的配置信息重新初始化对应的服务对象
     * @param conf
     */
	@Override
	public void process(String conf) {
		if (log.isInfoEnabled()) {
			log.info("new log configuration is received: " + conf);
		}
		JSONObject jsonObj = JSONObject.fromObject(conf);
		boolean changed = false;
		if (JSONValidator.isChanged(jsonObj, HOST_KEY, host)) {
			changed = true;
			host = jsonObj.getString(HOST_KEY);
		}
		if (JSONValidator.isChanged(jsonObj, PORT_KEY, port)) {
			changed = true;
			port = jsonObj.getString(PORT_KEY);
		}
		if (JSONValidator.isChanged(jsonObj, TIMEOUT_KEY, timeOut)) {
			changed = true;
			timeOut = jsonObj.getString(TIMEOUT_KEY);
		}
		if (JSONValidator.isChanged(jsonObj, MAXACTIVE_KEY, maxActive)) {
			changed = true;
			maxActive = jsonObj.getString(MAXACTIVE_KEY);
		}
		if (JSONValidator.isChanged(jsonObj, MAXIDLE_KEY, maxIdle)) {
			changed = true;
			maxIdle = jsonObj.getString(MAXIDLE_KEY);
		}
		if (JSONValidator.isChanged(jsonObj, MAXWAIT_KEY, maxWait)) {
			changed = true;
			maxWait = jsonObj.getString(MAXWAIT_KEY);
		}
		if (JSONValidator.isChanged(jsonObj, TESTONBORROW_KEY, testOnBorrow)) {
			changed = true;
			testOnBorrow = jsonObj.getString(TESTONBORROW_KEY);
		}
		if (JSONValidator.isChanged(jsonObj, TESTONRETURN_KEY, testOnReturn)) {
			changed = true;
			testOnReturn = jsonObj.getString(TESTONRETURN_KEY);
		}
		if (JSONValidator.isChanged(jsonObj, DBINDEX_KEY, "" + dbIndex)) {
			dbIndex = jsonObj.getInt(DBINDEX_KEY);
		}
		if (JSONValidator.isChanged(jsonObj, TWEMPROXY_KEY, twemproxy)) {
			twemproxy = jsonObj.getString(TWEMPROXY_KEY);
		}

		if (changed) {
			cacheClient = new RedisCacheClient(conf);
			if (log.isInfoEnabled()) {
				log.info("cache server address is changed to " + conf);
			}
		}
	}

	public ConfigurationCenter getConfCenter() {
		return confCenter;
	}

	public void setConfCenter(ConfigurationCenter confCenter) {
		this.confCenter = confCenter;
	}

	public String getConfPath() {
		return confPath;
	}

	public void setConfPath(String confPath) {
		this.confPath = confPath;
	}

	public void addItemToList(String key, Object object) {
		cacheClient.addItemToList(dbIndex, key, object);
	}

	@SuppressWarnings("rawtypes")
	public List getItemFromList(String key) {
		return cacheClient.getItemFromList(dbIndex, key);

	}

	public void addItem(String key, Object object) {
		if ("true".equalsIgnoreCase(twemproxy)) {
			cacheClient.addItem(key, object);
		} else {
			cacheClient.addItem(dbIndex, key, object);
		}

	}

	public void addItem(String key, Object object, int seconds) {
		if ("true".equalsIgnoreCase(twemproxy)) {
			cacheClient.addItem(key, object, seconds);
		} else {
			cacheClient.addItem(dbIndex, key, object, seconds);
		}
	}

	public Object getItem(String key) {
		if ("true".equalsIgnoreCase(twemproxy)) {
			return cacheClient.getItem(key);
		} else {
			return cacheClient.getItem(dbIndex, key);
		}

	}

	public void delItem(String key) {
		if ("true".equalsIgnoreCase(twemproxy)) {
			cacheClient.delItem(key);
		} else {
			cacheClient.delItem(dbIndex, key);
		}

	}

	public long getIncrement(String key) {
		if ("true".equalsIgnoreCase(twemproxy)) {
			return cacheClient.getIncrement(key);
		} else {
			return cacheClient.getIncrement(dbIndex, key);
		}

	}

	/**
	 * 设置map 可以存储用户信息
	 * 
	 * @param key
	 * @param map
	 */
	public void addMap(String key, Map<String, String> map) {
		if ("true".equalsIgnoreCase(twemproxy)) {
			cacheClient.addMap(key,map);
		} else {
			cacheClient.addMap(dbIndex,key,map);

		}

	}

	public Map<String, String> getMap(String key) {
		if ("true".equalsIgnoreCase(twemproxy)) {
			return cacheClient.getMap(key);
		} else {
			return cacheClient.getMap(dbIndex, key);

		}
	}
	
	public String getMapItem(String key,String field) {
		if ("true".equalsIgnoreCase(twemproxy)) {
			return cacheClient.getMapItem(key, field);
		} else {
			return cacheClient.getMapItem(dbIndex, key,field);

		}
	}

	/**
	 * 添加set
	 * 
	 * @param key
	 * @param set
	 */
	public void addSet(String key, Set<String> set) {
		if ("true".equalsIgnoreCase(twemproxy)) 
			cacheClient.addSet(key, set);
		else
			cacheClient.addSet(dbIndex, key, set);
	}

	public Set<String> getSet(String key) {
		if ("true".equalsIgnoreCase(twemproxy)) 
			return cacheClient.getSet(key);
		else
			return cacheClient.getSet(dbIndex, key);
	}
	
	/**
	 * 添加list
	 * 
	 * @param key
	 * @param list
	 */
	public void addList(String key, List<String> list) {
		if ("true".equalsIgnoreCase(twemproxy)) 
			cacheClient.addList(key, list);
		else
			cacheClient.addList(dbIndex, key, list);
	}

	public List<String> getList(String key) {
		if ("true".equalsIgnoreCase(twemproxy)) 
			return cacheClient.getList(key);
		else
			return cacheClient.getList(dbIndex, key);
	}

	@Override
	public String flushDB() {
		return cacheClient.flushDB(dbIndex);
	}

	public void destroy() {
		cacheClient.destroyPool();
		cacheClient = null;
	}
	
	@Override
	public void addItemFile(String key, byte[] file) {
		cacheClient.addItemFile(key, file);
	}

	@Override
	public void addMapItem(String key, String field,String value) {
		cacheClient.addMapItem(key, field,value);
	}

	@Override
	public void delMapItem(String key, String field) {
		cacheClient.delMapItem(key, field);
	}
}
