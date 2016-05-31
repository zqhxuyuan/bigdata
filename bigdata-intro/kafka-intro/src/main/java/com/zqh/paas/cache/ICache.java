package com.zqh.paas.cache;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @ClassName: CacheSvc
 * @Description:
 * @author: lixh
 * @date:2014
 */
public interface ICache {
	public void addItemToList(String key, Object object);

	@SuppressWarnings("rawtypes")
	public List getItemFromList(String key);

	public void addItem(String key, Object object);

	public void addItem(String key, Object object, int seconds);

	public String flushDB();

	public Object getItem(String key);

	public void delItem(String key);

	public long getIncrement(String key);

	public void addMap(String key, Map<String, String> map);

	public Map<String, String> getMap(String key);
	
	public String getMapItem(String key, String field);
	
	public void addMapItem(String key, String field, String value);
	
	public void delMapItem(String key, String field);

	public void addSet(String key, Set<String> set);

	public Set<String> getSet(String key);
	
	public void addList(String key, List<String> list);

	public List<String> getList(String key);
	
	public void addItemFile(String key, byte[] file);
}
