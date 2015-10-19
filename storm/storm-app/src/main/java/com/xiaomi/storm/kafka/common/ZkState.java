package com.xiaomi.storm.kafka.common;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

public class ZkState {

	public static final Logger LOG = LoggerFactory.getLogger(ZkState.class);
	
	@SuppressWarnings("unchecked")
	private CuratorFramework newCurator(Map stateConf) {
		Integer port = (Integer) stateConf.get(Config.TRANSACTIONAL_ZOOKEEPER_PORT);
		String zkServerPorts = "";
		for(String server : (List<String>)stateConf.get(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS)){
			zkServerPorts += server + ":" + port + ",";
		}
		
		//LOG.info("@@@@@@@@@@@@@@@" + zkServerPorts);
		
		try {
			return CuratorFrameworkFactory.newClient(
				zkServerPorts, 
				Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)), 
				20000, 
				new RetryNTimes(Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)),
						Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
		}catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public ZkState(Map stateConf) {
		//LOG.info("ZkState class init.");
		this._stateConf = new HashMap(stateConf);
		
		try {
			_curator = newCurator(this._stateConf);
			_curator.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public CuratorFramework getCurator() {
		assert _curator != null;
		//LOG.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!###################"+ _curator);
		return _curator;
	}
	
	public void close() {
		_curator.close();
		_curator = null;
	}
	
	public void writeBytes(String path, byte[] bytes) {
		try {
			if(_curator.checkExists().forPath(path) == null) {
				_curator.create()
						.creatingParentsIfNeeded()
						.withMode(CreateMode.PERSISTENT)
						.forPath(path, bytes);
			}else {
				_curator.setData().forPath(path, bytes);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
	}
	
	public byte[] readBytes(String path) {
		try {
			if(_curator.checkExists().forPath(path) != null) {
				return _curator.getData().forPath(path);
			} else {
				return null;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
	}
	
	public void writeJson(String path, Map<Object, Object> data) {
		LOG.info("Writing " + path + " the data " + data.toString());
		writeBytes(path, JSONValue.toJSONString(data).getBytes(Charset.forName("UTF-8")));
	}
	
	public Map<Object, Object> readJson(String path) {
		byte[] bytes = readBytes(path);
		if(bytes != null) {
			try {
				return (Map<Object, Object>)JSONValue.parse(new String(bytes, "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				throw new RuntimeException(e);
			}
		} else {
			return null;
		}
	}
	
	private CuratorFramework _curator;
	private Map _stateConf;
	
}
