package com.zqh.paas.log.impl;

import java.util.Date;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.zqh.paas.PaasException;
import com.zqh.paas.config.ConfigurationCenter;
import com.zqh.paas.config.ConfigurationWatcher;
import com.zqh.paas.file.impl.MongoDBClient;
import com.zqh.paas.log.ILogWriter;
import com.zqh.paas.util.JSONValidator;

public class MongoLogWriter implements ConfigurationWatcher, ILogWriter {
	private static final Logger log = Logger.getLogger(MongoLogWriter.class);

	private String confPath = "/com/zqh/paas/logger/conf";

	private static final String LOG_SERVER_KEY = "logServer";
	private static final String LOG_REPO_KEY = "logRepo";
	private static final String LOG_PATH_KEY = "logPath";
	private static final String USERNAME = "userName";
	private static final String PASSWORD = "password";

	private String logServer = null;
	private String logRepo = null;
	private String logPath = null;
	private String userName = null;
	private String password = null;
	private MongoDBClient mongo = null;
	private ConfigurationCenter confCenter = null;

	public MongoLogWriter() {

	}

	public void init() {
		try {
			process(confCenter.getConfAndWatch(confPath, this));
		} catch (PaasException e) {
			e.printStackTrace();
		}
	}

	public void process(String conf) {
		if (log.isInfoEnabled()) {
			log.info("new log configuration is received: " + conf);
		}
		try {
			JSONObject json = JSONObject.fromObject(conf);
			boolean changed = false;
			if (JSONValidator.isChanged(json, LOG_SERVER_KEY, logServer)) {
				changed = true;
				logServer = json.getString(LOG_SERVER_KEY);
			}
			if (JSONValidator.isChanged(json, LOG_REPO_KEY, logRepo)) {
				changed = true;
				logRepo = json.getString(LOG_REPO_KEY);
			}
			if (JSONValidator.isChanged(json, USERNAME, userName)) {
				changed = true;
				userName = json.getString(USERNAME);
			}
			if (JSONValidator.isChanged(json, PASSWORD, password)) {
				changed = true;
				password = json.getString(PASSWORD);
			}
			if (JSONValidator.isChanged(json, LOG_PATH_KEY, logPath)) {
				// changed = true;
				logPath = json.getString(LOG_PATH_KEY);
			}
			if (changed) {
				if (logServer != null) {
					//mongo = new MongoDBClient(logServer, logRepo, userName, password);
					mongo = new MongoDBClient(logServer);
					if (log.isInfoEnabled()) {
						log.info("log server address is changed to " + logServer);
					}
				}
			}
		} catch (Exception e) {
			log.error("", e);
		}
	}

	public void write(JSONObject logJson) {
		mongo.insertJSON(logRepo, logPath, logJson);
	}

	@SuppressWarnings("rawtypes")
	public void write(Map logMap) {
		mongo.insert(logRepo, logPath, logMap);
	}

	public void write(String log) {
		mongo.insert(logRepo, logPath, log);
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
}
