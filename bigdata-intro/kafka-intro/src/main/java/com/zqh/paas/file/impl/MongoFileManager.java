package com.zqh.paas.file.impl;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.zqh.paas.PaasContextHolder;
import com.zqh.paas.PaasException;
import com.zqh.paas.config.ConfigurationCenter;
import com.zqh.paas.config.ConfigurationWatcher;
import com.zqh.paas.file.IFileManager;
import com.zqh.paas.util.JSONValidator;

public class MongoFileManager implements ConfigurationWatcher, IFileManager {
	private static final Logger log = Logger.getLogger(MongoFileManager.class);

	private String confPath = "/com/zqh/paas/file/conf";

	private static final String File_SERVER_KEY = "fileServer";
	private static final String File_REPO_KEY = "fileRepo";
	private static final String USERNAME = "username";
	private static final String PASSWORD = "password";

	private String fileServer = null;
	private String fileRepo = null;
	private String userName = null;
	private String password = null;
	private MongoDBClient mongo = null;
	private ConfigurationCenter confCenter = null;

	public MongoFileManager() {
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
			if (JSONValidator.isChanged(json, File_SERVER_KEY, fileServer)) {
				changed = true;
				fileServer = json.getString(File_SERVER_KEY);
			}
			if (JSONValidator.isChanged(json, USERNAME, userName)) {
				changed = true;
				userName = json.getString(USERNAME);
			}
			if (JSONValidator.isChanged(json, PASSWORD, password)) {
				changed = true;
				password = json.getString(PASSWORD);
			}
			if (JSONValidator.isChanged(json, File_REPO_KEY, fileRepo)) {
				// changed = true;
				fileRepo = json.getString(File_REPO_KEY);
			}
			if (changed) {
				if (fileServer != null) {
					mongo = new MongoDBClient(fileServer, fileRepo, userName, password);
					if (log.isInfoEnabled()) {
						log.info("log server address is changed to " + fileServer);
					}
				}
			}
		} catch (Exception e) {
			log.error("",e);
		}
	}

	public String saveFile(String fileName, String fileType) {
		return mongo.saveFile(fileRepo, fileName, fileType);
	}

	public String saveFile(byte[] byteFile, String fileName, String fileType) {
		return mongo.saveFile(fileRepo, byteFile, fileName, fileType);
	}

	public byte[] readFile(String fileId) {
		return mongo.readFile(fileRepo, fileId);
	}

	public byte[] readFileByName(String fileName) {
		return mongo.readFileByName(fileRepo, fileName);
	}

	public void readFile(String fileId, String localFileName) {
		mongo.readFile(fileRepo, fileId, localFileName);
	}

	public void readFileByName(String fileName, String localFileName) {
		mongo.readFileByName(fileRepo, fileName, localFileName);
	}

	public void deleteFile(String fileId) {
		mongo.deleteFile(fileRepo, fileId);
	}

	public void deleteFileByName(String fileName) {
		mongo.deleteFileByName(fileRepo, fileName);
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

	public void destroy() {
		if (null != mongo) {
			mongo.destroyMongoDB();
			mongo = null;
		}
	}

	@Override
	public String getFileName(String fileId) {
		return mongo.getFileName(fileRepo,fileId);
	}
}
