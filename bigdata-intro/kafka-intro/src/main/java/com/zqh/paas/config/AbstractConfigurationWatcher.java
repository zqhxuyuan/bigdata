package com.zqh.paas.config;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;

import com.zqh.paas.exception.PropertyMissingException;

/**
 * 实现了初始化加载配置和处理配置的抽象类
 * 
 * @author 毕希研
 * 
 */
public abstract class AbstractConfigurationWatcher implements ConfigurationWatcher, InitializingBean, Serializable {
	private static final long serialVersionUID = 6483952766605690318L;
	public static final Logger log = Logger.getLogger(AbstractConfigurationWatcher.class);
	protected String conf = null;
	protected ConfigurationCenter confCenter = null;

	@Override
	public void afterPropertiesSet() throws Exception {
		if (getConfPath() == null)
			throw new PropertyMissingException("confPath");
		conf = confCenter.getConfAndWatch(getConfPath(), this);
		process(conf);
	}

	public abstract String getConfPath();

	public abstract void process(String conf);

	public String getConf() {
		return conf;
	}

	public void setConf(String conf) {
		this.conf = conf;
	}

	public ConfigurationCenter getConfCenter() {
		return confCenter;
	}

	public void setConfCenter(ConfigurationCenter confCenter) {
		this.confCenter = confCenter;
	}

}
