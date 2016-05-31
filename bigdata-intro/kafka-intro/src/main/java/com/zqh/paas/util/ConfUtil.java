package com.zqh.paas.util;

import com.zqh.paas.PaasContextHolder;
import com.zqh.paas.config.ConfigurationCenter;

public class ConfUtil {
	private static ConfigurationCenter confCenter = null;

	private ConfUtil() {
		// 禁止实例化
	}

	private static ConfigurationCenter getIntance() {
		if (null != confCenter)
			return confCenter;
		else {
			confCenter = (ConfigurationCenter) PaasContextHolder.getContext()
					.getBean("confCenter");
			return confCenter;
		}
	}

	public static String getConf(String key) throws Exception {
		return getIntance().getConf(key);
	}

	public static void main(String[] args) throws Exception {
		System.out.println(ConfUtil.getConf("/com/ai/paas/message/messageSender"));
	}
}
