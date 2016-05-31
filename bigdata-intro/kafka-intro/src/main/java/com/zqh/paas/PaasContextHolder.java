package com.zqh.paas;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 用于初始化并保存paas的相应的bean
 *
 */
public class PaasContextHolder {
	private static ApplicationContext ctx = new ClassPathXmlApplicationContext(
			new String[] { "paasContext.xml" });

	public static ApplicationContext getContext() {
		return ctx;
	}
	public static void closeCtx(){
		((ClassPathXmlApplicationContext) ctx).close();
	}

}
