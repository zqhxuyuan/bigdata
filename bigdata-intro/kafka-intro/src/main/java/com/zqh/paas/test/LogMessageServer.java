package com.zqh.paas.test;

import com.zqh.paas.message.impl.MessageConsumer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class LogMessageServer {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		String conf = "paasContext.xml";
		if (args.length > 1) {
			conf = args[0];
		}
		ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { conf });
        MessageConsumer server = (MessageConsumer) ctx.getBean("messageConsumer");
		while (true) {
			try {
				Thread.sleep(100000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
