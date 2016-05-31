package com.zqh.paas.test;

import com.zqh.paas.PaasException;
import com.zqh.paas.config.ConfigurationCenter;
import com.zqh.paas.log.impl.MongoLogWriter;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by zqhxuyuan on 15-3-9.
 */
public class TestConfig {

    private static final Logger log = Logger.getLogger(TestConfig.class);

    public static void main(String[] args) throws PaasException,InterruptedException {
        ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { "paasContext.xml" });
        ConfigurationCenter confCenter = (ConfigurationCenter) ctx.getBean("confCenter");

        log.error(confCenter.getConf("/com/zqh/paas/session/conf"));

        MongoLogWriter logWriter = (MongoLogWriter)ctx.getBean("logWriter");
        while(true) {
            Thread.sleep(1000);
        }
    }
}
