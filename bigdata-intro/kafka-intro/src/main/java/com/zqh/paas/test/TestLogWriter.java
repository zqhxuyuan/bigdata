package com.zqh.paas.test;

import com.zqh.paas.PaasException;
import com.zqh.paas.log.ILogWriter;
import com.zqh.paas.log.impl.MongoLogWriter;
import net.sf.json.JSONObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Date;

/**
 * Created by zqhxuyuan on 15-3-9.
 */
public class TestLogWriter {

    public static void main(String[] args) throws PaasException {
        ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { "paasContext.xml" });
        ILogWriter logWriter = (MongoLogWriter) ctx.getBean("logWriter");
        for (int i = 0; i < 10; i++) {
            System.out.println(i);
            String log = "{level:'ERROR',server:'10.1.1.3',log:'test log " + new Date() + "'}";
            logWriter.write(log);
            JSONObject json = JSONObject.fromObject("{level:'ERROR是倒萨大',server:'10.1.1.4',log:'test log" + new Date() + "'}");
            logWriter.write(json);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
