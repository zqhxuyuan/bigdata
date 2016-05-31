package com.zqh.paas.test;

import com.zqh.paas.PaasContextHolder;
import com.zqh.paas.message.impl.MessageSender;

/**
 * Created by zqhxuyuan on 15-3-9.
 */
public class TestMessageSender {

    public static void main(String[] args) {
        MessageSender sender = (MessageSender) PaasContextHolder.getContext().getBean("messageSender");
        System.out.println("-------------------1-");
        sender.sendMessage("1111", "paas_log_mongo_topic");
        System.out.println("-------------------2-");
    }
}
