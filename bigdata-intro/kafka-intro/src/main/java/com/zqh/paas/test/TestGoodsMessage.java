package com.zqh.paas.test;

import com.zqh.paas.util.JSONValidator;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.zqh.paas.PaasConstant;
import com.zqh.paas.PaasContextHolder;
import com.zqh.paas.message.impl.MessageSender;

public class TestGoodsMessage {
	private static final Logger logger = Logger.getLogger(TestGoodsMessage.class);
	private static MessageSender messageSender = null;
	private static String goodsConfPath = "/com/zqh/paas/storm/goods/update/conf";
	private static String goodsMessageTopic = "paas_goods_topic";

	private TestGoodsMessage() {
	}

	private static MessageSender getIntance() {
		if (null != messageSender)
			return messageSender;
		else {
			messageSender = (MessageSender) PaasContextHolder.getContext().getBean("messageSender");
			return messageSender;
		}
	}

	public static void sendGoodsMessage(String goodsId, String provCode) throws Exception {
		process(getIntance().getConfCenter().getConf(goodsConfPath));
		// 转换成JSON
		JSONObject json = new JSONObject();
		json.put(PaasConstant.JSON_MESSAGE_KEY_GOODS_ID, goodsId);
		json.put(PaasConstant.JSON_MESSAGE_KEY_PROV_CODE, provCode);
		getIntance().sendMessage(json.toString(), goodsMessageTopic);
		logger.info("send goods update message:" + goodsId + " to topic:" + goodsMessageTopic);
	}

	private static void process(String conf) {
		JSONObject json = JSONObject.fromObject(conf);
		if (JSONValidator.isChanged(json, "kafka.topic", goodsMessageTopic)) {
			goodsMessageTopic = json.getString("kafka.topic");
		}
	}

	public static void main(String[] args) {
		try {
			TestGoodsMessage.sendGoodsMessage("1111111111", "11");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
