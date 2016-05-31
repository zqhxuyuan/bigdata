package com.zqh.elasticsearch.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ESUtils {
    public static final String INDEX_NAME = "shop";

    public static String getIndexName() {
        return INDEX_NAME;
    }

    public static final String TYPE_NAME = "product";

    public static String getTypeName() {
        return TYPE_NAME;
    }

    public static Client getClient() {
        Settings settings = ImmutableSettings.settingsBuilder()
                //.put("cluster.name", "elasticsearch")   //指定集群名称
                //.put("client.transport.sniff", true)    //探测集群中机器状态
                .build();
        // 创建客户端,所有的操作都由客户端开始,这个就好像是 JDBC 的 Connection对象用完记得要关闭
        Client client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        return client;
    }

    public static void closeClient(Client esClient) {
        if (esClient != null) {
            esClient.close();
        }
    }

    //-----------------------------
    public static ObjectMapper mapper = new ObjectMapper();

    public static ObjectMapper getMapper(){
        return mapper;
    }

    public static String toJson(Object o){
        try {
            ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
            return writer.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "转换 JSON 时发生异常";
        }
    }
}
