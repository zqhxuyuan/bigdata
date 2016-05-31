package com.zqh.elasticsearch.example;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * http://www.elasticsearch.org/guide
 *
 * http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/client.html
 *
 * https://gist.github.com/1639766
 */
public class HelloWorld {

    public static void main(String[] args) {
        Node node = nodeBuilder().node();
        // with cluster.name in elasticsearch.yml
        //node = nodeBuilder().clusterName("elasticsearch").client(true).local(true).node();
        Client client = node.client();

        // Transport Client: 不启动节点就可以和es集群进行通信, 即不需要创建Node节点
        //Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        //client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        System.out.println("client start...");

        Map<String, Object> json = new HashMap<String, Object>();
        json.put("user","kimchy");
        json.put("postDate",new Date());
        json.put("message","trying out Elasticsearch");

        IndexResponse indexResponse = client.prepareIndex("twitter", "tweet", "1")
                .setSource(json)
                .execute()
                .actionGet();

        GetResponse getResponse = client.prepareGet("twitter", "tweet", "1")
                .execute()
                .actionGet();

        Map<String, Object> rpMap = getResponse.getSource();
        if (rpMap == null) return;
        Iterator<Map.Entry<String, Object>> rpItor = rpMap.entrySet().iterator();
        while (rpItor.hasNext()) {
            Map.Entry<String, Object> rpEnt = rpItor.next();
            System.out.println(rpEnt.getKey() + " : " + rpEnt.getValue());
        }

        client.close();
        node.close();
    }
}