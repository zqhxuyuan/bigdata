package com.zqh.elasticsearch.example;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

import java.security.SecureRandom;
import java.util.Random;

/**
 * Created by hadoop on 15-1-5.
 */
public class TestBulk {

    public static void main(String[] args) {
        Client client = ESUtils.getClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        User user = new User();
        Random r = new Random(100);
        for(int i=1;i<40001;i++){
            user.setName("user_" + r.nextInt());
            SecureRandom random = new SecureRandom();
            long l = Math.abs(random.nextLong());
            user.setWeight(l);
            user.setMarried(l % 1 == 0 ? true : false);
            user.setAge(l % 2 == 0 ? 28 : 82);
            IndexRequestBuilder ir =
                    client.prepareIndex(ESUtils.INDEX_NAME,
                            ESUtils.TYPE_NAME,
                            String.valueOf(i)).setSource(ESUtils.toJson(user));
            bulkRequest.add(ir);
        }
        long beginTime = System.currentTimeMillis();
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        long useTime = System.currentTimeMillis() - beginTime;
        System.out.println("useTime:" + useTime);   //1406ms
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
        }
    }
}
