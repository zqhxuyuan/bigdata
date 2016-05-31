package com.zqh.elasticsearch.example;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

/**
 * Created by hadoop on 15-1-5.
 *
 * 索引名称: shop
 * 索引类型: product
 */
public class TestCRUD {

    public static void main(String[] args) {
        Client client = ESUtils.getClient();
        // 删除
        /*
        DeleteResponse delResponse =
                client.prepareDelete().setIndex(ESUtils.getIndexName())
                        .setType(ESUtils.getTypeName())
                        .setId("1")
                        .execute()
                        .actionGet();
        System.out.println("del is found="+delResponse.isFound());
        */

        // 增加
        IndexResponse indexResponse =
                client.prepareIndex().setIndex(ESUtils.getIndexName())
                        .setType(ESUtils.getTypeName())
                        .setSource("{\"prodId\":1,\"prodName\":\"ipad5\",\"prodDesc\":\"比你想的更强大\",\"catId\":1}")
                        .setId("1")
                        .execute()
                        .actionGet();
        System.out.println("添加成功,isCreated="+indexResponse.isCreated());

        // 查询
        GetResponse getResponse =
                client.prepareGet().setIndex(ESUtils.getIndexName())
                        .setType(ESUtils.getTypeName())
                        .setId("1").execute()
                        .actionGet();
        System.out.println("get="+getResponse.getSourceAsString());

        // 更新
        System.out.println("berfore update version="+getResponse.getVersion());
        UpdateResponse updateResponse =
                client.prepareUpdate().setIndex(ESUtils.getIndexName())
                        .setType(ESUtils.getTypeName())
                        .setDoc("{\"prodId\":1,\"prodName\":\"ipad5\",\"prodDesc\":\"比你想的更强大噢耶\",\"catId\":1}")
                        .setId("1")
                        .execute()
                        .actionGet();
        System.out.println("更新成功,isCreated="+updateResponse.isCreated());
        getResponse =
                client.prepareGet().setIndex(ESUtils.getIndexName())
                        .setType(ESUtils.getTypeName())
                        .setId("1")
                        .execute()
                        .actionGet();
        System.out.println("get version="+getResponse.getVersion());

        // 查询
        QueryBuilder query = QueryBuilders.queryString("ipad5");
        SearchResponse response = client.prepareSearch(ESUtils.INDEX_NAME)
                .setQuery(query)             //设置查询条件

                .setFrom(0).setSize(60)
                .execute()
                .actionGet();
        //SearchHits是SearchHit的复数形式,表示这个是一个列表
        SearchHits shs = response.getHits();
        System.out.println("总共有:"+shs.hits().length);
        for(SearchHit hit : shs){
            System.out.println(hit.getSourceAsString());
        }


        ESUtils.closeClient(client);
    }
}
