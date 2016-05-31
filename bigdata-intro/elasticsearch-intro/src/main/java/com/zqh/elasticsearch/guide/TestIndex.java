package com.zqh.elasticsearch.guide;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zqh.elasticsearch.example.User;
import junit.framework.TestCase;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.ScriptService;
import org.junit.After;
import org.junit.Before;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by hadoop on 15-1-6.
 *
 * 测试方法:
 * 0. 删除es的data目录: cd ~/soft/elasticsearch-1.4.0 && rm -rf data
 * 1. 启动es服务(因为这里的测试代码使用的是TransportClient方式, 而不是Node方式): bin/elasticsearch
 * 2. 运行该测试类里的测试方法
 * 3. 观察某个测试方法的输出
 *
 * 注意: 不要运行整个类, 否则testDelete执行后, testUpdate会报错. 所以可以一个一个测试方法执行, 或者暂时注释掉testDelete
 * http://localhost:9200/twitter/tweet/_search?q=*&pretty=true
 */
public class TestIndex extends TestCase{

    Client client;
    
    final String INDEX_NAME = "twitter";
    final String TYPE_NAME = "tweet";

    @Before
    public void setUp() throws Exception {
        client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
    }
    @After
    public void tearDown() throws Exception {
        client.close();
    }

    // ---------------------------------------------------------------------------------------
    // http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/index_.html
    // ---------------------------------------------------------------------------------------
    public void testJson() throws Exception{
        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";

        indexDocument(json, "1");
    }

    public void testMap() throws Exception{
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("user","kimchy");
        json.put("postDate",new Date());
        json.put("message","trying out Elasticsearch");

        indexDocument(json, "2");
    }

    public void testMapperJson() throws Exception{
        // instance a json mapper
        ObjectMapper mapper = new ObjectMapper(); // create once, reuse

        // generate json
        String json = mapper.writeValueAsString(new User());

        indexDocument(json, "3");
    }

    public void testXContent() throws Exception{
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("postDate", new Date())
                .field("message", "trying out Elasticsearch")
                .endObject();

        String json = builder.string();

        indexDocument(json, "4");
    }

    public void indexDocument(String json, String id) throws Exception{
        IndexResponse response = client.prepareIndex(INDEX_NAME, TYPE_NAME, id)
                .setSource(json)
                .execute()
                .actionGet();
        printIndexResponse(response);
    }

    public void indexDocument(Map<String, Object> json, String id) throws Exception{
        IndexResponse response = client.prepareIndex(INDEX_NAME, TYPE_NAME, id)
                .setSource(json)
                .execute()
                .actionGet();
        printIndexResponse(response);
    }

    public void indexDocument(String json) throws Exception{
        IndexResponse response = client.prepareIndex(INDEX_NAME, TYPE_NAME)
                .setSource(json)
                .execute()
                .actionGet();
        printIndexResponse(response);

    }

    public void printIndexResponse(IndexResponse response){
        // Index name
        String _index = response.getIndex();
        // Type name
        String _type = response.getType();
        // Document ID (generated or not)
        String _id = response.getId();
        // Version (if it's the first time you index this document, you will get: 1)
        long _version = response.getVersion();

        System.out.println("  Index:" + _index);
        System.out.println("   Type:" + _type);
        System.out.println("     Id:" + _id);
        System.out.println("Version:" + _version);
    }

    // -------------------------------------------------------------------------------------
    // http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/delete.html
    // -------------------------------------------------------------------------------------
    public void testDelete(){
        DeleteResponse response = client.prepareDelete(INDEX_NAME, TYPE_NAME, "1")
                .setOperationThreaded(false)
                .execute()
                .actionGet();
    }

    // -------------------------------------------------------------------------------------
    // http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/get.html
    // -------------------------------------------------------------------------------------
    public void testGet(){
        GetResponse response = client.prepareGet(INDEX_NAME, TYPE_NAME, "1")
                //.setOperationThreaded(false)
                .execute()
                .actionGet();
        System.out.println(response.getVersion());
    }

    // ------------------------------------------------------------------------------------------------
    // http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/java-update-api.html
    // ------------------------------------------------------------------------------------------------
    public void testUpdate() throws Exception{
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(INDEX_NAME);
        updateRequest.type(TYPE_NAME);
        updateRequest.id("1");
        updateRequest.doc(jsonBuilder()
                .startObject()
                .field("gender", "male")
                .endObject());
        client.update(updateRequest).get();
    }

    public void testPrepUpdate() throws Exception{
        client.prepareUpdate(INDEX_NAME, TYPE_NAME, "1")
                .setScript("ctx._source.gender = \"male\""  , ScriptService.ScriptType.INLINE)
                .get();

        client.prepareUpdate(INDEX_NAME, TYPE_NAME, "1")
                .setDoc(jsonBuilder()
                        .startObject()
                        .field("gender", "male")
                        .endObject())
                .get();
    }

    public void testScriptUpdate() throws Exception{
        UpdateRequest updateRequest = new UpdateRequest(INDEX_NAME, TYPE_NAME, "1")
                .script("ctx._source.gender = \"male\"");
        client.update(updateRequest).get();
    }

    public void testMergeUpdate() throws Exception{
        UpdateRequest updateRequest = new UpdateRequest(INDEX_NAME, TYPE_NAME, "1")
                .doc(jsonBuilder()
                        .startObject()
                        .field("gender", "female")
                        .endObject());
        client.update(updateRequest).get();
    }

    public void testUpsert() throws Exception{
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME, TYPE_NAME, "1")
                .source(jsonBuilder()
                        .startObject()
                        .field("name", "Joe Smith")
                        .field("gender", "male")
                        .endObject());
        UpdateRequest updateRequest = new UpdateRequest(INDEX_NAME, TYPE_NAME, "1")
                .doc(jsonBuilder()
                        .startObject()
                        .field("gender", "male")
                        .endObject())
                .upsert(indexRequest);
        client.update(updateRequest).get();
    }
}
