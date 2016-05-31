package com.zqh.elasticsearch.jest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.indices.CreateIndex;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by hadoop on 14-12-31.
 */
public class HelloJest {

    public static void main(String[] args) throws Exception{
        // Construct a new Jest client according to configuration via factory
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();

        // creating an index
        client.execute(new CreateIndex.Builder("articles").build());

        // indexing documents
        String source = jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("postDate", "date")
                .field("message", "trying out Elastic Search")
                .endObject().string();

        Index index = new Index.Builder(source).index("twitter").type("tweet").build();
        client.execute(index);

        // searching documents
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("user", "kimchy"));

        Search search = new Search.Builder(searchSourceBuilder.toString())
                // multiple index or types can be added.
                .addIndex("twitter")
                .addIndex("tweet")
                .build();

        SearchResult result = client.execute(search);

        // getting documents
        //Get get = new Get.Builder("twitter", "1").type("tweet").build();

        //JestResult result = client.execute(get);
    }
}
