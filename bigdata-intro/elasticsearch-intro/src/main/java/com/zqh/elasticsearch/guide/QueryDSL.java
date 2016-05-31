package com.zqh.elasticsearch.guide;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by zhengqh on 15/9/23.
 *
 * http://www.bkjia.com/yjs/914863.html
 */
public class QueryDSL {

    public static void main(String[] args) {
        QueryBuilder rangeQueryBuilder = QueryBuilders
                .rangeQuery("activity.eventOccurTime")
                .from(1442419200000L)
                .to(1442505600000L)
                .includeLower(true)
                .includeUpper(false);
        System.out.println(rangeQueryBuilder.toString());
    }

    public void testDSL()throws Exception{
        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "kimchy elasticsearch");

        QueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(
                "kimchy elasticsearch",     // Text you are looking for
                "user", "message"           // Fields you query on
        );

        QueryBuilder boolQueryBuilder = QueryBuilders
                .boolQuery()
                .must(termQuery("content", "test1"))
                .must(termQuery("content", "test4"))
                .mustNot(termQuery("content", "test2"))
                .should(termQuery("content", "test3"));

        QueryBuilder boostingQueryBuilder = QueryBuilders.boostingQuery()
                .positive(termQuery("name", "kimchy"))
                .negative(termQuery("name", "dadoonet"))
                .negativeBoost(0.2f);

        QueryBuilder rangeQueryBuilder = QueryBuilders
                .rangeQuery("price")
                .from(5)
                .to(10)
                .includeLower(true)
                .includeUpper(false);

        QueryBuilder queryStringQueryBuilder = QueryBuilders.queryString("+kimchy -elasticsearch");

        QueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "kimchy");

        // mlt Query
        QueryBuilder moreLikeThisQueryBuilder = QueryBuilders.moreLikeThisQuery("name.first", "name.last")      // Fields
                .likeText("text like this one")                         // Text
                .minTermFreq(1)                                         // Ignore Threshold
                .maxQueryTerms(12);                                     // Max num of Terms in generated queries mlt_field Query

        QueryBuilders.moreLikeThisFieldQuery("name.first")              // Only on single field
                .likeText("text like this one")
                .minTermFreq(1)
                .maxQueryTerms(12);
    }
}
