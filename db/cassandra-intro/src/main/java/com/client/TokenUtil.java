package com.client;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by zhengqh on 15/11/12.
 */
public class TokenUtil {

    static String host = "192.168.6.52";
    static int port = 7199;
    static String keyspace = "forseti";

    public static Session getSession(){
        LoadBalancingPolicy policy = new DCAwareRoundRobinPolicy();
        Cluster cluster = Cluster.builder().addContactPoints(host).withPort(port).withLoadBalancingPolicy(policy).build();
        Session session = cluster.connect();
        return session;
    }

    public static Host getLocalHost(Metadata metadata, LoadBalancingPolicy policy) {
        Set<Host> allHosts = metadata.getAllHosts();
        StringBuilder s = new StringBuilder();
        Host localHost = null;
        for (Host host : allHosts) {
            if (policy.distance(host) == HostDistance.LOCAL) {
                localHost = host;
                break;
            }
        }
        return localHost;
    }

    public static Set<TokenRange> unwrapTokenRanges(Set<TokenRange> wrappedRanges) {
        HashSet<TokenRange> tokenRanges = new HashSet<TokenRange>();
        for (TokenRange tokenRange : wrappedRanges) {
            tokenRanges.addAll(tokenRange.unwrap());
        }
        return tokenRanges;
    }

    //http://www.planetcassandra.org/blog/data-locality-w-cassandra-how-to-scan-the-local-token-range-of-a-table/
    public static void buildCluster() {
        LoadBalancingPolicy policy = new DCAwareRoundRobinPolicy();
        Cluster cluster = Cluster.builder().addContactPoints(host).withPort(port).withLoadBalancingPolicy(policy).build();
        Session session = cluster.connect();

        String columns = "";
        String table = "velocity";
        int pageSize = 100;
        String partitionKey = "";

        TokenRange range = null;
        Metadata metadata = cluster.getMetadata();
        Host localhost = getLocalHost(metadata, policy);
        Set<TokenRange> tokenRanges = unwrapTokenRanges(metadata.getTokenRanges(keyspace, localhost));//.toArray(new TokenRange[0]);

        Statement stmt = QueryBuilder.select(columns.split(",")).from(table)
                .where(gt(token(partitionKey), range.getStart().getValue()))
                .and(lte(token(partitionKey), range.getEnd().getValue()));
        stmt.setFetchSize(pageSize);
        ResultSet resultSet = session.execute(stmt);
        Iterator<Row> iterator = resultSet.iterator();
        while (!resultSet.isFullyFetched()) {
            resultSet.fetchMoreResults();
            Row row = iterator.next();
            System.out.println(row);
        }
    }

    public static void DSL(){
        Session session = getSession();

        //update
        Statement exampleQuery = QueryBuilder.update("keyspace","table").with(QueryBuilder.set("height", 180))
                .and(QueryBuilder.set("width", 300)).where(QueryBuilder.eq("id", 5145924587302797538L));
        session.execute(exampleQuery);

        //insert
        exampleQuery= QueryBuilder.insertInto("keyspace","table").value("id",12245L)
                .value("data", ByteBuffer.wrap(new byte[]{0x11})).ifNotExists();
        session.execute(exampleQuery);

        //select
        exampleQuery= QueryBuilder.select().from("keyspace","table").where(QueryBuilder.gt("height",450))
                .and(QueryBuilder.eq("product","test")).limit(50).orderBy(QueryBuilder.asc("id"));
        ResultSet results= session.execute(exampleQuery);
        for(Row r:results.all()){
            System.out.println(r.toString());
        }
    }
}
