package com.datastax;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.util.Iterator;

/**
 * Created by zqhxuyuan on 15-8-25.
 */
public class SimpleCRUD {

    public static void main(String[] args) throws Exception{
        QueryOptions options = new QueryOptions();
        options.setConsistencyLevel(ConsistencyLevel.ONE);

        Cluster cluster = Cluster.builder()
                .addContactPoint("192.168.6.52")
                //.withCredentials("cassandra", "cassandra")
                .withQueryOptions(options)
                .build();

        // 针对keyspace的session，后面表名前面不用加keyspace
        Session session = cluster.connect("kp");

        simple(session);

        session.close();
        cluster.close();
        System.exit(0);
    }

    public static void simple(Session kpSession){
        kpSession.execute("CREATE  KEYSPACE kp WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        kpSession.execute("CREATE TABLE tbl(a INT,  b INT, c INT, PRIMARY KEY(a));");

        RegularStatement insert = QueryBuilder.insertInto("kp", "tbl").values(new String[] {"a", "b", "c"}, new Object[] {1, 2, 3});
        kpSession.execute(insert);

        RegularStatement insert2 = QueryBuilder.insertInto("kp", "tbl").values(new String[] {"a", "b", "c"}, new Object[] {3, 2, 1});
        kpSession.execute(insert2);

        RegularStatement delete = QueryBuilder.delete().from("kp", "tbl").where(QueryBuilder.eq("a", 1));
        kpSession.execute(delete);

        RegularStatement update = QueryBuilder.update("kp", "tbl").with(QueryBuilder.set("b", 6)).where(QueryBuilder.eq("a", 3));
        kpSession.execute(update);

        RegularStatement select = QueryBuilder.select().from("kp", "tbl").where(QueryBuilder.eq("a", 3))
                .and(QueryBuilder.eq("c",1));
        ResultSet rs = kpSession.execute(select);
        Iterator<Row> iterator = rs.iterator();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            System.out.println("a=" + row.getInt("a"));
            System.out.println("b=" + row.getInt("b"));
            System.out.println("c=" + row.getInt("c"));
        }
    }
}
