package com.datastax;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.util.Iterator;
import java.util.List;

/**
 * Created by zhengqh on 15/10/16.
 */
public class CassClientTest {

    static CassClient client = new CassClient("forseti", "192.168.6.52,192.168.6.53","DC1");

    //Velocity原先的查询方式
    static String velocity = "select * from velocity where attribute=? and partner_code=? and app_name=? and type=?";
    //给Velocity加上order by和limit
    static String velocityOrder = "select * from velocity where attribute=? and partner_code=? and app_name=? and type=? order by partner_code desc,app_name desc,type desc,timestamp desc limit 1000";
    //拆表之后的查询
    static String velocity_app = "select * from velocity_app where attribute=? and partner_code=? and app_name=? and type=? order by timestamp desc limit 1000";
    static String velocity_partner = "select * from velocity_partner where attribute=? and partner_code=? order by timestamp desc limit 1000";
    static String velocity_global = "select * from velocity_global where attribute=? order by timestamp desc limit 1000";

    public static void main(String[] args) {
        client.init();

        //insertByClient();
        //insertUsingTTL();

        //velocityDataSyncAll1();
        //velocityDataSyncAll2();
        velocityDataSyncAll3();

        client.close();
        System.exit(0);
    }

    public static void truncateVelocity3(){
        truncate("velocity_app");
        truncate("velocity_partner");
        truncate("velocity_global");
    }
    public static void truncate(String tbl){
        PreparedStatement truncate = client.getSession().prepare("truncate " + tbl);
        client.execute(truncate);
    }

    //TODO: 线上环境显然不能直接查询全量数据, 可以采用分页? 但是C*的分页使用token方式实现. 有点麻烦
    //1.使用Batch, 类似于事务操作的原子性, 会很慢
    public static void velocityDataSyncAll1(){
        truncateVelocity3();
        long start = System.currentTimeMillis();

        PreparedStatement pstmtSelectAll = client.getPrepareSTMT("select * from velocity");
        List<Row> rows = client.getAll(pstmtSelectAll);

        PreparedStatement pstmtInsert = client.getPrepareSTMT(
                "BEGIN BATCH" +
                        " insert into velocity_app(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?);" +
                        " insert into velocity_partner(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?);" +
                        " insert into velocity_global(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?);" +
                        "APPLY BATCH");

        for(Row row : rows){
            String attribute = row.getString("attribute");
            String partner_code = row.getString("partner_code");
            String app_name = row.getString("app_name");
            String type = row.getString("type");
            Long timestamp = row.getLong("timestamp");
            String event = row.getString("event");
            String sequence_id = row.getString("sequence_id");

            //使用Session操作, Statement要用bind绑定paramValues.
            client.getSession().execute(pstmtInsert.bind(
                    attribute, partner_code, app_name, type, timestamp, event, sequence_id,
                    attribute, partner_code, app_name, type, timestamp, event, sequence_id,
                    attribute, partner_code, app_name, type, timestamp, event, sequence_id
            ));
        }
        long end = System.currentTimeMillis();
        System.out.println(end-start);
    }

    //2.分开插入
    public static void velocityDataSyncAll2(){
        truncateVelocity3();
        long start = System.currentTimeMillis();

        PreparedStatement pstmtSelectAll = client.getPrepareSTMT("select * from velocity");
        List<Row> rows = client.getAll(pstmtSelectAll);

        PreparedStatement pstmtInsert1 = client.getPrepareSTMT("insert into velocity_app(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)");
        PreparedStatement pstmtInsert2 = client.getPrepareSTMT("insert into velocity_partner(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)");
        PreparedStatement pstmtInsert3 = client.getPrepareSTMT("insert into velocity_global(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)");

        for(Row row : rows){
            String attribute = row.getString("attribute");
            String partner_code = row.getString("partner_code");
            String app_name = row.getString("app_name");
            String type = row.getString("type");
            Long timestamp = row.getLong("timestamp");
            String event = row.getString("event");
            String sequence_id = row.getString("sequence_id");

            client.execute(pstmtInsert1, new Object[]{attribute, partner_code, app_name, type, timestamp, event, sequence_id});
            client.execute(pstmtInsert2, new Object[]{attribute, partner_code, app_name, type, timestamp, event, sequence_id});
            client.execute(pstmtInsert3, new Object[]{attribute, partner_code, app_name, type, timestamp, event, sequence_id});
        }
        long end = System.currentTimeMillis();
        System.out.println(end-start);
    }

    //3.Paging Operation分页查询
    public static void velocityDataSyncAll3(){
        truncateVelocity3();
        long start = System.currentTimeMillis();

        Statement statement = client.getSimpleSTMT("select * from velocity");
        statement.setFetchSize(1000);

        PreparedStatement pstmtInsert1 = client.getPrepareSTMT("insert into velocity_app(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)");
        PreparedStatement pstmtInsert2 = client.getPrepareSTMT("insert into velocity_partner(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)");
        PreparedStatement pstmtInsert3 = client.getPrepareSTMT("insert into velocity_global(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)");

        ResultSet resultSet = client.getSession().execute(statement);
        Iterator<Row> iterator = resultSet.iterator();
        while(iterator.hasNext()){
            Row row = iterator.next();

            String attribute = row.getString("attribute");
            String partner_code = row.getString("partner_code");
            String app_name = row.getString("app_name");
            String type = row.getString("type");
            Long timestamp = row.getLong("timestamp");
            String event = row.getString("event");
            String sequence_id = row.getString("sequence_id");

            client.execute(pstmtInsert1, new Object[]{attribute, partner_code, app_name, type, timestamp, event, sequence_id});
            client.execute(pstmtInsert2, new Object[]{attribute, partner_code, app_name, type, timestamp, event, sequence_id});
            client.execute(pstmtInsert3, new Object[]{attribute, partner_code, app_name, type, timestamp, event, sequence_id});
        }
        long end = System.currentTimeMillis();
        System.out.println(end-start);
    }

    //插入数据: Exception in thread "main" com.datastax.driver.core.exceptions.InvalidQueryException: You must use conditional updates for serializable writes
    //Exception in thread "main" com.datastax.driver.core.exceptions.InvalidTypeException: Invalid type for value 4 of CQL type bigint, expecting class java.lang.Long but class java.lang.Integer provided
    public static void insertByClient(){
        PreparedStatement pstmtInsert = client.getPrepareSTMT("insert into velocity(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)");
        for(int i=0;i<500;i++) {
            for(int j=0;j<10;j++)
               client.execute(pstmtInsert, new Object[]{"test_"+j, "koudai", "koudai_ios", "account", Long.valueOf(12345678 + i), "raw event json str", "" + Long.valueOf(12345678 + i)});
        }
    }

    public static void insertUsingTTL(){
        PreparedStatement pstmtInsert = client.getPrepareSTMT("insert into velocity(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?) using ttl 3600");
        for(int i=0;i<8000;i++) {
            client.execute(pstmtInsert, new Object[]{
                    "test3", "koudai", "koudai_ios", "account", Long.valueOf(12345678 + i), "raw event json str", "" + Long.valueOf(12345678 + i)
            });
        }
    }

    public static void insertByAPI(){
        QueryOptions options = new QueryOptions();
        options.setConsistencyLevel(ConsistencyLevel.ONE);

        Cluster cluster = Cluster.builder()
                .addContactPoint("192.168.6.52")
                 //.withCredentials("cassandra", "cassandra")
                .withQueryOptions(options)
                .build();
        Session session = cluster.connect("forseti");

        insertVelocity(session, "velocity_app", "test");
        insertVelocity(session, "velocity_partner", "test");
        insertVelocity(session, "velocity_global", "test");
        insertVelocity(session, "velocity_test", "test");

        session.close();
        cluster.close();
    }
    public static void insertVelocity(Session session, String tbl, String rowKey){
        for(int i=0;i<8000;i++){
            RegularStatement insert = QueryBuilder.insertInto("forseti", tbl).values(
                    "attribute,partner_code,app_name,type,timestamp,event,sequence_id".split(","),
                    new Object[]{
                            rowKey, "koudai", "koudai_ios", "account", 12345678+i, "raw event json str", ""+12345678+i
                    });
            session.execute(insert);
        }
    }

    public static void velocityQuery(){
        PreparedStatement pstmtSelectAppScope = client.getPrepareSTMT(velocity);
        PreparedStatement preparedStatementOrder = client.getPrepareSTMT(velocityOrder);

        PreparedStatement preparedStatementOrder_app = client.getPrepareSTMT(velocity_app);
        PreparedStatement preparedStatementOrder_partner = client.getPrepareSTMT(velocity_partner);
        PreparedStatement preparedStatementOrder_global = client.getPrepareSTMT(velocity_global);

        query(preparedStatementOrder_app, new Object[]{"test", "koudai", "koudai_ios", "account"});
        query(preparedStatementOrder_partner, new Object[]{"test", "koudai"});
        query(preparedStatementOrder_global, new Object[]{"test"});
    }

    public static void query(PreparedStatement pstmt, Object... params){
        int loopTime = 1;
        //使用不同的row-key查询,主要是为了排除多次查询是否有Cache的影响.
        long start1 = System.currentTimeMillis();

        for(int i=0;i<loopTime;i++) {
            List<Row> rows = client.getAllOfSize(pstmt, 1000, params);
        }
        long end1 = System.currentTimeMillis();

        System.out.println("Cost(ms):" + (end1-start1));
    }
}
