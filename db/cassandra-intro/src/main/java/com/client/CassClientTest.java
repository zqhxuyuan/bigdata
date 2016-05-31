package com.client;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Created by zhengqh on 15/10/16.
 */
public class CassClientTest {

//    static CassClient client = new CassClient("forseti", "192.168.6.52","DC1");
//    static CassClient client = new CassClient("forseti", "192.168.6.53","DC1");
    static CassClient client = new CassClient("forseti", "192.168.6.70","DC1");

    //Velocity原先的查询方式
    static String velocity = "select * from velocity where attribute=? and partner_code=? and app_name=? and type=?";
    //给Velocity加上order by和limit
    static String velocityOrder = "select * from velocity where attribute=? and partner_code=? and app_name=? and type=? order by partner_code desc,app_name desc,type desc,timestamp desc limit 1000";
    //拆表之后的查询
    static String velocity_app = "select * from velocity_app where attribute=? and partner_code=? and app_name=? and type=? order by timestamp desc limit 1000";
    static String velocity_partner = "select * from velocity_partner where attribute=? and partner_code=? order by timestamp desc limit 1000";
    static String velocity_global = "select * from velocity_global where attribute=? order by timestamp desc limit 1000";

    static String velocity_rowkeys = "select distinct attribute from velocity";

    public static void testOK(){
//        String sql = "select * from forseti.velocity limit 10";
//        PreparedStatement stmt = client.getPrepareSTMT(sql);
//
//        List<Row> rows = client.getAll(stmt);
//        for(Row row : rows) {
//            String attribute = row.getString("attribute");
//            String partner_code = row.getString("partner_code");
//            String app_name = row.getString("app_name");
//            String type = row.getString("type");
//            Long timestamp = row.getLong("timestamp");
//            String event = row.getString("event");
//            String sequence_id = row.getString("sequence_id");
//
//            System.out.println(attribute + "," + partner_code + "," + app_name);
//        }

        String sql2 = "insert into velocity(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement pstmtInsert1 = client.getPrepareSTMT(sql2);
        client.execute(pstmtInsert1, new Object[]{
                "test_zqh", "kd", "kd", "account",1234567890L, "event","1234567890-0001"
        });

    }

    public static void main(String[] args) {
        client.init2();
        long start = System.currentTimeMillis();


        testOK();

        //insertByClient();
        //insertUsingTTL();
        //testInsert3Velocity();

        //velocityDataSyncAll1();
        //velocityDataSyncAll2();    //5000, 32421
        //velocityDataSyncAll3(1000);  //5000, 33285

        //velocityDataSyncAll3(1000);

        //velocityDataSyncAll4(1000);
        try{
            //insertLongKey();
            //queryLongKey();

            //insertAndQueryFP();
            //keepQuery();
            //rowKeys();

            //token2();
        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            System.out.println(System.currentTimeMillis() - start);
            client.close();
        }
    }

    public static void token2(){
        String sql = "select distinct attribute from velocity";
        Statement statement = client.getSimpleSTMT(sql);
        statement.setFetchSize(1000);
        int i=0;

        ResultSet rs = client.execute(statement);
        Iterator<Row> iter = rs.iterator();

        while (!rs.isFullyFetched()) {
            rs.fetchMoreResults();
            Row row = iter.next();
            System.out.println(i++ + ":" + row.getString("attribute"));
        }
    }

    public static void tokenTest(){
        String sql = "select distinct attribute from velocity where token(attribute) > token(?) limit 10";
        PreparedStatement statement = client.getPrepareSTMT(sql);
        String lastKey = "";

        while(true){
            System.out.println(sql.replace("?",lastKey));
            ResultSet resultSet = client.execute(statement, new Object[]{lastKey});
            Iterator<Row> it = resultSet.iterator();
            Row row = null;
            if(it == null) break;
            while (it.hasNext()) {
                row = it.next();
                String attribute = row.getString("attribute");
                System.out.print(attribute + " ");
            }
            System.out.println();
            lastKey = row.getString("attribute");
            sql = "select distinct attribute from velocity where token(attribute) > token('"+lastKey+"') limit 10";
        }
    }

    //com.datastax.driver.core.exceptions.NoHostAvailableException: All host(s) tried for query failed (tried: /192.168.6.52:9042
    // (com.datastax.driver.core.OperationTimedOutException: [/192.168.6.52:9042] Operation timed out), /192.168.6.53:9042 (com.datastax.driver.core.OperationTimedOutException: [/192.168.6.53:9042] Operation timed out))
    public static void rowKeys(){
        PreparedStatement statement = client.getPrepareSTMT(velocity_rowkeys);
        ResultSet resultSet = client.execute(statement);
        Iterator<Row> it = resultSet.iterator();
        while (it.hasNext()) {
            Row row = it.next();
            String attribute = row.getString("attribute");
            System.out.println(attribute);
        }
    }

    public static void keepQuery() throws Exception{
        String sql2 = "select count(*) from test2.test";
        PreparedStatement pstmt = client.getPrepareSTMT(sql2);

        while (true){
            List<Row> rows = client.getAll(pstmt);
            for(Row row : rows) {
                long count = row.getLong("count");
                System.out.println(count);
            }
            Thread.sleep(500);
        }
    }

    public static void insertAndQueryFP() throws Exception{
        Random r1 = new Random(1234);
        Random r2 = new Random(2345);

        String sql = "insert into test2.test(a,b)values(?,?)";
        String sql2 = "select count(*) from test2.test";

        PreparedStatement pstmtInsert1 = client.getPrepareSTMT(sql);
        PreparedStatement pstmt = client.getPrepareSTMT(sql2);

        while(true){
            int a = r1.nextInt(1000);
            int b = r2.nextInt(1000);
            System.out.println(a + "," + b);
            client.execute(pstmtInsert1, new Object[]{""+a, ""+b,});

            List<Row> rows = client.getAll(pstmt);
            for(Row row : rows) {
                long count = row.getLong("count");
                System.out.println(count);
            }
            Thread.sleep(500);
        }
    }

    public static void queryLongKey(){
        String sql = "select * from velocity_test limit 1";
        PreparedStatement stmt = client.getPrepareSTMT(sql);

        List<Row> rows = client.getAll(stmt);
        for(Row row : rows) {
            String attribute = row.getString("attribute");
            String partner_code = row.getString("partner_code");
            String app_name = row.getString("app_name");
            String type = row.getString("type");
            Long timestamp = row.getLong("timestamp");
            String event = row.getString("event");
            String sequence_id = row.getString("sequence_id");

            System.out.println(attribute + "," + partner_code + "," + app_name);
        }
    }

    public static void insertLongKey(){
        String sql = "insert into velocity_test(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)";

        StringBuffer sb = new StringBuffer();
        //3280*20=65600, 但是实际的Partiton key size=65623
        //Caused by: com.datastax.driver.core.exceptions.InvalidQueryException: Partition key size 65623 exceeds maximum 65535
        for(int i=0;i<3275;i++) {
            sb.append("aaaaaaaaaaaaaaaaaaaa");
        }
        //3275*20=65500 +23=65523, add another 12 char to reach 65535
        sb.append("aaaaaaaaaaa");

        PreparedStatement pstmtInsert1 = client.getPrepareSTMT(sql);
        client.execute(pstmtInsert1, new Object[]{
                sb.toString(), "kd", "kd", "account",1234567890L, "event","1234567890-0001"
        });
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
        System.out.println(end - start);
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
    public static void velocityDataSyncAll3(int fetchSize){
        truncateVelocity3();
        long start = System.currentTimeMillis();

        Statement statement = client.getSimpleSTMT("select * from velocity");
        statement.setFetchSize(fetchSize);

        String ttl = "using ttl " + 3600*19;
        ttl = "";

        PreparedStatement pstmtInsert1 = client.getPrepareSTMT("insert into velocity_app(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)" + ttl);
        PreparedStatement pstmtInsert2 = client.getPrepareSTMT("insert into velocity_partner(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)" + ttl);
        PreparedStatement pstmtInsert3 = client.getPrepareSTMT("insert into velocity_global(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)" + ttl);

        ResultSet resultSet = client.getSession().execute(statement);
        Iterator<Row> iterator = resultSet.iterator();
        int i=0;
        while(iterator.hasNext()){
            Row row = iterator.next();

            String attribute = row.getString("attribute");
            String partner_code = row.getString("partner_code");
            String app_name = row.getString("app_name");
            String type = row.getString("type");
            Long timestamp = row.getLong("timestamp");
            String event = row.getString("event");
            String sequence_id = row.getString("sequence_id");
            //System.out.println(i++ + attribute + "," + sequence_id);

            client.execute(pstmtInsert1, new Object[]{attribute, partner_code, app_name, type, timestamp, event, sequence_id});
            client.execute(pstmtInsert2, new Object[]{attribute, partner_code, app_name, type, timestamp, event, sequence_id});
            client.execute(pstmtInsert3, new Object[]{attribute, partner_code, app_name, type, timestamp, event, sequence_id});
        }
        long end = System.currentTimeMillis();
        System.out.println(i+" records cost: "+(end-start));
    }

    public static void velocityDataSyncAll4(int fetchSize) {
        //truncateVelocity3();
        long start = System.currentTimeMillis();

        PreparedStatement statement = client.getPrepareSTMT("select * from velocity");
        BoundStatement boundStatement = new BoundStatement(statement);
        boundStatement.setFetchSize(fetchSize);

        String ttl = "using ttl " + 3600*19;
        ttl = "";

        PreparedStatement pstmtInsert1 = client.getPrepareSTMT("insert into velocity_app(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)" + ttl);
        PreparedStatement pstmtInsert2 = client.getPrepareSTMT("insert into velocity_partner(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)" + ttl);
        PreparedStatement pstmtInsert3 = client.getPrepareSTMT("insert into velocity_global(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)" + ttl);

        ResultSet resultSet = client.getSession().execute(boundStatement);
        Iterator<Row> iterator = resultSet.iterator();
        int i=0;
        while(!resultSet.isFullyFetched()){
            resultSet.fetchMoreResults();
            Row row = iterator.next();

            String attribute = row.getString("attribute");
            String partner_code = row.getString("partner_code");
            String app_name = row.getString("app_name");
            String type = row.getString("type");
            Long timestamp = row.getLong("timestamp");
            String event = row.getString("event");
            String sequence_id = row.getString("sequence_id");
            //System.out.println(i++ + attribute + "," + sequence_id);

            client.execute(pstmtInsert1, new Object[]{attribute, partner_code, app_name, type, timestamp, event, sequence_id});
            client.execute(pstmtInsert2, new Object[]{attribute, partner_code, app_name, type, timestamp, event, sequence_id});
            client.execute(pstmtInsert3, new Object[]{attribute, partner_code, app_name, type, timestamp, event, sequence_id});
            i++;
        }
        long end = System.currentTimeMillis();
        System.out.println(i + " records cost: " + (end - start));
    }

    public static void testInsert3Velocity(){
        String ttl = "using ttl " + 1800; //设置TTL为半个小时
        PreparedStatement pstmtInsert = client.getPrepareSTMT("insert into velocity_app(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)" + ttl);
        for(int i=0;i<5000;i++) {
            for(int j=0;j<10;j++)
            client.execute(pstmtInsert, new Object[]{"ttl", "koudai", "koudai_ios", "account", Long.valueOf(12345678 + i), "raw event json str", "" + Long.valueOf(12345678 + i)});
        }

    }

    //插入数据: Exception in thread "main" com.datastax.driver.core.exceptions.InvalidQueryException: You must use conditional updates for serializable writes
    //Exception in thread "main" com.datastax.driver.core.exceptions.InvalidTypeException: Invalid type for value 4 of CQL type bigint, expecting class java.lang.Long but class java.lang.Integer provided
    public static void insertByClient(){
        PreparedStatement pstmtInsert = client.getPrepareSTMT("insert into velocity(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)");
        for(int i=0;i<500;i++) {
            for(int j=0;j<100;j++)
               client.execute(pstmtInsert, new Object[]{"prod_"+j, "koudai"+j, "koudai_ios"+j, "account", Long.valueOf(12345678 + i), "raw event json str", "" + Long.valueOf(12345678 + i)});
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
        //insertVelocity(session, "velocity_test", "test");

        session.close();
        cluster.close();
    }
    public static void insertVelocity(Session session, String tbl, String rowKey){
        for(int i=0;i<4000;i++){
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
