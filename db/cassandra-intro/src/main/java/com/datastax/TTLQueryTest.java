package com.datastax;

import com.client.CassClient;
import com.datastax.driver.core.PreparedStatement;

/**
 * Created by zhengqh on 15/10/21.
 */
public class TTLQueryTest {

    static CassClient client = new CassClient("test", "localhost","DC1");

    public static void insert(){
        long timestamp = System.currentTimeMillis();

        PreparedStatement pstmtInsert = client.getPrepareSTMT("" +
                "insert into velocity(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)" + "using ttl ?");
        for(int i=0;i<2000;i++) {
            client.execute(pstmtInsert, new Object[]{
                    //模拟过去一段时间的数据. 它们的TTL都不一样, 越早的数据TTL越短.
                    "test4", "koudai", "koudai_ios", "account", Long.valueOf(timestamp - i*30*1000), "raw event json str", ""+i, 5*60+i*30
            });
        }
        System.out.printf("END");
        System.exit(0);
    }

    public static void query(){
        long timestamp = System.currentTimeMillis();

        String select1 = "select * from velocity where attribute='test4' and partner_code='koudai' and app_name='koudai_ios' and type='account' " +
                "order by partner_code desc,app_name desc,type desc,timestamp desc limit 1000";
        String select2 = "select * from velocity where attribute='test4' and partner_code='koudai' and app_name='koudai_ios' and type='account' " +
                "order by partner_code desc,app_name desc,type desc,timestamp desc limit 1000";
    }

    public static void easyInsert()throws Exception{
        //ttl=10s, 10s产生一条数据, 1分钟只有60条
        //ttl=2s, 2s产生一条, 1分钟产生30条, 100条,3min=3*60/2=90loop
        int ttl = 2;
        PreparedStatement pstmtInsert = client.getPrepareSTMT("" +
                "insert into velocity(attribute, partner_code, app_name, type, timestamp, event, sequence_id) values(?, ?, ?, ?, ?, ?, ?)" + "using ttl " + ttl);
        for(int i=0;i<90;i++) {
            long timestamp = System.currentTimeMillis();
            client.execute(pstmtInsert, new Object[]{
                    "test4", "koudai", "koudai_ios", "account", Long.valueOf(timestamp), "raw event json str", ""+i, ttl
            });
            Thread.sleep(2000);
        }
        System.out.printf("END");
        System.exit(0);
    }

    public static void main(String[] args) throws Exception{
        client.init();

        easyInsert();

        /**
         * 一条记录有3个tombstone
         *
         * Read 66 live and 102 tombstone cells
         * 总共100条记录, 66条存活, 则34条被删除. 那么tombstone为什么是102呢?
         * 102/34=3.
         *
         *
         */

        //Bad Request: Invalid operator < for PRIMARY KEY part timestamp
        //PreparedStatement delete = client.getPrepareSTMT("delete from velocity where attribute='test' and partner_code='koudai' and app_name='koudai_ios' and type='account' and timestamp<" + 12345678+50);

        /*
        for(int i=0;i<50;i++){
            PreparedStatement deleteOne = client.getPrepareSTMT("delete from velocity where attribute='test' and partner_code='koudai' and app_name='koudai_ios' and type='account' and timestamp=" + (i+12345678));
            client.execute(deleteOne);
        }
        */

        /*
        PreparedStatement select = client.getPrepareSTMT("select * from velocity");
        ResultSet resultSet = client.execute(select);
        Iterator<Row> it = resultSet.iterator();
        int i=0;
        while (it.hasNext()){
            Row row = it.next();
            String attribute = row.getString("attribute");
            String partner_code = row.getString("partner_code");
            String app_name = row.getString("app_name");
            String type = row.getString("type");
            Long timestamp = row.getLong("timestamp");
            String event = row.getString("event");
            String sequence_id = row.getString("sequence_id");
            System.out.println(i++ + attribute + "," + sequence_id);
        }
        */
    }
}
