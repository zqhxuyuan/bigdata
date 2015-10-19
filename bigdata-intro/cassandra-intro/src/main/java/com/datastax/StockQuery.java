package com.datastax;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Created by zhengqh on 15/9/11.
 *
 * Caused by: com.datastax.driver.core.exceptions.ReadTimeoutException:
 * Cassandra timeout during read query at consistency ONE (1 responses were required but only 0 replica responded)
 */
public class StockQuery {

    public static void main(String[] args) {
        //query("forseti");
        query("forseti_fp");
        //System.exit(1);
    }

    public static void query(String ks){
        // Connect to the cluster and keyspace "demo"
        Cluster cluster = Cluster.builder().addContactPoints("192.168.6.56").build();
        Session session = cluster.connect(ks);

        System.out.println(ks + ":");
        ResultSet results = session.execute("select * from "+ks+".historical_prices where ticker='ORCL' and date>='2015-07-01 00:00:00+0800'");
        //System.out.println(results.all().size());
        for (Row row : results) {
            System.out.format("%s %s\n", row.getDecimal("open"), row.getDecimal("high"));
        }
        System.out.println("END...");
    }
}
