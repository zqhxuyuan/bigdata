package com.client;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;

import java.util.List;

/**
 * Created by zhengqh on 16/1/15.
 */
public class LocalTest {

    static CassClient client = new CassClient("mykeyspace", "localhost","DC1");

    public static void main(String[] args) {
        client.setReadConsistencyLevel(ConsistencyLevel.QUORUM);
        client.init2();

        String sql = "select * from mykeyspace.users";
        PreparedStatement stmt = client.getPrepareSTMT(sql);

        List<Row> rows = client.getAll(stmt);
        for(Row row : rows) {
            String fname = row.getString("fname");
            String lname = row.getString("lname");

            System.out.println(fname + "," + lname);
        }
    }
}
