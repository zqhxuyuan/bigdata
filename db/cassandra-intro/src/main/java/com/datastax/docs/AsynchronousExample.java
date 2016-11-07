package com.datastax.docs;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AsynchronousExample extends SimpleClient {
    public AsynchronousExample() {
    }

    public ResultSetFuture getRows() {
        Statement query = QueryBuilder.select().all().from("simplex", "songs");
        return getSession().executeAsync(query);
    }

    public static void main(String[] args) {
        AsynchronousExample client = new AsynchronousExample();
        client.connect("127.0.0.1");
        client.createSchema();
        client.loadData();
        ResultSetFuture results = client.getRows();

        //虽然ResultSetFuture是异步的,但是getUninterruptibly,还是会阻塞
        for (Row row : results.getUninterruptibly()) {
            System.out.printf( "%s: %s / %s\n",
                    row.getString("artist"),
                    row.getString("title"),
                    row.getString("album") );
        }

        //client.dropSchema("simplex");
        client.close();
    }
}