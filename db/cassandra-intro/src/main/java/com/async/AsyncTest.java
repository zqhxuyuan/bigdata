package com.async;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.util.concurrent.*;


/**
 * Created by zhengqh on 15/11/3.
 */
public class AsyncTest {

    public static void main(String[] args) {
        Cluster cluster = Cluster.builder()
                .addContactPoint("192.168.6.52")
                .build();

        ListenableFuture<Session> session = null;//cluster.connectAsync();

        // Use transform with an AsyncFunction to chain an async operation after another:
        ListenableFuture<ResultSet> resultSet = Futures.transform(session,
                new AsyncFunction<Session, ResultSet>() {
                    public ListenableFuture<ResultSet> apply(Session session) throws Exception {
                        return session.executeAsync("select release_version from system.local");
                    }
                });

        // Use transform with a simple Function to apply a synchronous computation on the result:
        ListenableFuture<String> version = Futures.transform(resultSet,
                new Function<ResultSet, String>() {
                    public String apply(ResultSet rs) {
                        return rs.one().getString("release_version");
                    }
                });

        // Use a callback to perform an action once the future is complete:
        Futures.addCallback(version, new FutureCallback<String>() {
            public void onSuccess(String version) {
                System.out.printf("Cassandra version: %s%n", version);
            }

            public void onFailure(Throwable t) {
                System.out.printf("Failed to retrieve the version: %s%n",
                        t.getMessage());
            }
        });
    }
}
