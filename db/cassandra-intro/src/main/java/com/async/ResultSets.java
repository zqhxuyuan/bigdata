package com.async;

import java.util.List;
import java.util.concurrent.Future;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

/**
 * Utility methods to demonstrate how to compose result set futures.
 */
public class ResultSets {

    /**
     * Executes the same query on different partitions, and returns all the results as a list.
     *
     * @param session the {@code Session} to query Cassandra.
     * @param query a query string with a single bind parameter for the partition key.
     * @param partitionKeys the list of partition keys to execute the query on.
     *
     * @return a future that will complete when all the queries have completed, and contain the list of matching rows.
     */
    public static Future<List<ResultSet>> queryAllAsList(Session session, String query, Object... partitionKeys) {
        //一系列的ResultSetFuture, RSF中是ResultSet,代表ResultSet的返回结果, 即每个ResultSetFuture中是一个ResultSet的结果
        List<ResultSetFuture> futures = sendQueries(session, query, partitionKeys);
        //转换为只有一个Future, 这个Future里是一系列的ResultSet结果.
        return Futures.successfulAsList(futures);
    }

    /**
     * Executes the same query on different partitions, and returns the results as they become available.
     * @return a list of futures in the order of their completion.
     */
    public static List<ListenableFuture<ResultSet>> queryAll(Session session, String query, Object... partitionKeys) {
        List<ResultSetFuture> futures = sendQueries(session, query, partitionKeys);
        //原先List里是ResultSetFuture, Future里是ResultSet: ResultSetFuture<ResultSet>,
        //将ResultSetFuture转换为可监听的ListenableFuture, 结果还是ResultSet.
        return Futures.inCompletionOrder(futures);
    }

    /**
     * Executes the same query on different partitions, and returns an {code Observable} that emits the results as they become available.
     * @return the observable.
     */
    public static Observable<ResultSet> queryAllAsObservable(Session session, String query, Object... partitionKeys) {
        List<ResultSetFuture> futures = sendQueries(session, query, partitionKeys);
        //转换为Rx的模式, 将ListenableFuture转换为观察者类型Observable, 最后合并为一个Observable<ResultSet>
        Scheduler scheduler = Schedulers.io();
        List<Observable<ResultSet>> observables = Lists.transform(futures, (ResultSetFuture future) -> Observable.from(future, scheduler));
        return Observable.merge(observables);
    }

    //SQL的IN查询, 查询多个partition keys.
    private static List<ResultSetFuture> sendQueries(Session session, String query, Object[] partitionKeys) {
        List<ResultSetFuture> futures = Lists.newArrayListWithExpectedSize(partitionKeys.length);
        for (Object partitionKey : partitionKeys)
            //每个partition key交给session进行异步处理
            futures.add(session.executeAsync(query, partitionKey));
        return futures;
    }
}
