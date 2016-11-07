package com.datastax.docs;

import java.text.SimpleDateFormat;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class TracingExample extends SimpleClient {
    private SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");

    public TracingExample() {
    }

    //INSERT INTO simplex.songs(id, title, album, artist) VALUES (da7c6910-a6a4-11e2-96a9-4db56cdc5fe7,'Golden Brown', 'La Folie', 'The Stranglers');
    public void traceInsert() {
        Statement insert = QueryBuilder.insertInto("simplex", "songs")
                .value("id", UUID.randomUUID())
                .value("title", "Golden Brown")
                .value("album", "La Folie")
                .value("artist", "The Stranglers")
                .setConsistencyLevel(ConsistencyLevel.ONE).enableTracing();
        ResultSet results = getSession().execute(insert);

        ExecutionInfo executionInfo = results.getExecutionInfo();
        printTraceLog(executionInfo);
        insert.disableTracing();
    }

    public void traceSelect() {
        Statement scan = new SimpleStatement("SELECT * FROM simplex.songs;");
        ExecutionInfo executionInfo = getSession().execute(scan.enableTracing()).getExecutionInfo();
        printTraceLog(executionInfo);
        scan.disableTracing();
    }

    public void traceBoundBind(String id){
        PreparedStatement selectStatement = getSession().prepare("select * from simplex.songs where id=?");
        BoundStatement boundStatement = new BoundStatement(selectStatement).bind(UUID.fromString(id));
        ExecutionInfo executionInfo = getSession().execute(boundStatement.enableTracing()).getExecutionInfo();
        printTraceLog(executionInfo);
        System.out.println(boundStatement.isTracing());
        boundStatement.disableTracing();
        System.out.println(boundStatement.isTracing());
    }

    @Deprecated
    public void traceBoundException(){
        PreparedStatement selectStatement = getSession().prepare("select * from simplex.songs where id=?");
        BoundStatement boundStatement = null;
        ResultSet rs = null;
        try{
            boundStatement = new BoundStatement(selectStatement).bind(UUID.fromString("da7c6910-a6a4-11e2-96a9-4db56cdc5fe7"));
            rs = getSession().execute(boundStatement);
        } catch (Exception e) {
            if(boundStatement != null){
                //必须重新执行一次,带有enableTracing. 如果执行时不带enableTracing, 则不会有QueryTrace
                ExecutionInfo executionInfo = getSession().execute(boundStatement.enableTracing()).getExecutionInfo();
                printTraceLog(executionInfo);
            }
            e.printStackTrace();
        }
    }

    //超时控制
    public ResultSet getRows(Statement stmt) {
        return getRows(stmt, 500);
    }
    public ResultSet getRows(Statement stmt, int timeout) {
        ResultSetFuture future = getSession().executeAsync(stmt);
        ResultSet rs = null;
        try {
            rs = future.get(timeout, TimeUnit.MILLISECONDS);
            return rs;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            future.cancel(true);
        }
        return null;
    }

    public ResultSet traceQuery(String id){
        PreparedStatement selectStatement = getSession().prepare("select * from simplex.songs where id=?");
        BoundStatement boundStatement = new BoundStatement(selectStatement);
        ResultSet rs = null;
        try{
            //bind的类型和参数类型一致,这里因为id是UUID,所以要把字符串转成UUID
            //rs = getSession().execute(boundStatement.bind(UUID.fromString(id)).enableTracing());
            rs = getRows(boundStatement.bind(UUID.fromString(id)).enableTracing());
            return rs;
        } catch (Exception e) {
            if(boundStatement != null){
                ExecutionInfo executionInfo = rs.getExecutionInfo();
                printTraceLog(executionInfo);
            }
            e.printStackTrace();
        }
        return null;
    }

    private Object millis2Date(long timestamp) {
        return format.format(timestamp);
    }
    private String duplicate(int count){
        StringBuffer sb = new StringBuffer();
        for(int i=0;i<count;i++){
            sb.append("-");
        }
        sb.append("+");
        return sb.toString();
    }
    private void printTraceLog(ExecutionInfo executionInfo){
        System.out.println("******************************************************************");
        System.out.printf( "Host (queried): %s\n", executionInfo.getQueriedHost().toString() );
        for (Host host : executionInfo.getTriedHosts()) {
            System.out.printf( "Host (tried): %s\n", host.toString() );
        }
        QueryTrace queryTrace = executionInfo.getQueryTrace();
        System.out.printf("Trace id: %s\n\n", queryTrace.getTraceId());
        System.out.printf("%-100s | %-12s | %-10s | %-12s\n", "activity", "timestamp", "source", "source_elapsed");
        System.out.println(duplicate(101) + duplicate(14) + duplicate(12) + duplicate(15));
        for (QueryTrace.Event event : queryTrace.getEvents()) {
            System.out.printf("%100s | %12s | %10s | %12s\n", event.getDescription(),
                    millis2Date(event.getTimestamp()),
                    event.getSource(), event.getSourceElapsedMicros());
        }
    }


    public static void main(String[] args) {
        TracingExample client = new TracingExample();
        client.connect("127.0.0.1");
        //client.createSchema();
        //client.traceInsert();
        //client.traceSelect();
        client.traceQuery("da7c6910-a6a4-11e2-96a9-4db56cdc5fe7");
        client.close();
    }
}