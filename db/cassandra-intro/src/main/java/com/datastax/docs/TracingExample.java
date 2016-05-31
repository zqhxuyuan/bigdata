package com.datastax.docs;

import java.text.SimpleDateFormat;
import java.util.UUID;

import com.datastax.driver.core.*;
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
        System.out.printf( "Host (queried): %s\n", executionInfo.getQueriedHost().toString() );
        for (Host host : executionInfo.getTriedHosts()) {
            System.out.printf( "Host (tried): %s\n", host.toString() );
        }
        QueryTrace queryTrace = executionInfo.getQueryTrace();
        System.out.printf("Trace id: %s\n\n", queryTrace.getTraceId());
        System.out.printf("%-38s | %-12s | %-10s | %-12s\n", "activity", "timestamp", "source", "source_elapsed");
        System.out.println("---------------------------------------+--------------+------------+--------------");
        for (QueryTrace.Event event : queryTrace.getEvents()) {
            System.out.printf("%38s | %12s | %10s | %12s\n", event.getDescription(),
                    millis2Date(event.getTimestamp()),
                    event.getSource(), event.getSourceElapsedMicros());
        }
        insert.disableTracing();
    }

    public void traceSelect() {
        Statement scan = new SimpleStatement("SELECT * FROM simplex.songs;");
        ExecutionInfo executionInfo = getSession().execute(scan.enableTracing()).getExecutionInfo();
        System.out.printf( "Host (queried): %s\n", executionInfo.getQueriedHost().toString() );
        for (Host host : executionInfo.getTriedHosts()) {
            System.out.printf( "Host (tried): %s\n", host.toString() );
        }
        QueryTrace queryTrace = executionInfo.getQueryTrace();
        System.out.printf("Trace id: %s\n\n", queryTrace.getTraceId());
        System.out.printf("%-38s | %-12s | %-10s | %-12s\n", "activity", "timestamp", "source", "source_elapsed");
        System.out.println("---------------------------------------+--------------+------------+--------------");
        for (QueryTrace.Event event : queryTrace.getEvents()) {
            System.out.printf("%38s | %12s | %10s | %12s\n", event.getDescription(),
                    millis2Date(event.getTimestamp()),
                    event.getSource(), event.getSourceElapsedMicros());
        }
        scan.disableTracing();
    }

    public void traceBound(){
        PreparedStatement selectStatement = getSession().prepare("select * from songs where id=?");
        BoundStatement boundStatement = new BoundStatement(selectStatement);

        BoundStatement boundStatement1 = boundStatement.bind("da7c6910-a6a4-11e2-96a9-4db56cdc5fe7");
        ExecutionInfo executionInfo = getSession().execute(boundStatement1.enableTracing()).getExecutionInfo();

        System.out.printf( "Host (queried): %s\n", executionInfo.getQueriedHost().toString() );
        for (Host host : executionInfo.getTriedHosts()) {
            System.out.printf( "Host (tried): %s\n", host.toString() );
        }
        QueryTrace queryTrace = executionInfo.getQueryTrace();
        System.out.printf("Trace id: %s\n\n", queryTrace.getTraceId());
        System.out.printf("%-38s | %-12s | %-10s | %-12s\n", "activity", "timestamp", "source", "source_elapsed");
        System.out.println("---------------------------------------+--------------+------------+--------------");
        for (QueryTrace.Event event : queryTrace.getEvents()) {
            System.out.printf("%38s | %12s | %10s | %12s\n", event.getDescription(),
                    millis2Date(event.getTimestamp()),
                    event.getSource(), event.getSourceElapsedMicros());
        }
        boundStatement1.disableTracing();
    }

    private Object millis2Date(long timestamp) {
        return format.format(timestamp);
    }

    public static void main(String[] args) {
        TracingExample client = new TracingExample();
        client.connect("127.0.0.1");
        client.createSchema();
        client.traceInsert();
        client.traceSelect();
        client.close();
    }
}