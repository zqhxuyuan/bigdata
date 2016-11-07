package com.datastax.docs;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SimpleClient {
    private Cluster cluster;
    private Session session;

    public Session getSession() {
        return this.session;
    }

    public void connect(String node) {
        cluster = Cluster.builder()
                .addContactPoint(node)
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                .build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for ( Host host : metadata.getAllHosts() ) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }

    public void createSchema() {
        session.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};");
        session.execute(
                "CREATE TABLE IF NOT EXISTS simplex.songs (" +
                        "id uuid PRIMARY KEY," +
                        "title text," +
                        "album text," +
                        "artist text," +
                        "tags set<text>," +
                        "data blob" +
                        ");");
        session.execute(
                "CREATE TABLE IF NOT EXISTS simplex.playlists (" +
                        "id uuid," +
                        "title text," +
                        "album text, " +
                        "artist text," +
                        "song_id uuid," +
                        "PRIMARY KEY (id, title, album, artist)" +
                        ");");
    }

    public void loadData() {
        session.execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                        "VALUES (" +
                        "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                        "'La Petite Tonkinoise'," +
                        "'Bye Bye Blackbird'," +
                        "'Joséphine Baker'," +
                        "{'jazz', '2013'})" +
                        ";");
        session.execute(
                "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
                        "VALUES (" +
                        "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                        "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                        "'La Petite Tonkinoise'," +
                        "'Bye Bye Blackbird'," +
                        "'Joséphine Baker'" +
                        ");");
    }

    public void querySchema() {
        ResultSet results = session.execute("SELECT * FROM simplex.playlists " +
                "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
        System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
                "-------------------------------+-----------------------+--------------------"));
        for (Row row : results) {
            System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"), row.getString("album"),  row.getString("artist")));
        }
        System.out.println();
    }

    public void querySchema2() {
        Statement statement = new SimpleStatement(
                "INSERT INTO simplex.songs " +
                        "(id, title, album, artist) " +
                        "VALUES (da7c6910-a6a4-11e2-96a9-4db56cdc5fe7," +
                        "'Golden Brown', 'La Folie', 'The Stranglers'" +
                        ");");
        try {
            getSession().execute(statement);
        } catch (NoHostAvailableException e) {
            System.out.printf("No host in the %s cluster can be contacted to execute the query.\n",
                    getSession().getCluster());
        } catch (QueryExecutionException e) {
            System.out.println("An exception was thrown by Cassandra because it cannot " +
                    "successfully execute the query with the specified consistency level.");
        } catch (QueryValidationException e) {
            System.out.printf("The query is not valid, for example, incorrect syntax.");
        } catch (IllegalStateException e) {
            System.out.println("The BoundStatement is not ready.");
        }
    }

    public void close() {
        session.close();
        cluster.close();
    }

    public static void main(String[] args) {
        SimpleClient client = new SimpleClient();
        client.connect("127.0.0.1");
        client.createSchema();
        client.loadData();
        client.querySchema2();
        client.close();
    }
}