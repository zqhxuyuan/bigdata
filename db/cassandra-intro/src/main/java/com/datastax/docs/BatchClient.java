package com.datastax.docs;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;

import java.util.Date;
import java.util.UUID;

/**
 * Created by zhengqh on 16/5/31.
 */
public class BatchClient extends SimpleClient {

    public void loadData_Simple() {
        getSession().execute(
                "BEGIN BATCH USING TIMESTAMP " +
                        "   INSERT INTO simplex.songs (id, title, album, artist) VALUES (" + UUID.randomUUID() + ", 'Poulaillers'' Song', 'Jamais content', 'Alain Souchon'); " +
                        "   INSERT INTO simplex.songs (id, title, album, artist) VALUES (" + UUID.randomUUID() + ", 'Bonnie and Clyde', 'Bonnie and Clyde', 'Serge Gainsbourg'); " +
                        "   INSERT INTO simplex.songs (id, title, album, artist) VALUES (" + UUID.randomUUID() + ", 'Lighthouse Keeper', 'A Clockwork Orange', 'Erika Eigen'); " +
                "APPLY BATCH"
        );
    }

    public void loadData_Prepare() {
        long timestamp = new Date().getTime();
        PreparedStatement insertPreparedStatement = getSession().prepare(
                "BEGIN BATCH USING TIMESTAMP " + timestamp +
                        "   INSERT INTO simplex.songs (id, title, album, artist) VALUES (?, ?, ?, ?); " +
                        "   INSERT INTO simplex.songs (id, title, album, artist) VALUES (?, ?, ?, ?); " +
                        "   INSERT INTO simplex.songs (id, title, album, artist) VALUES (?, ?, ?, ?); " +
                "APPLY BATCH"
        );

        getSession().execute(
                insertPreparedStatement.bind(
                        UUID.randomUUID(), "Seaside Rendezvous", "A Night at the Opera", "Queen",
                        UUID.randomUUID(), "Entre Nous", "Permanent Waves", "Rush",
                        UUID.randomUUID(), "Frank Sinatra", "Fashion Nugget", "Cake"
                ));
    }

    public void loadData() {
        PreparedStatement insertSongPreparedStatement = getSession().prepare("INSERT INTO simplex.songs (id, title, album, artist) VALUES (?, ?, ?, ?);");

        BatchStatement batch = new BatchStatement();
        batch.add(insertSongPreparedStatement.bind(UUID.randomUUID(), "Die Mösch", "In Gold", "Willi Ostermann"));
        batch.add(insertSongPreparedStatement.bind(UUID.randomUUID(), "Memo From Turner", "Performance", "Mick Jagger"));
        batch.add(insertSongPreparedStatement.bind(UUID.randomUUID(), "La Petite Tonkinoise", "Bye Bye Blackbird", "Joséphine Baker"));
        getSession().execute(batch);
    }
}
