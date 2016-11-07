
     CREATE TYPE complex.address (
     street text,
     city text,
     zipCode int,
     phones list<text>
     );

     CREATE TABLE complex.accounts (
     email text PRIMARY KEY,
     name text,
     addr frozen<address>
     );

    CREATE KEYSPACE IF NOT EXISTS simplex WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
    CREATE TABLE IF NOT EXISTS simplex.songs (" +
                            "id uuid PRIMARY KEY," +
                            "title text," +
                            "album text," +
                            "artist text," +
                            "tags set<text>," +
                            "data blob" +
                            ");
    CREATE TABLE IF NOT EXISTS simplex.playlists (" +
                            "id uuid," +
                            "title text," +
                            "album text, " +
                            "artist text," +
                            "song_id uuid," +
                            "PRIMARY KEY (id, title, album, artist)" +
                            ");