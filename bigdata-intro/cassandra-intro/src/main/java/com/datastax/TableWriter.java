package com.datastax;

import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/**
 * Created by zqhxuyuan on 15-8-25.
 */
public class TableWriter {

    public static void main(String[] args) throws Exception{
        String schema = "CREATE TABLE myKs.myTable ("
                + "  k int PRIMARY KEY,"
                + "  v1 text,"
                + "  v2 int"
                + ")";
        String insert = "INSERT INTO myKs.myTable (k, v1, v2) VALUES (?, ?, ?)";

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory("path/to/directory")
                .forTable(schema)
                .using(insert).build();

        writer.addRow(0, "test1", 24);
        writer.addRow(1, "test2", null);
        writer.addRow(2, "test3", 42);

        writer.close();
    }
}
