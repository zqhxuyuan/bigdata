package com.bulk;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.AbstractPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.AbstractSSTableSimpleWriter;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * Created by zhengqh on 15/11/9.
 */
public class SimpleWriteTable {

    public static void main(String[] args) throws Exception{

        ByteBuffer key = ByteBuffer.wrap("key".getBytes());
        ByteBuffer colName = ByteBuffer.wrap("cl1".getBytes());
        ByteBuffer colValue = ByteBuffer.wrap("cl2".getBytes());

        AbstractPartitioner partitioner = new Murmur3Partitioner();
        AbstractSSTableSimpleWriter writer = new SSTableSimpleUnsortedWriter(
                new File(""),partitioner, "forseti","velocity", UTF8Type.instance,null,512);

        //行, 创建新的一行
        writer.newRow(key);
        //列, 添加新的一列
        writer.addColumn(colName, colValue, 0L);
    }
}
