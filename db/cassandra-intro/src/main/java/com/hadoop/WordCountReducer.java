package com.hadoop;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Rahul.Chauhan
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, ByteBuffer, List<Mutation>> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        List<Mutation> columnsToAdd = new ArrayList<Mutation>();

        //计算word对应的count, 相同的word会被同一个reduce函数处理.
        Integer wordCount = 0;
        for(IntWritable value : values) {
            wordCount += value.get();
        }

        //将结果写入到Cassandra. 构造C*的Column. 这个Column是thrift协议的Column,还有个CQL的Column.
        Column countCol = new Column(ByteBuffer.wrap("count".getBytes()));  //列名
        countCol.setValue(intToByteArray(wordCount));  //列的值
        countCol.setTimestamp(new Date().getTime());
        countCol.setTtl(0);

        //比较麻烦,还要包装上ColumnOrSuperColumn
        ColumnOrSuperColumn wordCosc = new ColumnOrSuperColumn();
        wordCosc.setColumn(countCol);

        //更新代表INSERT或者DELETE, 再包装上Mutation
        Mutation countMut = new Mutation();
        countMut.column_or_supercolumn = wordCosc;

        columnsToAdd.add(countMut);

        //Key是row-key, 即partition-key, Value是这一行对应的所有列
        //最后才设置row-key, 前面的工作都是这一行的columns.........
        context.write(ByteBuffer.wrap(key.toString().getBytes()), columnsToAdd);
    }

    private byte[] intToByteArray (final Integer intValue) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeInt(intValue);
        dos.flush();
        return bos.toByteArray();
    }

}