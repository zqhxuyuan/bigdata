/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.com.zqh;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class CClient {
    public static void main(String[] args) throws TException, InvalidRequestException, UnavailableException, UnsupportedEncodingException, NotFoundException, TimedOutException {

        TTransport tr = new TFramedTransport(new TSocket("localhost", 9160));
        TProtocol proto = new TBinaryProtocol(tr);
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();

        String key_user_id = "1";

        // insert data
        long timestamp = System.currentTimeMillis();
        client.set_keyspace("Keyspace1");
        ColumnParent parent = new ColumnParent("Standard1");

        Column nameColumn = new Column(toByteBuffer("name"));
        nameColumn.setValue(toByteBuffer("Chris Goffinet"));
        nameColumn.setTimestamp(timestamp);
        client.insert(toByteBuffer(key_user_id), parent, nameColumn, ConsistencyLevel.ONE);

        Column ageColumn = new Column(toByteBuffer("age"));
        ageColumn.setValue(toByteBuffer("24"));
        ageColumn.setTimestamp(timestamp);
        client.insert(toByteBuffer(key_user_id), parent, ageColumn, ConsistencyLevel.ONE);

        ColumnPath path = new ColumnPath("Standard1");

        // read single column
        path.setColumn(toByteBuffer("name"));
        System.out.println(client.get(toByteBuffer(key_user_id), path, ConsistencyLevel.ONE));

        // read entire row
        SlicePredicate predicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange(toByteBuffer(""), toByteBuffer(""), false, 10);
        predicate.setSlice_range(sliceRange);

        List<ColumnOrSuperColumn> results = client.get_slice(toByteBuffer(key_user_id),
                                                             parent, predicate, ConsistencyLevel.ONE);
        for (ColumnOrSuperColumn result : results) {
            Column column = result.column;
            System.out.println(toString(column.name) + " -> " + toString(column.value));
        }
        tr.close();

    }

    public static ByteBuffer toByteBuffer(String value) throws UnsupportedEncodingException {
        return ByteBuffer.wrap(value.getBytes("UTF-8"));
    }

    public static String toString(ByteBuffer buffer) throws UnsupportedEncodingException {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, "UTF-8");
    }
}